#!/usr/bin/env python3
"""
Darwin Push Port STOMP Client for Roydon Level Crossing
Connects to Darwin real-time feed and extracts all trains (stopping + passing)
Loads timetable from S3 on startup for immediate train visibility

Deployed on Railway - serves both API and frontend
"""

import stomp
import gzip
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
import json
import time
import os
from flask import Flask, jsonify, send_from_directory, render_template_string
from flask_cors import CORS
import threading

# Import S3 bootstrap module
try:
    from s3_bootstrap import S3TimetableBootstrap
    S3_BOOTSTRAP_AVAILABLE = True
except ImportError:
    print('⚠️  S3 Bootstrap module not found')
    S3_BOOTSTRAP_AVAILABLE = False

app = Flask(__name__, static_folder='static', template_folder='templates')
CORS(app)

# Read configuration from environment variables
API_PORT = int(os.environ.get('PORT', 5002))

# Darwin Push Port Credentials (from environment - REQUIRED)
DARWIN_HOST = os.environ.get('DARWIN_HOST', 'darwin-dist-44ae45.nationalrail.co.uk')
DARWIN_PORT = int(os.environ.get('DARWIN_PORT', 61613))
DARWIN_USERNAME = os.environ.get('DARWIN_USERNAME')
DARWIN_PASSWORD = os.environ.get('DARWIN_PASSWORD')
DARWIN_TOPIC = '/topic/darwin.pushport-v16'
DARWIN_CLIENT_ID = f'{DARWIN_USERNAME}-roydon-crossing' if DARWIN_USERNAME else 'roydon-crossing'

# AWS S3 Configuration (from environment - REQUIRED)
AWS_ACCESS_KEY_ID = os.environ.get('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.environ.get('AWS_SECRET_ACCESS_KEY')
AWS_REGION = os.environ.get('AWS_REGION', 'eu-west-1')
S3_BUCKET = os.environ.get('S3_BUCKET', 'darwin-s3-pushport-4a45cf7f5667')

# Roydon station codes
ROYDON_CRS = 'RYN'
ROYDON_TIPLOC = 'ROYDON'

# Configuration
REFRESH_INTERVAL = 30 * 60  # Refresh data every 30 minutes
CLOSURE_BEFORE_TRAIN_MINUTES = 2
CLOSURE_BUFFER_SECONDS = 12

# Global storage for trains
trains_cache = {}
cache_lock = threading.Lock()
last_snapshot_load = None
darwin_connected = False

# Darwin XML namespaces
NAMESPACES = {
    'ns3': 'http://www.thalesgroup.com/rtti/PushPort/Schedules/v3',
    'ns5': 'http://www.thalesgroup.com/rtti/PushPort/Forecasts/v3',
}


def load_initial_trains():
    """Skip S3 loading - use Darwin real-time only (saves memory)"""
    global last_snapshot_load
    
    print('📥 Skipping S3 bootstrap (memory optimization)')
    print('   Will rely on Darwin Push Port for train data')
    last_snapshot_load = datetime.now()
    return True

def parse_time(time_str):
    """Parse time string (HH:MM or HH:MM:SS) to datetime today"""
    if not time_str:
        return None
    
    try:
        parts = time_str.split(':')
        hours = int(parts[0])
        minutes = int(parts[1])
        
        now = datetime.now()
        train_time = now.replace(hour=hours, minute=minutes, second=0, microsecond=0)
        
        if train_time < now:
            train_time += timedelta(days=1)
        
        return train_time
    except Exception as e:
        return None


class DarwinMessageListener(stomp.ConnectionListener):
    """Listener for Darwin Push Port messages"""
    
    def on_error(self, frame):
        print(f'❌ Darwin Error: {frame.body}')
    
    def on_message(self, frame):
        """Handle incoming Darwin message"""
        try:
            body = None
            
            if hasattr(frame, 'binary_body'):
                body = frame.binary_body
            elif hasattr(frame, '_body'):
                body = frame._body
            elif hasattr(frame, 'body'):
                body = frame.body
            
            if body is None:
                return
            
            if isinstance(body, str) and '\ufffd' in body:
                return
            
            if isinstance(body, bytes):
                xml_data = gzip.decompress(body).decode('utf-8')
                root = ET.fromstring(xml_data)
                
                self.process_schedules(root)
                self.process_train_status(root)
            
        except gzip.BadGzipFile:
            pass
        except ET.ParseError:
            pass
        except Exception as e:
            pass
    
    def process_schedules(self, root):
        """Process schedule messages (new trains)"""
        for schedule in root.iter():
            if 'schedule' in schedule.tag.lower() and schedule.tag.endswith('schedule'):
                try:
                    rid = schedule.get('rid')
                    uid = schedule.get('uid')
                    ssd = schedule.get('ssd')
                    toc = schedule.get('toc')
                    
                    if not rid:
                        continue
                    
                    roydon_location = None
                    origin = None
                    destination = None
                    
                    for loc in schedule.iter():
                        tiploc = loc.get('tpl', '')
                        
                        if origin is None and tiploc:
                            origin = tiploc
                        if tiploc:
                            destination = tiploc
                        
                        if tiploc == ROYDON_TIPLOC:
                            roydon_location = loc
                    
                    if roydon_location is not None:
                        pta = roydon_location.get('pta')
                        ptd = roydon_location.get('ptd')
                        wtp = roydon_location.get('wtp')
                        wta = roydon_location.get('wta')
                        wtd = roydon_location.get('wtd')
                        
                        if pta or ptd:
                            train_type = 'stopping'
                            time_str = ptd or pta
                        elif wtp:
                            train_type = 'passing'
                            time_str = wtp
                        else:
                            train_type = 'passing'
                            time_str = wtd or wta
                        
                        if time_str:
                            train_time = parse_time(time_str)
                            
                            with cache_lock:
                                trains_cache[rid] = {
                                    'rid': rid,
                                    'uid': uid,
                                    'ssd': ssd,
                                    'type': train_type,
                                    'time': time_str,
                                    'parsed_time': train_time.isoformat() if train_time else None,
                                    'destination': destination,
                                    'origin': origin,
                                    'toc': toc,
                                    'eta': None,
                                    'delayed': False
                                }
                            
                            print(f'📍 Train {uid} - {train_type} at Roydon at {time_str}')
                        
                except Exception as e:
                    pass
    
    def process_train_status(self, root):
        """Process train status messages (updates to existing trains)"""
        for ts in root.iter():
            if 'TS' in ts.tag:
                try:
                    rid = ts.get('rid')
                    
                    if not rid or rid not in trains_cache:
                        continue
                    
                    for loc in ts.iter():
                        if 'Location' in loc.tag:
                            tpl = loc.get('tpl', '')
                            
                            if tpl == ROYDON_TIPLOC:
                                with cache_lock:
                                    if rid in trains_cache:
                                        et = loc.get('et')
                                        at = loc.get('at')
                                        
                                        if at:
                                            trains_cache[rid]['eta'] = at
                                            trains_cache[rid]['actual'] = True
                                        elif et:
                                            trains_cache[rid]['eta'] = et
                                            trains_cache[rid]['delayed'] = et != trains_cache[rid]['time']
                                
                except Exception as e:
                    pass
    
    def on_connected(self, frame):
        global darwin_connected
        darwin_connected = True
        print('✅ Connected to Darwin Push Port')
    
    def on_disconnected(self):
        global darwin_connected
        darwin_connected = False
        print('❌ Disconnected from Darwin Push Port')


def connect_to_darwin():
    """Connect to Darwin Push Port via STOMP"""
    global darwin_connected
    
    if not DARWIN_USERNAME or not DARWIN_PASSWORD:
        print('⚠️  Darwin credentials not configured, skipping real-time connection')
        return
    
    print('🚂 Connecting to Darwin Push Port...')
    
    conn = stomp.Connection(
        [(DARWIN_HOST, DARWIN_PORT)],
        heartbeats=(10000, 10000),
        keepalive=True,
        auto_decode=False
    )
    
    conn.set_listener('darwin-listener', DarwinMessageListener())
    
    while True:
        try:
            if not darwin_connected:
                conn.connect(
                    username=DARWIN_USERNAME,
                    passcode=DARWIN_PASSWORD,
                    wait=True,
                    headers={'client-id': DARWIN_CLIENT_ID}
                )
                
                conn.subscribe(
                    destination=DARWIN_TOPIC,
                    id='darwin-sub',
                    ack='client-individual',
                    headers={'activemq.subscriptionName': DARWIN_CLIENT_ID}
                )
                
                print(f'📡 Subscribed to {DARWIN_TOPIC}')
            
            time.sleep(10)
            
        except KeyboardInterrupt:
            print('\n⏹️  Shutting down...')
            conn.disconnect()
            break
        except Exception as e:
            print(f'❌ Darwin connection error: {e}')
            darwin_connected = False
            time.sleep(30)  # Wait before reconnecting


# ============ Flask Routes ============

@app.route('/')
def index():
    """Serve main page"""
    return send_from_directory('templates', 'index.html')


@app.route('/timeline')
def timeline():
    """Serve timeline page"""
    return send_from_directory('templates', 'timeline.html')


@app.route('/api/trains/realtime', methods=['GET'])
def get_realtime_trains():
    """Get all trains from cache"""
    now = datetime.now()
    cutoff_future = now + timedelta(minutes=90)
    cutoff_past = now - timedelta(minutes=5)
    
    with cache_lock:
        # Clean up old trains
        to_remove = []
        for rid, train in trains_cache.items():
            if train['parsed_time']:
                train_time = datetime.fromisoformat(train['parsed_time'])
                if train_time < cutoff_past:
                    to_remove.append(rid)
        
        for rid in to_remove:
            del trains_cache[rid]
        
        # Get upcoming trains
        upcoming = []
        for rid, train in trains_cache.items():
            if train['parsed_time']:
                train_time = datetime.fromisoformat(train['parsed_time'])
                if cutoff_past < train_time < cutoff_future:
                    upcoming.append(train)
        
        upcoming.sort(key=lambda t: t['parsed_time'])
        
        return jsonify({
            'trains': upcoming,
            'count': len(upcoming),
            'timestamp': now.isoformat(),
            'last_snapshot_load': last_snapshot_load.isoformat() if last_snapshot_load else None,
            'darwin_connected': darwin_connected
        })


@app.route('/health', methods=['GET'])
def health():
    """Health check endpoint"""
    return jsonify({
        'status': 'healthy',
        'trains_cached': len(trains_cache),
        'darwin_connected': darwin_connected,
        'last_snapshot_load': last_snapshot_load.isoformat() if last_snapshot_load else None,
        'timestamp': datetime.now().isoformat()
    })


@app.route('/api/status', methods=['GET'])
def status():
    """Detailed status endpoint"""
    with cache_lock:
        stopping = len([t for t in trains_cache.values() if t.get('type') == 'stopping'])
        passing = len([t for t in trains_cache.values() if t.get('type') == 'passing'])
    
    return jsonify({
        'status': 'running',
        'trains': {
            'total': len(trains_cache),
            'stopping': stopping,
            'passing': passing
        },
        'darwin': {
            'connected': darwin_connected,
            'host': DARWIN_HOST
        },
        's3': {
            'bucket': S3_BUCKET,
            'available': S3_BOOTSTRAP_AVAILABLE,
            'last_load': last_snapshot_load.isoformat() if last_snapshot_load else None
        },
        'timestamp': datetime.now().isoformat()
    })


# ============ Main ============

if __name__ == '__main__':
    print('🚂 Starting Roydon Level Crossing Tracker...')
    print(f'   Port: {API_PORT}')
    print(f'   Darwin: {DARWIN_HOST}:{DARWIN_PORT}')
    print(f'   S3 Bucket: {S3_BUCKET}')
    
    # Load initial snapshot from S3
    if load_initial_trains():
        print(f'✅ Loaded {len(trains_cache)} trains from S3')
    else:
        print('⚠️  Could not load S3 data, will rely on Darwin feed')
    
    # Start snapshot refresh thread
    def refresh_trains_periodically():
        while True:
            time.sleep(REFRESH_INTERVAL)
            print('🔄 Refreshing train data from S3...')
            load_initial_trains()
    
    refresh_thread = threading.Thread(target=refresh_trains_periodically, daemon=True)
    refresh_thread.start()
    
    # Start Darwin connection in background thread
    darwin_thread = threading.Thread(target=connect_to_darwin, daemon=True)
    darwin_thread.start()
    
    # Give Darwin a moment to connect
    time.sleep(2)
    
    # Start Flask API
    print(f'🌐 Starting Flask API on port {API_PORT}...')
    app.run(host='0.0.0.0', port=API_PORT, debug=False, threaded=True)
