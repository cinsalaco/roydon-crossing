#!/usr/bin/env python3
"""
Memory-Optimised Bootstrap from Darwin S3 Timetable Feed
Uses streaming XML parsing to handle large files without running out of memory
"""

import boto3
import gzip
import io
import os
from datetime import datetime, timedelta
from xml.etree.ElementTree import iterparse

# S3 Configuration - reads from environment variables
S3_ACCESS_KEY = os.environ.get('AWS_ACCESS_KEY_ID')
S3_SECRET_KEY = os.environ.get('AWS_SECRET_ACCESS_KEY')
S3_BUCKET = os.environ.get('S3_BUCKET', 'darwin.xmltimetable')
S3_PREFIX = 'PPTimetable/'
S3_REGION = os.environ.get('AWS_REGION', 'eu-west-1')

# Roydon station
ROYDON_TIPLOC = 'ROYDON'

# Stations for route detection
STANSTED_CODES = ['STANSTD', 'STANAIR', 'SSTEDAP']
CAMBRIDGE_CODES = ['CAMBDGE', 'CAMBNTH', 'CAMBCSN']
ELY_CODES = ['ELY', 'ELYY']
STRATFORD_CODES = ['STFD']
LONDON_CODES = ['LIVST', 'LVRPLST']
BROXBOURNE = 'BROXBRN'
BISHOPS_STORTFORD = 'BSHPSFD'
CHESHUNT = 'CHESHNT'

# Calibration offsets (minutes)
CALIBRATION = {
    'stansted_out': -3,
    'stansted_in': 5,
    'cambridge_out': 9,
    'cambridge_in': -8,
    'ely_out': 9,
    'ely_in': -8,
    'stratford': -8,
}


class S3TimetableLoader:
    """Memory-efficient timetable loader using streaming XML parsing"""
    
    def __init__(self, tiploc='ROYDON', hours_ahead=2):
        self.tiploc = tiploc
        self.hours_ahead = hours_ahead
        self.s3_client = None
    
    def connect_s3(self):
        """Connect to S3"""
        if not S3_ACCESS_KEY or not S3_SECRET_KEY:
            print('❌ AWS credentials not configured')
            return False
        
        try:
            self.s3_client = boto3.client(
                's3',
                aws_access_key_id=S3_ACCESS_KEY,
                aws_secret_access_key=S3_SECRET_KEY,
                region_name=S3_REGION
            )
            print('✅ Connected to S3')
            return True
        except Exception as e:
            print(f'❌ S3 error: {e}')
            return False
    
    def find_latest_timetable(self):
        """Find latest timetable file"""
        try:
            response = self.s3_client.list_objects_v2(
                Bucket=S3_BUCKET,
                Prefix=S3_PREFIX
            )
            
            files = [obj['Key'] for obj in response.get('Contents', []) 
                    if obj['Key'].endswith('.gz')]
            
            if not files:
                print('❌ No timetable files found')
                return None
            
            latest = sorted(files)[-1]
            print(f'📄 Using: {latest}')
            return latest
            
        except Exception as e:
            print(f'❌ Error: {e}')
            return None
    
    def parse_time(self, time_str):
        """Parse time string to datetime"""
        if not time_str:
            return None
        try:
            parts = time_str.split(':')
            hours = int(parts[0])
            minutes = int(parts[1])
            
            now = datetime.now()
            train_time = now.replace(hour=hours, minute=minutes, second=0, microsecond=0)
            
            if train_time < now - timedelta(hours=1):
                train_time += timedelta(days=1)
            
            return train_time
        except:
            return None
    
    def load(self):
        """Load trains using streaming parser"""
        if not self.connect_s3():
            return []
        
        timetable_key = self.find_latest_timetable()
        if not timetable_key:
            return []
        
        now = datetime.now()
        cutoff = now + timedelta(hours=self.hours_ahead)
        today_str = now.strftime('%Y-%m-%d')
        
        trains = []
        stopping_count = 0
        passing_count = 0
        journeys_processed = 0
        
        print(f'🔄 Streaming timetable (this may take a minute)...')
        print(f'   Looking for trains between {now.strftime("%H:%M")} and {cutoff.strftime("%H:%M")}')
        
        try:
            # Download and decompress in memory
            response = self.s3_client.get_object(Bucket=S3_BUCKET, Key=timetable_key)
            compressed = response['Body'].read()
            print(f'   Downloaded {len(compressed) / 1024 / 1024:.1f}MB compressed')
            
            # Decompress
            xml_data = gzip.decompress(compressed)
            print(f'   Decompressed to {len(xml_data) / 1024 / 1024:.1f}MB')
            
            # Free compressed data
            del compressed
            
            # Use iterparse for memory-efficient parsing
            xml_stream = io.BytesIO(xml_data)
            
            # Track journeys for passing train inference
            roydon_area_journeys = []
            
            for event, elem in iterparse(xml_stream, events=['end']):
                # Only process Journey elements
                if not elem.tag.endswith('Journey'):
                    continue
                
                journeys_processed += 1
                
                if journeys_processed % 10000 == 0:
                    print(f'   Processed {journeys_processed} journeys...')
                
                try:
                    ssd = elem.get('ssd', '')
                    if ssd != today_str:
                        elem.clear()
                        continue
                    
                    rid = elem.get('rid', f'j{journeys_processed}')
                    uid = elem.get('uid', '')
                    toc = elem.get('toc', '')
                    
                    # Collect location data
                    locations = {}
                    loc_list = []
                    has_roydon = False
                    has_broxbourne = False
                    has_livst = False
                    has_bshpsfd = False
                    origin = None
                    destination = None
                    
                    for loc in elem:
                        tpl = loc.get('tpl', '')
                        if not tpl:
                            continue
                        
                        if origin is None:
                            origin = tpl
                        destination = tpl
                        loc_list.append(tpl)
                        
                        locations[tpl] = {
                            'pta': loc.get('pta'),
                            'ptd': loc.get('ptd'),
                            'wta': loc.get('wta'),
                            'wtd': loc.get('wtd'),
                            'wtp': loc.get('wtp'),
                        }
                        
                        if tpl == self.tiploc:
                            has_roydon = True
                        elif tpl == BROXBOURNE:
                            has_broxbourne = True
                        elif tpl in LONDON_CODES:
                            has_livst = True
                        elif tpl == BISHOPS_STORTFORD:
                            has_bshpsfd = True
                    
                    # STOPPING trains at Roydon
                    if has_roydon:
                        loc_data = locations.get(self.tiploc, {})
                        pta = loc_data.get('pta')
                        ptd = loc_data.get('ptd')
                        wtp = loc_data.get('wtp')
                        
                        if pta or ptd:
                            time_str = ptd or pta
                            train_time = self.parse_time(time_str)
                            
                            if train_time and now <= train_time <= cutoff:
                                trains.append({
                                    'rid': rid,
                                    'uid': uid,
                                    'ssd': ssd,
                                    'toc': toc,
                                    'type': 'stopping',
                                    'time': time_str,
                                    'parsed_time': train_time,
                                    'origin': origin,
                                    'destination': destination,
                                })
                                stopping_count += 1
                    
                    # Store journey for PASSING train inference
                    elif has_broxbourne and has_livst and has_bshpsfd:
                        # This train goes through Roydon area
                        roydon_area_journeys.append({
                            'rid': rid,
                            'uid': uid,
                            'ssd': ssd,
                            'toc': toc,
                            'origin': origin,
                            'destination': destination,
                            'locations': locations,
                        })
                
                except Exception as e:
                    pass
                
                # CRITICAL: Clear element to free memory
                elem.clear()
            
            # Free XML data
            del xml_data
            
            print(f'✅ Processed {journeys_processed} journeys')
            print(f'   Found {stopping_count} stopping trains')
            print(f'   Found {len(roydon_area_journeys)} potential passing routes')
            
            # Infer passing trains
            passing_trains = self.infer_passing_trains(roydon_area_journeys, now, cutoff)
            trains.extend(passing_trains)
            passing_count = len(passing_trains)
            
            print(f'✅ Inferred {passing_count} passing trains')
            print(f'✅ Total: {len(trains)} trains')
            
            # Sort by time
            trains.sort(key=lambda t: t.get('parsed_time') or datetime.max)
            
            # Convert datetime to ISO string for JSON
            for train in trains:
                if train.get('parsed_time'):
                    train['parsed_time'] = train['parsed_time'].isoformat()
            
            return trains
            
        except Exception as e:
            print(f'❌ Parse error: {e}')
            import traceback
            traceback.print_exc()
            return []
    
    def infer_passing_trains(self, journeys, now, cutoff):
        """Infer passing trains from route data"""
        passing = []
        
        for j in journeys:
            origin = j['origin']
            destination = j['destination']
            locations = j['locations']
            
            # Determine route type and direction
            is_stansted = origin in STANSTED_CODES or destination in STANSTED_CODES
            is_cambridge = origin in CAMBRIDGE_CODES or destination in CAMBRIDGE_CODES
            is_ely = origin in ELY_CODES or destination in ELY_CODES
            is_stratford = origin in STRATFORD_CODES and destination == BISHOPS_STORTFORD
            
            if not (is_stansted or is_cambridge or is_ely or is_stratford):
                continue
            
            # Determine direction
            is_inbound = (origin in STANSTED_CODES or origin in CAMBRIDGE_CODES or 
                         origin in ELY_CODES)
            
            # Select stations for time interpolation
            if is_stansted:
                route_type = 'stansted'
                if is_inbound:
                    before_stn, after_stn = BISHOPS_STORTFORD, 'LIVST'
                    cal_key = 'stansted_in'
                else:
                    before_stn, after_stn = CHESHUNT, BISHOPS_STORTFORD
                    cal_key = 'stansted_out'
            elif is_cambridge or is_ely:
                route_type = 'cambridge' if is_cambridge else 'ely'
                if is_inbound:
                    before_stn, after_stn = BISHOPS_STORTFORD, 'LIVST'
                    cal_key = f'{route_type}_in'
                else:
                    before_stn, after_stn = 'LIVST', BISHOPS_STORTFORD
                    cal_key = f'{route_type}_out'
            elif is_stratford:
                route_type = 'stratford'
                before_stn, after_stn = CHESHUNT, BISHOPS_STORTFORD
                cal_key = 'stratford'
            else:
                continue
            
            # Get times from stations
            before_loc = locations.get(before_stn) or locations.get('LVRPLST')
            after_loc = locations.get(after_stn) or locations.get('LVRPLST')
            
            if not before_loc or not after_loc:
                continue
            
            before_time_str = (before_loc.get('ptd') or before_loc.get('pta') or 
                              before_loc.get('wtd') or before_loc.get('wta'))
            after_time_str = (after_loc.get('pta') or after_loc.get('ptd') or 
                             after_loc.get('wta') or after_loc.get('wtd'))
            
            if not before_time_str or not after_time_str:
                continue
            
            before_time = self.parse_time(before_time_str)
            after_time = self.parse_time(after_time_str)
            
            if not before_time or not after_time:
                continue
            
            # Calculate midpoint
            if before_time < after_time:
                mid_seconds = (after_time - before_time).total_seconds() / 2
                pass_time = before_time + timedelta(seconds=mid_seconds)
            else:
                mid_seconds = (before_time - after_time).total_seconds() / 2
                pass_time = after_time + timedelta(seconds=mid_seconds)
            
            # Apply calibration
            calibration_mins = CALIBRATION.get(cal_key, 0)
            pass_time += timedelta(minutes=calibration_mins)
            
            # Check time window
            if pass_time < now or pass_time > cutoff:
                continue
            
            time_str = pass_time.strftime('%H:%M')
            
            passing.append({
                'rid': f"P{j['rid']}",
                'uid': j['uid'],
                'ssd': j['ssd'],
                'toc': j['toc'],
                'type': 'passing',
                'time': time_str,
                'parsed_time': pass_time,
                'origin': origin,
                'destination': destination,
            })
        
        return passing


# Compatibility alias
S3TimetableBootstrap = S3TimetableLoader


def bootstrap_from_s3(tiploc='ROYDON', hours_ahead=2):
    """Load trains from S3 timetable"""
    loader = S3TimetableLoader(tiploc, hours_ahead)
    return loader.load()


if __name__ == '__main__':
    print('🚂 Testing S3 Timetable Loader\n')
    trains = bootstrap_from_s3()
    
    if trains:
        stopping = [t for t in trains if t['type'] == 'stopping']
        passing = [t for t in trains if t['type'] == 'passing']
        
        print(f'\n📊 Results:')
        print(f'   Stopping: {len(stopping)}')
        print(f'   Passing: {len(passing)}')
        
        print(f'\n🚂 Next 10 trains:')
        for t in trains[:10]:
            print(f"   {t['time']} {t['type']:8s} {t['origin']:8s} → {t['destination']}")
