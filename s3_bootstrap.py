#!/usr/bin/env python3
"""
Bootstrap from Darwin S3 Timetable Feed
Downloads the complete UK rail timetable including ALL passing trains
"""

import boto3
import gzip
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
import io
import os

# S3 Configuration - reads from environment variables
S3_ACCESS_KEY = os.environ.get('AWS_ACCESS_KEY_ID')
S3_SECRET_KEY = os.environ.get('AWS_SECRET_ACCESS_KEY')
S3_BUCKET = os.environ.get('S3_BUCKET', 'darwin.xmltimetable')
S3_PREFIX = 'PPTimetable/'
S3_REGION = os.environ.get('AWS_REGION', 'eu-west-1')

# Roydon station
ROYDON_TIPLOC = 'ROYDON'


class S3TimetableBootstrap:
    """Load complete timetable from S3 including passing trains"""
    
    def __init__(self, tiploc='ROYDON', hours_ahead=2):
        self.tiploc = tiploc
        self.hours_ahead = hours_ahead
        self.s3_client = None
        self.trains = []
        self.all_journeys = []  # Store all journeys for inference
    
    def connect_s3(self):
        """Connect to S3 bucket"""
        if not S3_ACCESS_KEY or not S3_SECRET_KEY:
            print('❌ AWS credentials not configured (set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY)')
            return False
        
        try:
            self.s3_client = boto3.client(
                's3',
                aws_access_key_id=S3_ACCESS_KEY,
                aws_secret_access_key=S3_SECRET_KEY,
                region_name=S3_REGION
            )
            print('✅ Connected to S3 bucket')
            return True
        except Exception as e:
            print(f'❌ Failed to connect to S3: {e}')
            return False
    
    def find_latest_timetable(self):
        """Find the latest timetable file in S3"""
        try:
            # List all files in the PPTimetable folder
            # Need to paginate to get all files
            all_files = []
            paginator = self.s3_client.get_paginator('list_objects_v2')
            
            for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=S3_PREFIX):
                if 'Contents' in page:
                    all_files.extend(page['Contents'])
            
            if not all_files:
                print('❌ No timetable files found in S3')
                return None
            
            # Files are named: YYYYMMDDHHMMSS_vX.xml.gz
            # Example: 20260113020500_v8.xml.gz
            # We want the highest version (v8) from the most recent date
            # Exclude ref_v files (they're small reference files, not full timetables)
            
            # Filter for main timetable files (exclude ref_v files)
            main_files = [f for f in all_files if '_v' in f['Key'] and 'ref_v' not in f['Key']]
            
            if not main_files:
                print('❌ No main timetable files found')
                return None
            
            print(f'   Found {len(main_files)} main timetable files')
            
            # Sort by LastModified descending - most recent file
            main_files.sort(key=lambda x: x['LastModified'], reverse=True)
            
            # Show top 5 candidates
            print('   Recent files:')
            for f in main_files[:5]:
                print(f"     {f['Key']} - {f['Size']/1024/1024:.1f} MB - {f['LastModified']}")
            
            # Get the largest file from the most recent date
            # Group by date (first 8 chars of filename after prefix)
            from collections import defaultdict
            by_date = defaultdict(list)
            
            for f in main_files:
                # Extract YYYYMMDD from filename
                filename = f['Key'].split('/')[-1]
                date = filename[:8]
                by_date[date].append(f)
            
            # Get most recent date
            most_recent_date = sorted(by_date.keys(), reverse=True)[0]
            print(f'   Using files from date: {most_recent_date}')
            
            # Get largest file from that date (highest version number = most complete)
            files_for_date = by_date[most_recent_date]
            files_for_date.sort(key=lambda x: x['Size'], reverse=True)
            latest = files_for_date[0]
            
            print(f'\n📅 Selected timetable: {latest["Key"]}')
            print(f'   Size: {latest["Size"] / 1024 / 1024:.1f} MB')
            print(f'   Last modified: {latest["LastModified"]}')
            
            return latest['Key']
            
        except Exception as e:
            print(f'❌ Error finding timetable: {e}')
            import traceback
            traceback.print_exc()
            return None
    
    def download_timetable(self, key):
        """Download and decompress timetable file"""
        try:
            print(f'⬇️  Downloading {key}...')
            
            # Download file
            response = self.s3_client.get_object(Bucket=S3_BUCKET, Key=key)
            compressed_data = response['Body'].read()
            
            print(f'📦 Downloaded {len(compressed_data) / 1024 / 1024:.1f} MB')
            
            # Decompress gzip
            print('📂 Decompressing...')
            xml_data = gzip.decompress(compressed_data)
            
            print(f'📄 Decompressed to {len(xml_data) / 1024 / 1024:.1f} MB XML')
            
            return xml_data
            
        except Exception as e:
            print(f'❌ Error downloading timetable: {e}')
            return None
    
    def parse_timetable(self, xml_data):
        """Parse timetable XML and extract trains passing through our station"""
        try:
            print('🔍 Parsing timetable XML...')
            
            # Parse XML
            root = ET.fromstring(xml_data)
            
            # The namespace is declared in the root element
            # xmlns="http://www.thalesgroup.com/rtti/XmlTimetable/v8"
            namespace = {'ns': 'http://www.thalesgroup.com/rtti/XmlTimetable/v8'}
            
            # Calculate time window
            now = datetime.now()
            today_str = now.strftime('%Y-%m-%d')  # Today's date for filtering
            cutoff = now + timedelta(hours=self.hours_ahead)
            
            trains_found = 0
            trains_at_station = 0
            
            # Iterate through all Journey elements (schedules)
            # Use findall with namespace
            journeys = root.findall('ns:Journey', namespace)
            
            if not journeys:
                # Try without namespace (fallback)
                journeys = root.findall('Journey')
            
            print(f'   Found {len(journeys)} total journeys in timetable')
            print(f'   Filtering for trains on {today_str}')
            
            # Store all journeys for inference
            self.all_journeys = journeys
            
            for journey in journeys:
                trains_found += 1
                
                # Progress indicator for large files
                if trains_found % 10000 == 0:
                    print(f'   Processed {trains_found} trains, found {trains_at_station} at {self.tiploc}...')
                
                try:
                    rid = journey.get('rid', f"timetable_{trains_found}")
                    uid = journey.get('uid', 'unknown')
                    ssd = journey.get('ssd', now.strftime('%Y-%m-%d'))
                    toc = journey.get('toc', 'unknown')
                    
                    # FILTER: Only process trains scheduled for TODAY
                    if ssd != today_str:
                        continue
                    
                    # Look through all locations in this journey
                    # Location types: OR (origin), IP (intermediate stop), PP (passing point), DT (destination)
                    roydon_location = None
                    origin = None
                    destination = None
                    
                    for loc in journey:
                        tpl = loc.get('tpl', '')
                        
                        # Track origin (first location - usually OR element)
                        if origin is None:
                            origin = tpl
                        
                        # Track destination (last location - usually DT element)
                        destination = tpl
                        
                        # Check if this is our station
                        if tpl == self.tiploc:
                            roydon_location = loc
                            # Don't break - keep going to get destination
                    
                    # If train passes through our station
                    if roydon_location is not None:
                        train_data = self.extract_train_data(
                            rid, uid, ssd, toc,
                            roydon_location, 
                            origin, destination,
                            now, cutoff
                        )
                        
                        if train_data:
                            self.trains.append(train_data)
                            trains_at_station += 1
                
                except Exception as e:
                    # Skip invalid journeys
                    continue
            
            print(f'✅ Parsed {trains_found} total trains')
            print(f'✅ Found {trains_at_station} trains explicitly at {self.tiploc}')
            
            # Now find passing trains by inference
            print(f'🔍 Detecting passing trains by route inference...')
            print(f'   Time window: {now.strftime("%H:%M:%S")} to {cutoff.strftime("%H:%M:%S")}')
            passing_trains = self.find_passing_trains_by_inference(journeys, now, cutoff)
            
            if passing_trains:
                print(f'✅ Found {len(passing_trains)} passing trains via inference')
                self.trains.extend(passing_trains)
            else:
                print(f'   No passing trains detected via inference')
                print(f'   (This might be normal if there are no passing trains in the {self.hours_ahead}-hour window)')
            
            print(f'✅ Total trains (stopping + passing): {len(self.trains)}')
            
            return self.trains
            
        except Exception as e:
            print(f'❌ Error parsing timetable: {e}')
            import traceback
            traceback.print_exc()
            return []
    
    def extract_train_data(self, rid, uid, ssd, toc, location, origin, destination, now, cutoff):
        """Extract train data from a location in the timetable"""
        # Public times (stopping trains)
        pta = location.get('pta')  # Public arrival
        ptd = location.get('ptd')  # Public departure
        
        # Working times (includes passing)
        wta = location.get('wta')  # Working arrival
        wtd = location.get('wtd')  # Working departure  
        wtp = location.get('wtp')  # Working PASS time
        
        # Determine if stopping or passing
        if pta or ptd:
            train_type = 'stopping'
            time_str = ptd or pta
        elif wtp:
            train_type = 'passing'
            time_str = wtp
        else:
            # Passing through (has working times but no pass time)
            train_type = 'passing'
            time_str = wtd or wta
        
        if not time_str:
            return None
        
        # Parse time
        train_time = self.parse_time(time_str)
        
        # Check if in our time window
        if not train_time or train_time < now or train_time > cutoff:
            return None
        
        return {
            'rid': rid,
            'uid': uid,
            'ssd': ssd,
            'toc': toc,
            'type': train_type,
            'time': time_str,
            'parsed_time': train_time.isoformat(),
            'origin': origin,
            'destination': destination,
            'platform': location.get('plat'),
            'eta': None,
            'delayed': False
        }
    
    def parse_time(self, time_str):
        """Parse time string to datetime"""
        if not time_str:
            return None
        
        try:
            # Handle HH:MM:SS or HH:MM
            parts = time_str.split(':')
            hours = int(parts[0])
            minutes = int(parts[1])
            
            now = datetime.now()
            train_time = now.replace(hour=hours, minute=minutes, second=0, microsecond=0)
            
            # If in the past, assume tomorrow
            if train_time < now - timedelta(hours=1):  # Allow 1 hour in past for trains in progress
                train_time += timedelta(days=1)
            
            return train_time
            
        except Exception:
            return None
    
    def find_passing_trains_by_inference(self, journeys, now, cutoff):
        """
        Find express trains that pass through Roydon but aren't listed.
        Two routes:
        1. Liverpool St ↔ Cambridge: via Broxbourne → Roydon → Bishops Stortford
        2. Liverpool St ↔ Stansted: via Broxbourne → Roydon → Stansted
        """
        passing_trains = []
        today_str = now.strftime('%Y-%m-%d')  # Today's date for filtering
        
        # Stations before and after Roydon
        BEFORE_STATION = 'BROXBRN'  # Broxbourne (before Roydon on both routes)
        BEFORE_STATION_ALT = 'BROXBNJ'  # Broxbourne Junction (alternative code)
        BEFORE_STATION_ALT2 = 'CHESHNT'  # Cheshunt (further before, but has times!)
        
        # Two possible "after" stations depending on route:
        CAMBRIDGE_AFTER = 'BSHPSFD'   # Bishops Stortford (Cambridge route)
        STANSTED_AFTER = 'STANAIR'    # Stansted Airport (Stansted route) - CORRECT TIPLOC!
        
        # Endpoint codes
        LIVST_CODES = ['LIVST', 'LVRPLST']
        CAMBRIDGE_CODES = ['CAMBDGE', 'CAMBNTH', 'CAMBCSN']
        STANSTED_CODES = ['STANAIR']
        ELY_CODES = ['ELY', 'ELYY']  # Ely trains also pass through Roydon
        STRATFORD_CODES = ['STFD']  # Stratford local services
        
        # Debug counters
        has_both_stations_cambridge = 0
        has_both_stations_stansted = 0
        has_both_stations_ely = 0
        has_both_stations_stratford = 0
        on_right_route = 0
        missing_roydon = 0
        has_times = 0
        in_time_window = 0
        
        for journey in journeys:
            try:
                # Get all locations
                locations = {}
                for loc in journey:
                    tpl = loc.get('tpl', '')
                    locations[tpl] = loc
                
                # Check which route by looking at ENDPOINTS (origin/destination)
                # not intermediate stations, since some trains go through both routes
                all_locs = list(locations.keys())
                origin = all_locs[0] if all_locs else None
                destination = all_locs[-1] if all_locs else None
                
                has_broxbourne = BEFORE_STATION in locations or BEFORE_STATION_ALT in locations or BEFORE_STATION_ALT2 in locations
                actual_brox_station = BEFORE_STATION if BEFORE_STATION in locations else (BEFORE_STATION_ALT if BEFORE_STATION_ALT in locations else BEFORE_STATION_ALT2)
                
                # Check if destination is Stansted
                is_stansted_route = destination in STANSTED_CODES or origin in STANSTED_CODES
                # Check if destination is Cambridge area
                is_cambridge_route = destination in CAMBRIDGE_CODES or origin in CAMBRIDGE_CODES
                # Check if destination is Ely
                is_ely_route = destination in ELY_CODES or origin in ELY_CODES
                # Check if Stratford local service
                is_stratford_route = origin in STRATFORD_CODES and destination == 'BSHPSFD'
                
                if has_broxbourne and is_stansted_route:
                    has_both_stations_stansted += 1
                    
                    # Determine direction for Stansted trains
                    is_inbound = origin in STANSTED_CODES  # Stansted → London
                    is_outbound = destination in STANSTED_CODES  # London → Stansted
                    
                    if is_outbound:
                        # LIVST → STANAIR: use Cheshunt (before) and Bishops Stortford (after)
                        before_station_for_calc = BEFORE_STATION_ALT2  # Cheshunt
                        after_station = CAMBRIDGE_AFTER  # Bishops Stortford
                    else:
                        # STANAIR → LIVST: use Bishops Stortford (before) and Liverpool Street (after)
                        # Cheshunt has no times for inbound Stansted, so use the destination!
                        before_station_for_calc = CAMBRIDGE_AFTER  # Bishops Stortford
                        after_station = 'LIVST'  # Liverpool Street - final destination has times!
                    
                    route_type = "Stansted"
                elif has_broxbourne and is_ely_route and CAMBRIDGE_AFTER in locations:
                    has_both_stations_ely += 1
                    
                    # Determine direction for Ely trains
                    is_inbound_ely = origin in ELY_CODES  # Ely → London
                    
                    if is_inbound_ely:
                        # ELY → LIVST: Bishops Stortford (before) and Liverpool Street (after)
                        before_station_for_calc = CAMBRIDGE_AFTER  # Bishops Stortford
                        after_station = 'LIVST'
                    else:
                        # LIVST → ELY: Liverpool Street (before) and Bishops Stortford (after)
                        # Fast trains don't stop at Cheshunt, so use Liverpool Street!
                        before_station_for_calc = 'LIVST'
                        after_station = CAMBRIDGE_AFTER  # Bishops Stortford
                    
                    route_type = "Ely"
                elif has_broxbourne and is_stratford_route and CAMBRIDGE_AFTER in locations:
                    has_both_stations_stratford += 1
                    
                    # STFD → BSHPSFD: Cheshunt (before) and Bishops Stortford (after)
                    before_station_for_calc = BEFORE_STATION_ALT2  # Cheshunt
                    after_station = CAMBRIDGE_AFTER  # Bishops Stortford (destination)
                    
                    route_type = "Stratford"
                elif has_broxbourne and is_cambridge_route and CAMBRIDGE_AFTER in locations:
                    has_both_stations_cambridge += 1
                    
                    # Determine direction for Cambridge trains
                    is_inbound_camb = origin in CAMBRIDGE_CODES  # Cambridge → London
                    
                    if is_inbound_camb:
                        # CAMBRIDGE → LIVST: Bishops Stortford (before) and Liverpool Street (after)
                        before_station_for_calc = CAMBRIDGE_AFTER  # Bishops Stortford
                        after_station = 'LIVST'  # Liverpool Street - final destination has times!
                    else:
                        # LIVST → CAMBRIDGE: Liverpool Street (before) and Bishops Stortford (after)
                        # Fast trains don't stop at Cheshunt, so use Liverpool Street!
                        before_station_for_calc = 'LIVST'
                        after_station = CAMBRIDGE_AFTER  # Bishops Stortford
                    
                    route_type = "Cambridge"
                else:
                    continue  # Not on any known route
                
                # Check if this is a valid route train
                has_livst = any(code in locations for code in LIVST_CODES)
                has_cambridge = any(code in locations for code in CAMBRIDGE_CODES)
                has_stansted = any(code in locations for code in STANSTED_CODES)
                has_ely = any(code in locations for code in ELY_CODES)
                has_stratford = any(code in locations for code in STRATFORD_CODES)
                
                # Must be on a valid route (Liverpool St service OR Stratford local)
                is_valid_route = (has_livst and (has_cambridge or has_stansted or has_ely)) or \
                                 (has_stratford and CAMBRIDGE_AFTER in locations)
                
                if is_valid_route:
                    on_right_route += 1
                    
                    if self.tiploc not in locations:
                        missing_roydon += 1
                        
                        # This train passes through Roydon without stopping!
                        # Use before_station_for_calc and after_station set during route detection
                        
                        # Handle Liverpool Street having two possible codes for BOTH before and after
                        # NOTE: Use 'is None' checks, not 'or', because XML elements evaluate to False!
                        if before_station_for_calc == 'LIVST':
                            before_loc = locations.get('LIVST')
                            if before_loc is None:
                                before_loc = locations.get('LVRPLST')
                        else:
                            before_loc = locations.get(before_station_for_calc)
                        
                        if after_station == 'LIVST':
                            after_loc = locations.get('LIVST')
                            if after_loc is None:
                                after_loc = locations.get('LVRPLST')
                        else:
                            after_loc = locations.get(after_station)
                        
                        if before_loc is None or after_loc is None:
                            continue
                        
                        before_time_str = before_loc.get('ptd') or before_loc.get('pta') or before_loc.get('wtd') or before_loc.get('wta')
                        after_time_str = after_loc.get('pta') or after_loc.get('ptd') or after_loc.get('wta') or after_loc.get('wtd')
                        
                        if not before_time_str or not after_time_str:
                            continue
                        
                        # Parse times
                        before_time = self.parse_time(before_time_str)
                        after_time = self.parse_time(after_time_str)
                        
                        if not before_time or not after_time:
                            continue
                        
                        has_times += 1
                        
                        # Calculate pass time (midpoint between before and after stations)
                        if before_time < after_time:
                            pass_time = before_time + timedelta(minutes=(after_time - before_time).total_seconds() / 60 / 2)
                        else:
                            pass_time = after_time + timedelta(minutes=(before_time - after_time).total_seconds() / 60 / 2)
                        
                        # Apply calibration offsets based on route type
                        # (Derived from comparison with RealTimeTrains actual pass times)
                        is_outbound = destination in STANSTED_CODES or destination in CAMBRIDGE_CODES or destination in ELY_CODES or destination == 'BSHPSFD'
                        is_inbound = origin in STANSTED_CODES or origin in CAMBRIDGE_CODES or origin in ELY_CODES
                        
                        if route_type == "Stansted" and is_outbound:
                            # LIVST → STANAIR: We estimate ~3 min late
                            pass_time = pass_time - timedelta(minutes=3)
                        elif route_type == "Stansted" and is_inbound:
                            # STANAIR → LIVST: We estimate ~5 min early
                            pass_time = pass_time + timedelta(minutes=5)
                        elif route_type == "Cambridge" and is_inbound:
                            # CAMBNTH → LIVST: We estimate ~8 min late
                            pass_time = pass_time - timedelta(minutes=8)
                        elif route_type == "Cambridge" and is_outbound:
                            # LIVST → CAMBNTH: Using LIVST-BSHPSFD, calibrated from RTT (was -4min)
                            pass_time = pass_time + timedelta(minutes=9)
                        elif route_type == "Ely" and is_inbound:
                            # ELY → LIVST: Similar to Cambridge inbound
                            pass_time = pass_time - timedelta(minutes=8)
                        elif route_type == "Ely" and is_outbound:
                            # LIVST → ELY: Same as outbound Cambridge
                            pass_time = pass_time + timedelta(minutes=9)
                        elif route_type == "Stratford":
                            # STFD → BSHPSFD: Local service - calibrated from RTT comparison
                            pass_time = pass_time - timedelta(minutes=8)
                        
                        # Check if in our time window
                        if pass_time < now or pass_time > cutoff:
                            continue
                        
                        in_time_window += 1
                        
                        # origin and destination already set above
                        # (removed duplicate code)
                        
                        # Create train record
                        rid = journey.get('rid', f"inferred_{pass_time.strftime('%H%M')}")
                        uid = journey.get('uid', 'unknown')
                        ssd = journey.get('ssd', now.strftime('%Y-%m-%d'))
                        toc = journey.get('toc', 'unknown')
                        
                        # FILTER: Only include trains scheduled for TODAY
                        if ssd != today_str:
                            continue
                        
                        passing_trains.append({
                            'rid': rid,
                            'uid': uid,
                            'ssd': ssd,
                            'toc': toc,
                            'type': 'passing',
                            'time': pass_time.strftime('%H:%M'),
                            'parsed_time': pass_time.isoformat(),
                            'origin': origin,
                            'destination': destination,
                            'platform': None,
                            'eta': None,
                            'delayed': False
                        })
                    
            except Exception as e:
                # Skip invalid journeys silently
                continue
        
        # Print summary (kept for useful monitoring)
        print(f'   Detected: Cambridge={has_both_stations_cambridge}, Stansted={has_both_stations_stansted}, Ely={has_both_stations_ely}, Stratford={has_both_stations_stratford}')
        print(f'   With times: {has_times}, In time window: {in_time_window}')
        
        # Deduplicate by RID (some journeys appear twice in timetable)
        seen_rids = set()
        unique_passing_trains = []
        for train in passing_trains:
            if train['rid'] not in seen_rids:
                seen_rids.add(train['rid'])
                unique_passing_trains.append(train)
        
        return unique_passing_trains
    
    def load(self):
        """Main method to load timetable"""
        print(f'📥 Loading timetable from S3 for {self.tiploc}...')
        
        # Connect to S3
        if not self.connect_s3():
            return []
        
        # Find latest timetable
        timetable_key = self.find_latest_timetable()
        if not timetable_key:
            return []
        
        # Download timetable
        xml_data = self.download_timetable(timetable_key)
        if not xml_data:
            return []
        
        # Parse and extract trains
        trains = self.parse_timetable(xml_data)
        
        return trains


def bootstrap_from_s3(tiploc='ROYDON', hours_ahead=2):
    """
    Load trains from S3 timetable
    Returns list of train dictionaries
    """
    loader = S3TimetableBootstrap(tiploc, hours_ahead)
    return loader.load()


if __name__ == '__main__':
    # Test the bootstrap
    print('🚂 Testing S3 Timetable Bootstrap\n')
    
    trains = bootstrap_from_s3(tiploc='ROYDON', hours_ahead=2)
    
    print(f'\n📊 Bootstrap Results:')
    print(f'   Total trains: {len(trains)}')
    
    if trains:
        stopping = [t for t in trains if t['type'] == 'stopping']
        passing = [t for t in trains if t['type'] == 'passing']
        
        print(f'   Stopping: {len(stopping)}')
        print(f'   Passing: {len(passing)}')
        
        print(f'\n🚂 Sample trains:')
        for train in trains[:10]:
            print(f"   {train['time']} - {train['type']:8s} {train['origin']:8s} → {train['destination']:8s} ({train['toc']})")
