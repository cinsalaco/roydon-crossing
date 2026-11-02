import os, json, sys
os.environ.setdefault('RTT_TOKEN', 'test')
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from app import parse_lineup, parse_service

def loc(desc, long_code, short=None):
    return {'location': {'namespace': 'gb-nr', 'description': desc,
                         'longCodes': [long_code], 'shortCodes': [short] if short else []}}

def meta(identity, headcode, op='LE', raq=False, passenger=True):
    return {'uniqueIdentity': f'gb-nr:{identity}:2026-11-02', 'identity': identity,
            'departureDate': '2026-11-02', 'trainReportingIdentity': headcode,
            'operator': {'code': op, 'name': 'Greater Anglia'}, 'inPassengerService': passenger,
            'runsAsRequired': raq, 'stpIndicator': 'WTT'}

payload = {'services': [
    # 1. stopping train, on time
    {'scheduleMetadata': meta('P49509', '2S19'),
     'origin': [loc('Bishops Stortford', 'BSHPSFD', 'BIS')], 'destination': [loc('Stratford (London)', 'STFD', 'SRA')],
     'locationMetadata': {'platform': {'planned': '1'}},
     'temporalData': {'displayAs': 'CALL', 'scheduledCallType': 'ADVERTISED_OPEN',
                      'arrival': {'scheduleAdvertised': '2026-11-02T22:05:00Z', 'scheduleInternal': '2026-11-02T22:04:30Z',
                                  'realtimeForecast': '2026-11-02T22:05:00Z'},
                      'departure': {'scheduleAdvertised': '2026-11-02T22:05:00Z', 'scheduleInternal': '2026-11-02T22:05:30Z',
                                    'realtimeForecast': '2026-11-02T22:05:00Z'}}},
    # 2. passing train, on time (WTT only, half minute)
    {'scheduleMetadata': meta('P46858', '1B35'),
     'origin': [loc('Stansted Airport', 'STANAIR', 'SSD')], 'destination': [loc('London Liverpool Street', 'LIVST', 'LST')],
     'temporalData': {'displayAs': None, 'scheduledCallType': None,
                      'pass': {'scheduleInternal': '2026-11-02T22:10:30Z', 'realtimeForecast': '2026-11-02T22:10:45Z'}}},
    # 3. passing train, 6 min late
    {'scheduleMetadata': meta('P48320', '2H19'),
     'origin': [loc('Cambridge North', 'CAMBNTH')], 'destination': [loc('London Liverpool Street', 'LIVST', 'LST')],
     'temporalData': {'displayAs': 'PASS',
                      'pass': {'scheduleInternal': '2026-11-02T22:21:00Z', 'realtimeForecast': '2026-11-02T22:27:15Z',
                               'realtimeInternalLateness': 6}}},
    # 4. cancelled call
    {'scheduleMetadata': meta('P49489', '2S14'),
     'origin': [loc('Stratford (London)', 'STFD')], 'destination': [loc('Bishops Stortford', 'BSHPSFD')],
     'temporalData': {'displayAs': 'CANCELLED',
                      'departure': {'scheduleAdvertised': '2026-11-02T22:36:00Z', 'isCancelled': True}}},
    # 5. runs-as-required freight, not activated -> should be dropped
    {'scheduleMetadata': meta('C12345', '6M04', op='ZZ', raq=True, passenger=False),
     'origin': [loc('Barrington Pad Gbrf', 'BRNGTNP')], 'destination': [loc('Willesden Euroterminal', 'WLSDNET')],
     'temporalData': {'displayAs': 'PASS', 'pass': {'scheduleInternal': '2026-11-02T22:40:00Z'}}},
    # 6. runs-as-required freight, activated (has realtime) -> keep, 4 min late
    {'scheduleMetadata': meta('C12346', '4L73', op='ZZ', raq=True, passenger=False),
     'origin': [loc('Tinsley Intermodal', 'TINSLYI')], 'destination': [loc('London Gateway', 'LONGATE')],
     'temporalData': {'displayAs': 'PASS', 'pass': {'scheduleInternal': '2026-11-02T22:45:30Z',
                                                    'realtimeForecast': '2026-11-02T22:49:00Z'}}},
    # 7. train that already passed (actual time) - parsed but the API route filters by window
    {'scheduleMetadata': meta('P40000', '1B30'),
     'origin': [loc('London Liverpool Street', 'LIVST', 'LST')], 'destination': [loc('Stansted Airport', 'STANAIR', 'SSD')],
     'temporalData': {'displayAs': 'PASS', 'pass': {'scheduleInternal': '2026-11-02T21:50:00Z',
                                                    'realtimeActual': '2026-11-02T21:51:00Z'}}},
]}

trains = parse_lineup(payload)
print(f'{len(trains)} trains parsed (expected 5)\n')
for t in sorted(trains.values(), key=lambda t: t['parsed_time']):
    print(f"{t['time']}  eta={t['eta']!s:5} delayed={t['delayed']!s:5} {t['type']:8} {t['origin']:8} -> {t['destination']:8} {t['headcode']} actual={t['actual']}")

assert len(trains) == 5
assert trains['gb-nr:P49509:2026-11-02']['type'] == 'stopping' and trains['gb-nr:P49509:2026-11-02']['eta'] is None
assert trains['gb-nr:P46858:2026-11-02']['time'] == '22:10' and trains['gb-nr:P46858:2026-11-02']['delayed'] is False
assert trains['gb-nr:P48320:2026-11-02']['eta'] == '22:27' and trains['gb-nr:P48320:2026-11-02']['delayed'] is True
assert 'gb-nr:P49489:2026-11-02' not in trains
assert 'gb-nr:C12345:2026-11-02' not in trains
assert trains['gb-nr:C12346:2026-11-02']['eta'] == '22:49'
assert trains['gb-nr:P40000:2026-11-02']['actual'] is True
print('\nAll assertions passed')
print('\nSample record for the front-end:')
print(json.dumps(trains['gb-nr:P48320:2026-11-02'], indent=2))
