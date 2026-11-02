#!/usr/bin/env python3
"""
Roydon Level Crossing Tracker - Realtime Trains backend

Single data feed: the Realtime Trains next-generation API (https://data.rtt.io).
Polls the Roydon location line-up every POLL_INTERVAL seconds, keeps every
service that will pass or call at Roydon in the next WINDOW_MINUTES, and serves
it on the same /api/trains/realtime contract the front-end already uses.

Replaces darwin_pushport.py + s3_bootstrap.py (Darwin STOMP + S3 timetable).

Environment variables
---------------------
RTT_TOKEN          (required) Bearer token from https://api-portal.rtt.io
RTT_TOKEN_TYPE     "access" (default) or "refresh". With a refresh token the app
                   exchanges it for short-lived access tokens automatically.
RTT_LOCATION       Location code to query. Default "RYN" (Roydon).
RTT_API_VERSION    Optional. Pins the API version (sent as the Version header).
                   Leave unset to always use the latest.
POLL_INTERVAL      Seconds between polls. Default 60.
WINDOW_MINUTES     How far ahead to fetch. Default 120 (the API serves 90).
PORT               Set by Railway automatically.
"""

import os
import csv
import io
import json
import time
import uuid
import logging
import threading
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import requests
from flask import Flask, jsonify, send_from_directory, request, Response
from flask_cors import CORS

# --------------------------------------------------------------------------
# Configuration
# --------------------------------------------------------------------------

RTT_BASE_URL = 'https://data.rtt.io'
RTT_TOKEN = os.environ.get('RTT_TOKEN')
RTT_TOKEN_TYPE = os.environ.get('RTT_TOKEN_TYPE', 'access').lower()
RTT_LOCATION = os.environ.get('RTT_LOCATION', 'RYN')
RTT_API_VERSION = os.environ.get('RTT_API_VERSION')  # optional pin

POLL_INTERVAL = int(os.environ.get('POLL_INTERVAL', 60))
WINDOW_MINUTES = int(os.environ.get('WINDOW_MINUTES', 120))
API_PORT = int(os.environ.get('PORT', 5002))
DATA_DIR = os.environ.get('DATA_DIR', os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data'))
OBS_FILE = os.path.join(DATA_DIR, 'observations.json')

SERVE_WINDOW_MINUTES = 90       # what /api/trains/realtime returns
SERVE_PAST_MINUTES = 5          # keep very recent trains so the UI can finish a closure

LONDON = ZoneInfo('Europe/London')

# Codes the front-end looks for to decide "to London"; we emit TIPLOC-style
# long codes (LIVST, BSHPSFD, ...) exactly as the old backend did.
STOPPING_DISPLAY = {'CALL', 'STARTS', 'TERMINATES'}
SKIP_DISPLAY = {'CANCELLED', 'DIVERTED'}

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
log = logging.getLogger('roydon')

app = Flask(__name__, static_folder='static', template_folder='templates')
CORS(app)

# --------------------------------------------------------------------------
# Shared state
# --------------------------------------------------------------------------

state_lock = threading.Lock()
trains_cache = {}            # rid -> train dict (replaced wholesale on each good poll)
feed_ok = False
last_poll_at = None
last_poll_error = None
last_raw_response = None     # kept for /api/raw diagnostics
api_info = None              # /api/info response (version + entitlements)
rate_limit = {}              # last seen X-RateLimit-* headers

# --------------------------------------------------------------------------
# Token handling
# --------------------------------------------------------------------------

_access_token = None
_access_token_expires = None
_token_lock = threading.Lock()
_token_mode = RTT_TOKEN_TYPE          # 'access' or 'refresh'; may flip to 'refresh' on a 401
_token_last_error = None


def _base_headers(token):
    headers = {'Authorization': f'Bearer {token}', 'Accept': 'application/json'}
    if RTT_API_VERSION:
        headers['Version'] = RTT_API_VERSION
    return headers


def _exchange_refresh_token():
    """Call /api/get_access_token with RTT_TOKEN as the bearer. Returns (token, expires)."""
    now = datetime.now(timezone.utc)
    resp = requests.get(f'{RTT_BASE_URL}/api/get_access_token',
                        headers=_base_headers(RTT_TOKEN), timeout=20)
    resp.raise_for_status()
    data = resp.json()
    expires = parse_iso(data.get('validUntil')) or (now + timedelta(hours=1))
    log.info('Access token obtained, valid until %s; entitlements: %s',
             expires.isoformat(), data.get('entitlements'))
    return data['token'], expires


def get_access_token():
    """Return a usable access token, exchanging a refresh token if needed."""
    global _access_token, _access_token_expires

    if _token_mode != 'refresh':
        return RTT_TOKEN

    with _token_lock:
        now = datetime.now(timezone.utc)
        if _access_token and _access_token_expires and now < _access_token_expires - timedelta(minutes=5):
            return _access_token
        log.info('Exchanging refresh token for an access token')
        _access_token, _access_token_expires = _exchange_refresh_token()
        return _access_token


def handle_unauthorized():
    """
    Called after a 401. If we were treating RTT_TOKEN as an access token, try it
    as a refresh token instead (the portal issues either kind). Returns True if
    the mode changed and the caller should retry.
    """
    global _token_mode, _access_token, _access_token_expires, _token_last_error
    if _token_mode == 'refresh':
        _token_last_error = 'RTT returned 401 even after the refresh-token exchange'
        return False
    try:
        with _token_lock:
            _access_token, _access_token_expires = _exchange_refresh_token()
            _token_mode = 'refresh'
        log.warning('RTT_TOKEN was rejected as an access token but works as a refresh token; '
                    'switched to refresh mode (set RTT_TOKEN_TYPE=refresh to skip this check)')
        _token_last_error = None
        return True
    except Exception as exc:
        _token_last_error = f'RTT rejected the token as an access token (401) and as a refresh token ({exc})'
        log.error(_token_last_error)
        return False


# --------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------

def parse_iso(value):
    """Parse an ISO 8601 / RFC3339 string into an aware datetime (or None)."""
    if not value:
        return None
    try:
        if value.endswith('Z'):
            value = value[:-1] + '+00:00'
        dt = datetime.fromisoformat(value)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=LONDON)
        return dt
    except (ValueError, TypeError):
        return None


def to_london(dt):
    return dt.astimezone(LONDON) if dt else None


def hhmm(dt):
    """HH:MM in UK local time (seconds truncated, matching the RTT display)."""
    return to_london(dt).strftime('%H:%M') if dt else None


def code_of(location_pair):
    """Best short-ish code for an origin/destination entry, else its description."""
    loc = (location_pair or {}).get('location') or {}
    long_codes = loc.get('longCodes') or []
    short_codes = loc.get('shortCodes') or []
    if long_codes:
        return long_codes[0]
    if short_codes:
        return short_codes[0]
    return loc.get('description') or 'Unknown'


def name_of(location_pair):
    loc = (location_pair or {}).get('location') or {}
    return loc.get('description') or code_of(location_pair)


def pick_times(temporal):
    """
    From an IndividualTemporalData block return (scheduled, realtime).
    scheduled: advertised (public) time if present, else internal (WTT).
    realtime:  actual > forecast > estimate, or None.
    """
    if not temporal:
        return None, None
    scheduled = parse_iso(temporal.get('scheduleAdvertised')) or parse_iso(temporal.get('scheduleInternal'))
    realtime = (parse_iso(temporal.get('realtimeActual'))
                or parse_iso(temporal.get('realtimeForecast'))
                or parse_iso(temporal.get('realtimeEstimate')))
    return scheduled, realtime


# --------------------------------------------------------------------------
# Parsing one RTT service into the front-end's train shape
# --------------------------------------------------------------------------

def parse_service(service):
    """
    Convert one NetworkRailLocationLineUpObject into the dict the front-end
    expects. Returns None if the service should not close the crossing.
    """
    temporal = service.get('temporalData') or {}
    meta = service.get('scheduleMetadata') or {}

    display_as = temporal.get('displayAs') or 'PASS'       # null => PASS per spec
    if display_as in SKIP_DISPLAY:
        return None

    is_stopping = display_as in STOPPING_DISPLAY

    # Which activity defines "the train is on the crossing"?
    #   passing  -> pass time
    #   stopping -> departure (barriers stay down while it's at the platform);
    #               fall back to arrival for terminating services
    if is_stopping:
        activity = temporal.get('departure') or temporal.get('arrival')
        arr_sched, arr_rt = pick_times(temporal.get('arrival'))
    else:
        activity = temporal.get('pass') or temporal.get('departure') or temporal.get('arrival')
        arr_sched, arr_rt = None, None

    if not activity or activity.get('isCancelled'):
        return None

    scheduled, realtime = pick_times(activity)
    if not scheduled:
        return None

    # Runs-as-required paths (freight "Q" paths) often never run. Only count
    # them once RTT has a realtime time for them, i.e. the train is activated.
    if meta.get('runsAsRequired') and not realtime:
        return None

    effective = realtime or scheduled
    delayed = False   # running later than scheduled
    early = False     # running earlier than scheduled
    eta = None
    if realtime and hhmm(realtime) != hhmm(scheduled):
        eta = hhmm(realtime)
        delayed = realtime > scheduled
        early = realtime < scheduled

    operator = meta.get('operator') or {}
    origin = (service.get('origin') or [{}])[0]
    destination = (service.get('destination') or [{}])[0]

    rid = meta.get('uniqueIdentity') or f"{meta.get('identity')}:{meta.get('departureDate')}"

    return {
        # --- contract fields used by index.html / timeline.html ---
        'rid': rid,
        'uid': meta.get('identity'),
        'type': 'stopping' if is_stopping else 'passing',
        'time': hhmm(scheduled),
        'parsed_time': to_london(effective).isoformat(),
        'eta': eta,
        'delayed': delayed,
        'early': early,
        'origin': code_of(origin),
        'destination': code_of(destination),
        'toc': operator.get('code'),
        'platform': ((service.get('locationMetadata') or {}).get('platform') or {}).get('planned'),
        # --- extra, informational ---
        'headcode': meta.get('trainReportingIdentity'),
        'operator_name': operator.get('name'),
        'origin_name': name_of(origin),
        'destination_name': name_of(destination),
        'passenger_service': meta.get('inPassengerService'),
        'display_as': display_as,
        'scheduled_iso': to_london(scheduled).isoformat(),
        'realtime_iso': to_london(realtime).isoformat() if realtime else None,
        'actual': bool(parse_iso(activity.get('realtimeActual'))),
        'arrival_time': hhmm(arr_rt or arr_sched) if is_stopping else None,
        'lateness_min': activity.get('realtimeAdvertisedLateness') or activity.get('realtimeInternalLateness'),
        'stp': meta.get('stpIndicator'),
        'runs_as_required': bool(meta.get('runsAsRequired')),
    }


def parse_lineup(payload):
    """Parse a /gb-nr/location response into {rid: train}."""
    result = {}
    for service in payload.get('services') or []:
        try:
            train = parse_service(service)
        except Exception as exc:  # one bad service must not kill the poll
            log.warning('Could not parse service: %s', exc)
            continue
        if train:
            result[train['rid']] = train
    return result


# --------------------------------------------------------------------------
# Polling
# --------------------------------------------------------------------------

def fetch_api_info():
    global api_info
    try:
        resp = requests.get(f'{RTT_BASE_URL}/api/info', headers=_base_headers(get_access_token()), timeout=20)
        if resp.status_code == 401 and handle_unauthorized():
            resp = requests.get(f'{RTT_BASE_URL}/api/info', headers=_base_headers(get_access_token()), timeout=20)
        resp.raise_for_status()
        api_info = resp.json()
        creds = api_info.get('credentials') or {}
        log.info('RTT API version %s, entitlements %s', api_info.get('api_version'), creds.get('entitlements'))
        if 'allowDetailed' not in (creds.get('entitlements') or []):
            log.warning('Token has no allowDetailed entitlement: passing trains will NOT be returned. '
                        'Ask hello@realtimetrains.com for detailed access.')
    except Exception as exc:
        log.warning('Could not fetch /api/info: %s', exc)


def poll_once():
    global trains_cache, feed_ok, last_poll_at, last_poll_error, last_raw_response, rate_limit

    now_london = datetime.now(LONDON)
    params = {
        'code': RTT_LOCATION,
        'timeFrom': now_london.replace(microsecond=0).isoformat(),
        'timeWindow': WINDOW_MINUTES,
        'detailed': 'true',
    }
    resp = requests.get(f'{RTT_BASE_URL}/gb-nr/location',
                        headers=_base_headers(get_access_token()),
                        params=params, timeout=30)
    if resp.status_code == 401 and handle_unauthorized():
        resp = requests.get(f'{RTT_BASE_URL}/gb-nr/location',
                            headers=_base_headers(get_access_token()),
                            params=params, timeout=30)
    if resp.status_code == 401:
        raise RuntimeError(_token_last_error or 'RTT returned 401 Unauthorized - check RTT_TOKEN')

    rate_limit = {k: v for k, v in resp.headers.items() if k.lower().startswith('x-ratelimit')}

    if resp.status_code == 429:
        retry = int(resp.headers.get('Retry-After', POLL_INTERVAL))
        raise RuntimeError(f'Rate limited by RTT, retry after {retry}s')
    if resp.status_code == 204:
        payload = {'services': []}
    else:
        resp.raise_for_status()
        payload = resp.json()

    parsed = parse_lineup(payload)

    with state_lock:
        trains_cache = parsed
        last_raw_response = payload
        feed_ok = True
        last_poll_at = datetime.now(LONDON)
        last_poll_error = None

    stopping = sum(1 for t in parsed.values() if t['type'] == 'stopping')
    delayed = sum(1 for t in parsed.values() if t['delayed'])
    early = sum(1 for t in parsed.values() if t['early'])
    log.info('Poll OK: %d trains (%d stopping, %d passing, %d delayed, %d early)',
             len(parsed), stopping, len(parsed) - stopping, delayed, early)
    for t in parsed.values():
        if t['eta']:
            log.info('  %s %s %s %s -> %s sched %s now %s', 'LATE ' if t['delayed'] else 'EARLY',
                     t['headcode'], t['type'], t['origin'], t['destination'], t['time'], t['eta'])


def poll_forever():
    global feed_ok, last_poll_error
    fetch_api_info()
    while True:
        wait = POLL_INTERVAL
        try:
            poll_once()
        except Exception as exc:
            with state_lock:
                feed_ok = False
                last_poll_error = str(exc)
            log.error('Poll failed: %s', exc)
            if 'Rate limited' in str(exc):
                try:
                    wait = max(wait, int(str(exc).rsplit(' ', 1)[-1].rstrip('s')))
                except ValueError:
                    pass
        time.sleep(wait)


# --------------------------------------------------------------------------
# Flask routes (unchanged contract)
# --------------------------------------------------------------------------

@app.route('/')
def index():
    return send_from_directory('templates', 'index.html')


@app.route('/timeline')
def timeline():
    return send_from_directory('templates', 'timeline.html')


@app.route('/log')
def log_page():
    return send_from_directory('templates', 'log.html')


@app.route('/log/review')
def review_page():
    return send_from_directory('templates', 'review.html')


# --------------------------------------------------------------------------
# Observations (what you actually saw at the crossing) - for calibrating
# the lead/trail/merge assumptions. Stored as JSON under DATA_DIR.
# --------------------------------------------------------------------------

obs_lock = threading.Lock()


def _load_observations():
    try:
        with open(OBS_FILE) as f:
            return json.load(f)
    except (OSError, ValueError):
        return []


def _save_observations(obs):
    os.makedirs(DATA_DIR, exist_ok=True)
    tmp = OBS_FILE + '.tmp'
    with open(tmp, 'w') as f:
        json.dump(obs, f)
    os.replace(tmp, OBS_FILE)


@app.route('/api/observations', methods=['GET'])
def list_observations():
    with obs_lock:
        return jsonify(_load_observations())


@app.route('/api/observations', methods=['POST'])
def add_observation():
    body = request.get_json(silent=True) or {}
    if body.get('event') not in ('lights', 'down', 'train', 'up'):
        return jsonify({'error': 'event must be one of lights, down, train, up'}), 400
    obs = {
        'id': uuid.uuid4().hex[:12],
        'event': body['event'],
        'at': body.get('at') or datetime.now(LONDON).isoformat(),
        'note': (body.get('note') or '')[:200],
        'snapshot': body.get('snapshot') or {},
        'received': datetime.now(LONDON).isoformat(),
    }
    with obs_lock:
        all_obs = _load_observations()
        all_obs.append(obs)
        _save_observations(all_obs)
    log.info('Observation: %s at %s (%s)', obs['event'], obs['at'], obs['note'])
    return jsonify(obs), 201


@app.route('/api/observations/<obs_id>', methods=['DELETE'])
def delete_observation(obs_id):
    with obs_lock:
        all_obs = _load_observations()
        kept = [o for o in all_obs if o.get('id') != obs_id]
        _save_observations(kept)
    return jsonify({'deleted': len(all_obs) - len(kept)})


@app.route('/api/observations.csv', methods=['GET'])
def observations_csv():
    with obs_lock:
        all_obs = _load_observations()
    out = io.StringIO()
    w = csv.writer(out)
    w.writerow(['at', 'event', 'note', 'app_state', 'predicted_close', 'predicted_open',
                'nearest_train', 'nearest_train_at', 'closure_trains', 'data_age_sec', 'lead', 'trail', 'merge'])
    for o in all_obs:
        s = o.get('snapshot') or {}
        cfg = s.get('cfg') or {}
        w.writerow([o.get('at'), o.get('event'), o.get('note'), s.get('state'), s.get('predicted_close'),
                    s.get('predicted_open'), s.get('nearest_train'), s.get('nearest_train_at'),
                    s.get('closure_trains'), s.get('data_age_sec'), cfg.get('lead'), cfg.get('trail'), cfg.get('merge')])
    return Response(out.getvalue(), mimetype='text/csv',
                    headers={'Content-Disposition': 'attachment; filename=roydon-observations.csv'})


@app.route('/api/trains/realtime', methods=['GET'])
def get_realtime_trains():
    now = datetime.now(LONDON)
    cutoff_future = now + timedelta(minutes=SERVE_WINDOW_MINUTES)
    cutoff_past = now - timedelta(minutes=SERVE_PAST_MINUTES)

    with state_lock:
        upcoming = []
        for train in trains_cache.values():
            t = parse_iso(train['parsed_time'])
            if t and cutoff_past < t < cutoff_future:
                upcoming.append(train)
        upcoming.sort(key=lambda t: t['parsed_time'])

        return jsonify({
            'trains': upcoming,
            'count': len(upcoming),
            'timestamp': now.isoformat(),
            'feed': 'realtimetrains',
            'feed_connected': feed_ok,
            'darwin_connected': feed_ok,        # kept for older front-end builds
            'last_poll': last_poll_at.isoformat() if last_poll_at else None,
            'data_age_sec': int((now - last_poll_at).total_seconds()) if last_poll_at else None,
            'last_error': last_poll_error,
        })


@app.route('/health', methods=['GET'])
def health():
    return jsonify({
        'status': 'healthy' if feed_ok else 'degraded',
        'trains_cached': len(trains_cache),
        'feed_connected': feed_ok,
        'last_poll': last_poll_at.isoformat() if last_poll_at else None,
        'last_error': last_poll_error,
        'timestamp': datetime.now(LONDON).isoformat(),
    })


@app.route('/api/status', methods=['GET'])
def status():
    with state_lock:
        stopping = sum(1 for t in trains_cache.values() if t['type'] == 'stopping')
        delayed = sum(1 for t in trains_cache.values() if t['delayed'])
        total = len(trains_cache)
    creds = (api_info or {}).get('credentials') or {}
    return jsonify({
        'status': 'running',
        'trains': {'total': total, 'stopping': stopping, 'passing': total - stopping, 'delayed': delayed},
        'feed': {
            'source': 'realtimetrains',
            'location': RTT_LOCATION,
            'connected': feed_ok,
            'poll_interval_s': POLL_INTERVAL,
            'window_minutes': WINDOW_MINUTES,
            'last_poll': last_poll_at.isoformat() if last_poll_at else None,
            'last_error': last_poll_error,
            'api_version': (api_info or {}).get('api_version'),
            'entitlements': creds.get('entitlements'),
            'detailed_access': 'allowDetailed' in (creds.get('entitlements') or []),
            'token_mode': _token_mode,
            'token_error': _token_last_error,
            'rate_limit': rate_limit,
        },
        'timestamp': datetime.now(LONDON).isoformat(),
    })


@app.route('/api/debug', methods=['GET'])
def debug():
    with state_lock:
        all_trains = sorted(trains_cache.values(), key=lambda t: t['parsed_time'])
    return jsonify({
        'summary': {
            'total': len(all_trains),
            'stopping': sum(1 for t in all_trains if t['type'] == 'stopping'),
            'passing': sum(1 for t in all_trains if t['type'] == 'passing'),
            'delayed': sum(1 for t in all_trains if t['delayed']),
        },
        'delayed_trains': [t for t in all_trains if t['delayed']],
        'all_trains': all_trains,
        'timestamp': datetime.now(LONDON).isoformat(),
    })


@app.route('/api/raw', methods=['GET'])
def raw():
    """Last raw RTT response, untouched - for diagnosing field mappings."""
    with state_lock:
        return jsonify(last_raw_response or {})


# --------------------------------------------------------------------------
# Main
# --------------------------------------------------------------------------

def start_poller():
    if not RTT_TOKEN:
        log.error('RTT_TOKEN is not set - no train data will be loaded')
        return
    threading.Thread(target=poll_forever, daemon=True, name='rtt-poller').start()


if __name__ == '__main__':
    log.info('Starting Roydon Level Crossing Tracker (Realtime Trains feed)')
    log.info('  Location: %s  Poll: %ss  Window: %smin  Port: %s', RTT_LOCATION, POLL_INTERVAL, WINDOW_MINUTES, API_PORT)
    start_poller()
    app.run(host='0.0.0.0', port=API_PORT, debug=False, threaded=True)
