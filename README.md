# Roydon Level Crossing Tracker 🚂

Real-time view of when Roydon level crossing will close and for how long,
including the express and freight services that pass through without stopping.

## Features

- ✅ **Single live data feed** from the Realtime Trains next-generation API
- ✅ **Passing train detection** (Stansted Express, Cambridge, Ely, freight)
- ✅ **Delays shown** with the new expected time, taken from RTT's live forecasts
- ✅ **Cancelled trains dropped** automatically
- ✅ **Brief opening alerts** between trains in busy periods
- ✅ **90-minute timeline** view of upcoming closures

## How it works

`app.py` polls `https://data.rtt.io/gb-nr/location?code=RYN&detailed=true`
every 60 seconds, keeps every service that will call at or pass Roydon in the
next two hours, and serves it from `/api/trains/realtime`. The two HTML pages
in `templates/` poll that endpoint and work out the closures client-side.

## Deployment on Railway

### 1. Get a Realtime Trains API token
Sign up at https://api-portal.rtt.io. Your token needs the `allowDetailed`
entitlement, otherwise passing trains are not returned. Check with:

```bash
curl -s -H "Authorization: Bearer YOUR_TOKEN" https://data.rtt.io/api/info
```

### 2. Create a Railway project from this repository

### 3. Set environment variables
Railway → project → service → **Variables**:

```
RTT_TOKEN=your_token
```

Optional:

```
RTT_TOKEN_TYPE=access      # or "refresh" if the portal gave you a refresh token
RTT_LOCATION=RYN           # location to track
RTT_API_VERSION=           # pin an API version, blank = latest
POLL_INTERVAL=60           # seconds
WINDOW_MINUTES=120         # lookahead fetched from RTT
```

### 4. Deploy
Railway deploys automatically on every push to `main`. Check the deploy log for
`Poll OK: N trains` and open `/api/status` on your Railway URL.

## Local development

```bash
pip install -r requirements.txt
cp env.example .env   # fill in RTT_TOKEN
export $(grep -v '^#' .env | xargs)
python app.py
```

Open http://localhost:5002. Run the parser test with `python tests_parser.py`.

## API endpoints

| Endpoint | Description |
|----------|-------------|
| `GET /` | Main crossing status page |
| `GET /timeline` | 90-minute timeline view |
| `GET /api/trains/realtime` | JSON API consumed by the pages |
| `GET /api/status` | Feed health, API version, entitlements, rate limits |
| `GET /api/debug` | Every cached train, including delay details |
| `GET /api/raw` | Last untouched RTT response (for diagnosing field mappings) |
| `GET /health` | Health check |

## Data source

- [Realtime Trains API](https://github.com/realtimetrains/api-specification) — free for personal use.
  The token must never be exposed to the browser; the pages only ever talk to this server.
