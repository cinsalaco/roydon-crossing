# Roydon Level Crossing Tracker 🚂

Real-time train tracking for Roydon level crossing. Shows when the crossing will close and for how long, including both stopping and passing trains.

## Features

- ✅ **Real-time updates** via Darwin Push Port
- ✅ **Passing train detection** (Stansted Express, Cambridge, Ely services)
- ✅ **Sub-minute accuracy** through route-specific calibration
- ✅ **Brief opening alerts** between trains in busy periods
- ✅ **90-minute timeline** view of upcoming closures

## Deployment on Railway

### 1. Fork/Clone this repository

### 2. Create a new Railway project
- Go to [railway.app](https://railway.app)
- Click "New Project" → "Deploy from GitHub repo"
- Select this repository

### 3. Set environment variables in Railway
Go to your project → Variables, and add:

```
AWS_ACCESS_KEY_ID=your_aws_access_key
AWS_SECRET_ACCESS_KEY=your_aws_secret_key
AWS_REGION=eu-west-1
S3_BUCKET=darwin-s3-pushport-4a45cf7f5667
DARWIN_USERNAME=your_darwin_username
DARWIN_PASSWORD=your_darwin_password
```

### 4. Deploy!
Railway will automatically deploy when you push to main.

## Local Development

```bash
# Install dependencies
pip install -r requirements.txt

# Set environment variables
cp .env.example .env
# Edit .env with your credentials

# Run the server
python darwin_pushport.py
```

Open http://localhost:5002 in your browser.

## API Endpoints

| Endpoint | Description |
|----------|-------------|
| `GET /` | Main crossing status page |
| `GET /timeline` | 90-minute timeline view |
| `GET /api/trains/realtime` | JSON API for train data |
| `GET /health` | Health check |
| `GET /api/status` | Detailed system status |

## Data Sources

- **S3 Timetable**: Daily schedule data from Darwin S3 Push Port
- **Darwin Push Port**: Real-time updates via STOMP

## Credits

- Train data: [National Rail Open Data](https://opendata.nationalrail.co.uk/)
- Timetable: Darwin Push Port S3 feeds
