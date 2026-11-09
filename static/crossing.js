/*
 * Roydon level crossing – shared model
 *
 * One place that turns the train list from /api/trains/realtime into
 * closures, the current state, and the 90-minute strip. Used by the main
 * page, the timeline and the observation logger so they can never disagree.
 *
 * All times are JavaScript Dates with second precision, taken from the
 * ISO timestamps the backend sends (parsed_time / scheduled_iso /
 * realtime_iso). Nothing here rounds to the minute.
 */
window.Crossing = (function () {
  'use strict';

  // ---- The model's assumptions. Change them here and nowhere else. ----
  const CFG = {
    // The crossing is on the Harlow side of the platforms. RTT times the train at the
    // station, so the model first works out when the train is actually at the road:
    //   passing train            -> RTT pass time + roadAfterPassSec
    //   stopping, towards Harlow -> RTT departure + roadAfterDepSec (stops, then crosses)
    //   stopping, towards London -> RTT arrival - roadBeforeArrSec (crosses, then stops)
    // Barriers always go down before a stopping train *arrives*, so the lead is measured
    // from arrival for stopping trains and from the pass time for passing ones.
    leadSec: 120,        // barrier down this long before the RTT arrival/pass time (logged 92-128s)
    minLeadSec: 45,      // after a brief lift the barrier can come down again as late as this before the next train (logged 9 Nov: ~45-60s)
    trailSec: 15,        // ...and fully up this long after the train clears the road (logged 11-30s, Carmelo: 12-15s)
    roadAfterPassSec: 20,
    roadAfterDepSec: 40,  // barrier-up logged at departure +55/+57s for stopping trains towards Harlow (6 and 9 Nov)
    roadBeforeArrSec: 15,
    mergeGapSec: 90,     // if it would be up for less than this, it stays down (one closure)
    liftNoteMinSec: 15,  // inside a merged closure, note a possible lift if the gap is at least this (observed ~30s lifts at predicted gaps of 20-50s)
    closingSec: 60,      // "Closing" state once a closure is this close
    windowMin: 90,       // how far ahead the pages look
    staleWarnSec: 120,   // header shows "updating…" after this long without a successful poll
    staleFailSec: 240,   // grey "unavailable" screen after this long
  };

  const LONDON_NAMES = ['London Liverpool Street', 'Liverpool Street'];

  function parseIso(s) {
    if (!s) return null;
    const d = new Date(s);
    return isNaN(d.getTime()) ? null : d;
  }

  function shortName(name, code) {
    if (!name) return code || 'Unknown';
    if (LONDON_NAMES.includes(name)) return 'London';
    return name
      .replace(' (London)', '')
      .replace('Stansted Airport', 'Stansted Airport')
      .replace(/ Gbrf$| Fhh$| Frghtliner$| Gbf$/i, '');
  }

  // Normalise one API train into what the pages need.
  function normaliseTrain(t) {
    const liveAt = parseIso(t.realtime_iso);
    const schedAt = parseIso(t.scheduled_iso);
    const at = parseIso(t.parsed_time) || liveAt || schedAt;
    if (!at) return null;
    const diffMin = (liveAt && schedAt) ? Math.round((liveAt - schedAt) / 60000) : 0;
    const stops = t.type === 'stopping';
    const toLondon = LONDON_NAMES.includes(t.destination_name || '') || t.destination === 'LIVST';
    // Arrival at the platform (stopping trains only); fall back to the departure time.
    const arrAt = (stops && parseIso(t.arrival_iso)) || at;
    // When the train is actually on the road, and when the barriers start to come down.
    let roadAt, downFrom;
    if (!stops) { roadAt = at.getTime() + CFG.roadAfterPassSec * 1000; downFrom = at; }
    else if (toLondon) { roadAt = arrAt.getTime() - CFG.roadBeforeArrSec * 1000; downFrom = arrAt; }
    else { roadAt = at.getTime() + CFG.roadAfterDepSec * 1000; downFrom = arrAt; }
    return {
      rid: t.rid,
      headcode: t.headcode || null,
      at,                                  // RTT time at the station (departure for stopping trains)
      arrAt,
      roadAt: new Date(roadAt),            // best estimate of when it is on the crossing
      schedAt,
      liveAt,
      stops,
      origin: shortName(t.origin_name, t.origin),
      destination: shortName(t.destination_name, t.destination),
      toLondon,
      freight: t.passenger_service === false,
      late: diffMin > 0 ? diffMin : 0,
      early: diffMin < 0 ? -diffMin : 0,
      passed: !!t.actual,
      closeAt: new Date(downFrom.getTime() - CFG.leadSec * 1000),
      minCloseAt: new Date(downFrom.getTime() - CFG.minLeadSec * 1000),   // latest it could re-close after a brief lift
      openAt: new Date(roadAt + CFG.trailSec * 1000),
    };
  }

  // Group trains into closures. Returns closures sorted by start time.
  function buildClosures(trains) {
    const sorted = trains.slice().sort((a, b) => a.at - b.at);
    const closures = [];
    for (const tr of sorted) {
      const last = closures[closures.length - 1];
      if (last && (tr.closeAt - last.openAt) / 1000 < CFG.mergeGapSec) {
        // stays down: extend the closure. But if the signaller *could* lift it and
        // still have the minimum lead for the next train, note a possible brief lift.
        const liftSec = (tr.minCloseAt - last.openAt) / 1000;
        if (liftSec >= CFG.liftNoteMinSec) {
          last.lifts.push({ at: new Date(last.openAt), seconds: Math.round(liftSec), beforeTrain: tr });
        }
        last.trains.push(tr);
        if (tr.openAt > last.openAt) last.openAt = tr.openAt;
      } else {
        closures.push({ closeAt: tr.closeAt, openAt: tr.openAt, trains: [tr], lifts: [] });
      }
    }
    for (const c of closures) c.durationSec = Math.round((c.openAt - c.closeAt) / 1000);
    return closures;
  }

  // Alternating open/closed periods covering [now, now + window].
  function buildPeriods(closures, now, windowMin) {
    const end = new Date(now.getTime() + (windowMin || CFG.windowMin) * 60000);
    const periods = [];
    let cursor = now;
    for (const c of closures) {
      if (c.openAt <= now) continue;
      if (c.closeAt > end) break;
      const start = c.closeAt > cursor ? c.closeAt : cursor;
      if (start > cursor) periods.push({ kind: 'open', start: cursor, end: start });
      const stop = c.openAt < end ? c.openAt : end;   // clip a closure that runs past the window
      periods.push({ kind: 'closed', start, end: stop, closure: c });
      cursor = stop;
    }
    if (cursor < end) periods.push({ kind: 'open', start: cursor, end });
    for (const p of periods) p.seconds = Math.round((p.end - p.start) / 1000);
    return periods;
  }

  /*
   * The state of the crossing at `now`.
   *   kind: 'open' | 'closing' | 'closed' | 'unavailable'
   *   plus the closure it refers to and the seconds to the next change.
   */
  function stateAt(closures, now, dataAgeSec, paused) {
    // Rate-limit pause: the timetable we hold is still the best available,
    // so keep predicting from it (the pages show a banner) unless it is
    // older than the fetch window itself.
    const tooOld = dataAgeSec != null && dataAgeSec >= CFG.windowMin * 60;
    if (dataAgeSec != null && dataAgeSec >= CFG.staleFailSec && (!paused || tooOld)) {
      return { kind: 'unavailable', dataAgeSec };
    }
    let current = null, next = null;
    for (const c of closures) {
      if (c.closeAt <= now && now < c.openAt) { current = c; break; }
      if (c.closeAt > now) { next = c; break; }
    }
    if (current) {
      // inside a merged closure, are we in a gap where the barrier may lift?
      let lift = null;
      for (const l of current.lifts) {
        if (l.at <= now && now < l.beforeTrain.minCloseAt) { lift = l; break; }
      }
      const upcomingLift = current.lifts.find(l => l.at > now) || null;
      return {
        kind: 'closed',
        closure: current,
        secondsToChange: Math.max(0, Math.round((current.openAt - now) / 1000)),
        inLift: lift,
        upcomingLift,
        nextAfter: closures.find(c => c.closeAt >= current.openAt) || null,
      };
    }
    if (!next) return { kind: 'open', closure: null, secondsToChange: null };
    const secs = Math.round((next.closeAt - now) / 1000);
    return {
      kind: secs <= CFG.closingSec ? 'closing' : 'open',
      closure: next,
      secondsToChange: secs,
      nextAfter: closures.find(c => c.closeAt > next.openAt) || null,
    };
  }

  // ---- Formatting helpers shared by the pages ----
  function hhmm(d) {
    return d ? d.toLocaleTimeString('en-GB', { hour: '2-digit', minute: '2-digit' }) : '';
  }
  function hhmmss(d) {
    return d ? d.toLocaleTimeString('en-GB', { hour: '2-digit', minute: '2-digit', second: '2-digit' }) : '';
  }
  // Big countdown: minutes above 2 min, m:ss below.
  function countdown(sec) {
    if (sec == null) return '';
    if (sec >= 120) return Math.round(sec / 60) + ' min';
    const m = Math.floor(sec / 60), s = sec % 60;
    return m + ':' + String(s).padStart(2, '0');
  }
  function minutesWord(sec) {
    const m = Math.max(1, Math.round(sec / 60));
    return m + (m === 1 ? ' minute' : ' minutes');
  }
  function aboutMinutes(sec) {
    return 'about ' + minutesWord(sec);
  }
  function secondsWord(sec) {
    return sec < 90 ? 'about ' + (Math.round(sec / 10) * 10) + ' seconds' : aboutMinutes(sec);
  }

  // ---- Data loading with staleness tracking ----
  const loader = {
    lastGood: null,      // Date of last successful fetch (client clock)
    lastServerPoll: null, // Date the server last polled RTT successfully
    pausedUntil: null,    // Date live updates resume, when RTT has rate-limited us
    trains: [],
    raw: null,
    async fetch() {
      const r = await fetch('/api/trains/realtime', { cache: 'no-store' });
      if (!r.ok) throw new Error('HTTP ' + r.status);
      const data = await r.json();
      this.raw = data;
      this.trains = (data.trains || []).map(normaliseTrain).filter(Boolean);
      this.lastGood = new Date();
      this.lastServerPoll = parseIso(data.last_poll);
      this.pausedUntil = parseIso(data.paused_until);
      this.version = data.version || null;
      this.startedAt = parseIso(data.started_at);
      return data;
    },
    // Seconds since the data was last known-good, from the server's own poll
    // time when we have it, else from our last successful fetch.
    dataAgeSec(now) {
      const ref = this.lastServerPoll || this.lastGood;
      if (!ref) return null;
      return Math.max(0, Math.round((now - ref) / 1000));
    },
  };


  // What the model believed at `now`: stored with every logged observation so
  // predictions can be scored later against RTT's recorded times.
  function snapshot(now) {
    const closures = buildClosures(loader.trains);
    const st = stateAt(closures, now, loader.dataAgeSec(now));
    const c = st.closure;
    const nearest = loader.trains.slice().sort((a, b) => Math.abs(a.at - now) - Math.abs(b.at - now))[0] || null;
    return {
      state: st.kind,
      predicted_close: c ? c.closeAt.toISOString() : null,
      predicted_open: c ? c.openAt.toISOString() : null,
      closure_trains: c ? c.trains.map(t => `${t.headcode || ''} ${hhmm(t.at)} ${t.origin}>${t.destination}${t.stops ? ' stops' : ''}`).join('; ') : '',
      nearest_train: nearest ? `${nearest.headcode || ''} ${hhmm(nearest.at)} ${nearest.origin}>${nearest.destination}${nearest.stops ? ' stops' : ' passes'}${nearest.toLondon ? ' toLondon' : ' fromLondon'}` : '',
      nearest_train_at: nearest ? nearest.at.toISOString() : null,
      nearest_train_rid: nearest ? nearest.rid : null,
      nearest_train_stops: nearest ? nearest.stops : null,
      nearest_train_to_london: nearest ? nearest.toLondon : null,
      data_age_sec: loader.dataAgeSec(now),
      cfg: { lead: CFG.leadSec, trail: CFG.trailSec, merge: CFG.mergeGapSec, minLead: CFG.minLeadSec,
             roadPass: CFG.roadAfterPassSec, roadDep: CFG.roadAfterDepSec, roadArr: CFG.roadBeforeArrSec },
    };
  }

  // Post an observation (used by the log page and the home page's lift report).
  async function report(event, note, now) {
    now = now || new Date();
    const obs = { event, at: now.toISOString(), note: note || '', snapshot: snapshot(now) };
    const r = await fetch('/api/observations', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(obs) });
    if (!r.ok) throw new Error('report failed ' + r.status);
    return r.json();
  }

  return { CFG, parseIso, normaliseTrain, buildClosures, buildPeriods, stateAt,
           hhmm, hhmmss, countdown, minutesWord, aboutMinutes, secondsWord, loader, snapshot, report };
})();
