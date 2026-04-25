const { spawn } = require('child_process');
const fs = require('fs');
const os = require('os');
const path = require('path');
require('dotenv').config();
const mongoose = require('mongoose');

process.env.E2E_TEST_MODE = '1';

const app = require('../server');
const { getSecondaryConn } = require('../secondary-conn');

const DEBUG_PORT = Number(process.env.E2E_TIN_CUP_BROWSER_DEBUG_PORT || 9224);
let BASE = '';
const BROWSER_CANDIDATES = [
  process.env.E2E_BROWSER_BIN,
  'C:\\Program Files (x86)\\Microsoft\\Edge\\Application\\msedge.exe',
  'C:\\Program Files\\Microsoft\\Edge\\Application\\msedge.exe',
  'C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe',
].filter(Boolean);

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function expect(results, condition, name, detail = '') {
  results.push({ ok: Boolean(condition), name, detail });
}

function resolveBrowserPath() {
  for (const candidate of BROWSER_CANDIDATES) {
    if (candidate && fs.existsSync(candidate)) return candidate;
  }
  return null;
}

function findTinCupTrip(trips = []) {
  const list = Array.isArray(trips) ? trips : [];
  return list.find((trip) => /tin\s*cup/i.test(String((trip && trip.name) || '')))
    || list.find((trip) => /tin\s*cup/i.test(String((trip && trip.groupName) || '')))
    || list.find((trip) => /tin\s*cup/i.test(String((trip && trip.reservationNumber) || '')))
    || list.find((trip) => trip && trip.tinCupLive && typeof trip.tinCupLive === 'object')
    || null;
}

function withSecondary(pathname, secondary = false) {
  if (!secondary) return pathname;
  return pathname.includes('?') ? `${pathname}&myrtleBeach2026=true` : `${pathname}?myrtleBeach2026=true`;
}

function normalizeText(value = '') {
  return String(value || '').replace(/\s+/g, ' ').trim();
}

async function resolveTinCupTripContext() {
  for (const secondary of [false, true]) {
    const res = await fetch(`${BASE}${withSecondary('/api/trips', secondary)}`);
    if (!res.ok) continue;
    const trips = await res.json().catch(() => []);
    const found = findTinCupTrip(trips);
    if (found && found._id) {
      return { id: String(found._id), secondary };
    }
  }
  return null;
}

async function waitForBoot() {
  for (let i = 0; i < 120; i += 1) {
    try {
      const health = await fetch(`${BASE}/api/health`);
      if (health.status === 200) return true;
    } catch {}
    await sleep(500);
  }
  return false;
}

async function waitForJsonVersion() {
  for (let i = 0; i < 80; i += 1) {
    try {
      const res = await fetch(`http://127.0.0.1:${DEBUG_PORT}/json/version`);
      if (res.ok) return await res.json();
    } catch {}
    await sleep(250);
  }
  throw new Error(`Browser DevTools endpoint did not open on ${DEBUG_PORT}`);
}

async function openTarget(url) {
  const endpoint = `http://127.0.0.1:${DEBUG_PORT}/json/new?${encodeURIComponent(url)}`;
  let res;
  try {
    res = await fetch(endpoint, { method: 'PUT' });
  } catch {
    res = await fetch(endpoint);
  }
  if (!res.ok) throw new Error(`Failed to create browser target for ${url}: status=${res.status}`);
  return res.json();
}

async function closeTarget(id) {
  try {
    await fetch(`http://127.0.0.1:${DEBUG_PORT}/json/close/${id}`);
  } catch {}
}

async function withCdp(webSocketUrl, fn) {
  if (typeof WebSocket !== 'function') throw new Error('Global WebSocket is not available in this Node runtime');
  const ws = new WebSocket(webSocketUrl);
  const pending = new Map();
  const listeners = new Map();
  let nextId = 0;

  const opened = new Promise((resolve, reject) => {
    ws.addEventListener('open', resolve, { once: true });
    ws.addEventListener('error', reject, { once: true });
  });
  await opened;

  ws.addEventListener('message', (event) => {
    const msg = JSON.parse(event.data);
    if (typeof msg.id === 'number' && pending.has(msg.id)) {
      const { resolve, reject } = pending.get(msg.id);
      pending.delete(msg.id);
      if (msg.error) reject(new Error(msg.error.message || 'CDP error'));
      else resolve(msg.result);
      return;
    }
    if (msg.method && listeners.has(msg.method)) {
      for (const handler of listeners.get(msg.method)) handler(msg.params || {});
    }
  });

  function send(method, params = {}) {
    const id = ++nextId;
    ws.send(JSON.stringify({ id, method, params }));
    return new Promise((resolve, reject) => pending.set(id, { resolve, reject }));
  }

  function on(method, handler) {
    const set = listeners.get(method) || new Set();
    set.add(handler);
    listeners.set(method, set);
    return () => set.delete(handler);
  }

  try {
    return await fn({ send, on });
  } finally {
    for (const { reject } of pending.values()) reject(new Error('CDP connection closed'));
    pending.clear();
    ws.close();
  }
}

async function evalValue(send, expression) {
  const result = await send('Runtime.evaluate', {
    expression,
    awaitPromise: true,
    returnByValue: true,
  });
  return result && result.result ? result.result.value : undefined;
}

async function waitFor(send, predicateExpression, timeoutMs = 12000) {
  const started = Date.now();
  while ((Date.now() - started) < timeoutMs) {
    const ok = await evalValue(send, predicateExpression);
    if (ok) return true;
    await sleep(150);
  }
  return false;
}

async function runTinCupLiveFlow(results) {
  const target = await openTarget(`${BASE}/tin-cup/live-score-entry.html?local=1`);
  const errors = [];
  try {
    return await withCdp(target.webSocketDebuggerUrl, async ({ send, on }) => {
      let loaded = false;
      const removeLoad = on('Page.loadEventFired', () => { loaded = true; });
      const removeException = on('Runtime.exceptionThrown', (params) => {
        const details = params.exceptionDetails || {};
        const text = details.text || details.exception?.description || 'Runtime exception';
        errors.push(text);
      });
      const removeLog = on('Log.entryAdded', (params) => {
        const entry = params.entry || {};
        const textValue = entry.text || 'log error';
        if (/favicon\.ico/i.test(textValue)) return;
        if (entry.level === 'error' || entry.source === 'javascript') errors.push(textValue);
      });
      const removeConsole = on('Runtime.consoleAPICalled', (params) => {
        if (params.type === 'error' || params.type === 'assert') {
          const parts = (params.args || []).map((arg) => arg.value || arg.description || '').filter(Boolean);
          errors.push(parts.join(' ') || params.type);
        }
      });

      await send('Page.enable');
      await send('Runtime.enable');
      await send('Log.enable');
      await send('Network.enable');
      await send('Emulation.setDeviceMetricsOverride', { width: 390, height: 844, deviceScaleFactor: 2, mobile: true });
      await send('Page.addScriptToEvaluateOnNewDocument', {
        source: `(() => {
          try {
            localStorage.removeItem('tinCupLiveLocalStateV1');
            localStorage.removeItem('tinCupScoringStateV2');
            localStorage.removeItem('tinCupScorecardCodesV1');
            sessionStorage.clear();
          } catch (_err) {}
          const nativeFetch = window.fetch.bind(window);
          window.fetch = (input, init) => {
            const url = typeof input === 'string' ? input : ((input && input.url) || '');
            if (/\\/api\\/trips(?:\\?.*)?$/i.test(url)) {
              return Promise.resolve(new Response('[]', {
                status: 200,
                headers: { 'Content-Type': 'application/json' }
              }));
            }
            if (/\\/api\\/trips\\//i.test(url)) {
              return Promise.resolve(new Response('{"error":"Trip not found"}', {
                status: 404,
                headers: { 'Content-Type': 'application/json' }
              }));
            }
            return nativeFetch(input, init);
          };
          window.prompt = () => 'E2E Scorer';
        })();`
      });

      loaded = false;
      await send('Page.reload', { ignoreCache: true });
      for (let i = 0; i < 40 && !loaded; i += 1) await sleep(250);
      await sleep(800);

      const openerExists = await waitFor(send, `(() => !!document.querySelector('[data-open-scorecard="Day 1|0"]'))()`);
      expect(results, openerExists, 'Tin Cup open button rendered', openerExists ? 'Day 1 Group 1 available' : 'missing');
      if (!openerExists) return { errors };

      await evalValue(send, `(() => { document.querySelector('[data-open-scorecard="Day 1|0"]').click(); return true; })()`);
      const opened = await waitFor(send, `(() => {
        const holeCard = document.getElementById('holeEntryCard');
        const status = document.getElementById('status');
        return holeCard && holeCard.style.display === 'block' && /opened/i.test((status && status.textContent) || '');
      })()`);
      expect(results, opened, 'Tin Cup scorecard opens', opened ? 'hole entry visible' : 'scorecard did not open');

      const openingState = await evalValue(send, `(() => ({
        title: document.getElementById('scorecardTitle')?.textContent || '',
        holeLabel: document.querySelector('.hole-nav-hole')?.textContent || '',
        mobile: document.body.classList.contains('mobile-scorecard-active')
      }))()`);
      expect(results, /Day 1/.test(openingState.title || ''), 'Tin Cup title populated', openingState.title || 'missing');
      expect(results, /Hole 1/.test(openingState.holeLabel || ''), 'Tin Cup starts on current hole', openingState.holeLabel || 'missing');
      expect(results, openingState.mobile === true, 'Tin Cup mobile scorecard mode toggled', String(openingState.mobile));

      await evalValue(send, `(() => { const btn = document.querySelector('[data-toggle-full-scorecard="1"]'); if (btn) btn.click(); return true; })()`);
      const fullShown = await waitFor(send, `(() => document.getElementById('scorecardCard')?.style.display === 'block')()`);
      expect(results, fullShown, 'Tin Cup full scorecard opens', fullShown ? 'scorecard panel visible' : 'scorecard panel hidden');

      await evalValue(send, `(() => { const btn = document.getElementById('hideFullScorecardBtn'); if (btn) btn.click(); return true; })()`);
      const fullHidden = await waitFor(send, `(() => document.getElementById('scorecardCard')?.style.display === 'none')()`);
      expect(results, fullHidden, 'Tin Cup full scorecard hides', fullHidden ? 'scorecard panel hidden' : 'scorecard panel still visible');

      await evalValue(send, `saveHole('Matt', 1, 4).then(() => true)`);
      const scoreSaved = await waitFor(send, `(() => {
        const row = Array.from(document.querySelectorAll('.hole-player-row')).find((node) => /Matt/.test(node.innerText || ''));
        const value = row && row.querySelector('.hole-current-value');
        const status = document.getElementById('status');
        return value && value.textContent.trim() === '4' && /Saved Matt H1/.test((status && status.textContent) || '');
      })()`);
      const savedDetail = await evalValue(send, `(() => document.getElementById('status')?.textContent || '')()`);
      expect(results, scoreSaved, 'Tin Cup hole score updates in UI', scoreSaved ? 'Matt H1 = 4' : (savedDetail || 'save state missing')); 

      await evalValue(send, `(() => { currentHole = 3; renderHoleEntry(active.view); renderMarkers(active.view); return true; })()`);
      const hole3 = await waitFor(send, `(() => /Hole 3/.test(document.querySelector('.hole-nav-hole')?.textContent || ''))()`);
      expect(results, hole3, 'Tin Cup hole navigation advances', hole3 ? 'navigated to hole 3' : 'did not reach hole 3');

      const ctpVisible = await waitFor(send, `(() => /Closest To Pin/i.test(document.getElementById('markerTableWrap')?.innerText || ''))()`);
      expect(results, ctpVisible, 'Tin Cup CTP marker appears on par 3', ctpVisible ? 'marker visible on hole 3' : 'marker missing');

      await evalValue(send, `saveMarker('ctp', 3, 'Matt').then(() => true)`);
      const ctpSaved = await waitFor(send, `(() => {
        const input = document.querySelector('[data-marker-input="ctp|3"]');
        const status = document.getElementById('status');
        return input && input.value === 'Matt' && /Saved CTP H3/.test((status && status.textContent) || '');
      })()`);
      const markerDetail = await evalValue(send, `(() => document.getElementById('status')?.textContent || '')()`);
      expect(results, ctpSaved, 'Tin Cup CTP marker updates in UI', ctpSaved ? 'CTP H3 = Matt' : (markerDetail || 'marker save missing')); 

      await evalValue(send, `(() => {
        const select = document.getElementById('linkDaySelect');
        if (!select) return false;
        select.value = 'Scramble';
        select.dispatchEvent(new Event('change', { bubbles: true }));
        return true;
      })()`);
      const scrambleOpener = await waitFor(send, `(() => !!document.querySelector('[data-open-scorecard="Scramble|0"]'))()`);
      expect(results, scrambleOpener, 'Tin Cup scramble opener rendered', scrambleOpener ? 'Scramble Team 1 available' : 'missing');

      await evalValue(send, `(() => { document.querySelector('[data-open-scorecard="Scramble|0"]')?.click(); return true; })()`);
      const scrambleOpened = await waitFor(send, `(() => {
        const title = document.getElementById('scorecardTitle')?.textContent || '';
        const status = document.getElementById('status')?.textContent || '';
        return /Scramble/.test(title) && /opened/i.test(status);
      })()`);
      const scrambleTitle = await evalValue(send, `(() => document.getElementById('scorecardTitle')?.textContent || '')()`);
      expect(results, scrambleOpened, 'Tin Cup scramble scoring opens in scorecard view', scrambleOpened ? scrambleTitle : 'scramble view did not open');

      await evalValue(send, `saveHole('Team 1', 1, 3).then(() => true)`);
      const scrambleSaved = await waitFor(send, `(() => {
        const status = document.getElementById('status')?.textContent || '';
        return /Saved Team 1 H1/.test(status);
      })()`);
      const scrambleDetail = await evalValue(send, `(() => document.getElementById('status')?.textContent || '')()`);
      expect(results, scrambleSaved, 'Tin Cup scramble score uses shared hole entry UI', scrambleSaved ? 'Team 1 H1 = 3' : (scrambleDetail || 'scramble save missing'));

      removeLoad();
      removeException();
      removeLog();
      removeConsole();
      expect(results, errors.length === 0, 'Tin Cup live entry console clean', errors.join(' | ') || 'no errors');
      return { errors };
    });
  } finally {
    await closeTarget(target.id);
  }
}

async function runTinCupLeaderboardFlow(results) {
  const target = await openTarget(`${BASE}/tin-cup/leaderboard-2026.html`);
  const errors = [];
  try {
    return await withCdp(target.webSocketDebuggerUrl, async ({ send, on }) => {
      let loaded = false;
      const removeLoad = on('Page.loadEventFired', () => { loaded = true; });
      const removeException = on('Runtime.exceptionThrown', (params) => {
        const details = params.exceptionDetails || {};
        errors.push(details.text || details.exception?.description || 'Runtime exception');
      });
      const removeLog = on('Log.entryAdded', (params) => {
        const entry = params.entry || {};
        if (entry.level === 'error' || entry.source === 'javascript') errors.push(entry.text || 'log error');
      });
      await send('Page.enable');
      await send('Runtime.enable');
      await send('Log.enable');
      await send('Network.enable');
      await send('Page.addScriptToEvaluateOnNewDocument', {
        source: `(() => {
          const nativeFetch = window.fetch.bind(window);
          window.fetch = (input, init) => {
            const url = typeof input === 'string' ? input : ((input && input.url) || '');
            if (/\\/api\\/trips(?:\\?.*)?$/i.test(url)) {
              return Promise.resolve(new Response('[]', {
                status: 200,
                headers: { 'Content-Type': 'application/json' }
              }));
            }
            if (/\\/api\\/trips\\//i.test(url)) {
              return Promise.resolve(new Response('{"error":"Trip not found"}', {
                status: 404,
                headers: { 'Content-Type': 'application/json' }
              }));
            }
            return nativeFetch(input, init);
          };
        })();`
      });
      loaded = false;
      await send('Page.reload', { ignoreCache: true });
      for (let i = 0; i < 40 && !loaded; i += 1) await sleep(250);
      await sleep(1200);

      const boardReady = await waitFor(send, `(() => {
        const board = document.getElementById('tripBoard');
        return !!document.querySelector('#tripBoard table') && /Matt/.test((board && board.textContent) || '');
      })()`);
      expect(results, boardReady, 'Tin Cup leaderboard renders from local state', boardReady ? 'trip board visible' : 'trip board missing');

      const markerSynced = await waitFor(send, `(() => {
        const text = document.getElementById('workbookResultsBoard')?.textContent || '';
        return /Matt/.test(text) && /CTP\\s+1\\s*\\/\\s*LD\\s+0/.test(text);
      })()`, 15000);
      expect(results, markerSynced, 'Tin Cup leaderboard reflects local marker entry', markerSynced ? 'Matt CTP win visible in audit board' : 'marker summary missing from local leaderboard');

      const refreshText = await evalValue(send, `(() => document.getElementById('refreshNote')?.textContent || '')()`);
      expect(results, /Last refresh:/i.test(refreshText || ''), 'Tin Cup leaderboard refresh note shown', refreshText || 'missing');

      removeLoad();
      removeException();
      removeLog();
      expect(results, errors.length === 0, 'Tin Cup leaderboard console clean', errors.join(' | ') || 'no errors');
      return { errors };
    });
  } finally {
    await closeTarget(target.id);
  }
}

async function runTinCupRemoteResultsConsistency(results) {
  const context = await resolveTinCupTripContext();
  expect(results, Boolean(context && context.id), 'Tin Cup linked trip available', context ? `${context.id} (${context.secondary ? 'secondary' : 'primary'})` : 'missing');
  if (!context || !context.id) return;

  const apiPath = withSecondary(`/api/trips/${context.id}/tin-cup/live/leaderboard?day=Day+1&matchDay=Day+1`, context.secondary);
  const apiRes = await fetch(`${BASE}${apiPath}`);
  const payload = await apiRes.json().catch(() => ({}));
  expect(results, apiRes.ok, 'Tin Cup leaderboard API responds', apiRes.ok ? apiPath : JSON.stringify(payload));
  if (!apiRes.ok) return;

  const totals = Array.isArray(payload.totals) ? payload.totals : [];
  const scrambleTeams = Array.isArray(payload.scramble && payload.scramble.teams) ? payload.scramble.teams : [];
  const workbookRows = Array.isArray(payload.workbookResults) ? payload.workbookResults : [];
  const payoutRows = Array.isArray(payload.payouts && payload.payouts.rows) ? payload.payouts.rows : [];
  const sampleTotal = totals[0] || null;
  const sampleScramble = scrambleTeams.find((team) => team && team.label) || scrambleTeams[0] || null;
  const sampleWorkbook = sampleTotal
    ? (workbookRows.find((row) => normalizeText(row && row.name) === normalizeText(sampleTotal.name)) || workbookRows[0] || null)
    : (workbookRows[0] || null);
  const samplePayout = payoutRows[0] || null;

  const target = await openTarget(`${BASE}/tin-cup/leaderboard-2026.html?tripId=${encodeURIComponent(context.id)}&secondary=${context.secondary ? '1' : '0'}`);
  const errors = [];
  try {
    return await withCdp(target.webSocketDebuggerUrl, async ({ send, on }) => {
      let loaded = false;
      const removeLoad = on('Page.loadEventFired', () => { loaded = true; });
      const removeException = on('Runtime.exceptionThrown', (params) => {
        const details = params.exceptionDetails || {};
        errors.push(details.text || details.exception?.description || 'Runtime exception');
      });
      const removeLog = on('Log.entryAdded', (params) => {
        const entry = params.entry || {};
        if (entry.level === 'error' || entry.source === 'javascript') errors.push(entry.text || 'log error');
      });
      const removeConsole = on('Runtime.consoleAPICalled', (params) => {
        if (params.type === 'error' || params.type === 'assert') {
          const parts = (params.args || []).map((arg) => arg.value || arg.description || '').filter(Boolean);
          errors.push(parts.join(' ') || params.type);
        }
      });

      await send('Page.enable');
      await send('Runtime.enable');
      await send('Log.enable');
      await send('Network.enable');
      await send('Emulation.setDeviceMetricsOverride', { width: 1440, height: 1200, deviceScaleFactor: 1, mobile: false });

      for (let i = 0; i < 40 && !loaded; i += 1) await sleep(250);
      await sleep(1400);

      const pageReady = await waitFor(send, `(() => {
        return !!document.querySelector('#tripBoard tbody tr')
          && !!document.querySelector('#strokeBoard tbody tr')
          && !!document.querySelector('#scrambleBoard tbody tr')
          && !!document.querySelector('#workbookResultsBoard tbody tr')
          && !!document.getElementById('readableReport');
      })()`, 20000);
      expect(results, pageReady, 'Tin Cup linked results page renders sections', pageReady ? 'trip, stroke, scramble, workbook, and report sections visible' : 'results sections missing');
      if (!pageReady) return { errors };

      const pageState = await evalValue(send, `(() => {
        const collectRows = (selector) => Array.from(document.querySelectorAll(selector)).map((row) =>
          Array.from(row.querySelectorAll('td')).map((cell) => (cell.textContent || '').replace(/\\s+/g, ' ').trim())
        );
        return {
          tripRows: collectRows('#tripBoard tbody tr'),
          strokeRows: collectRows('#strokeBoard tbody tr'),
          scrambleRows: collectRows('#scrambleBoard tbody tr'),
          workbookRows: collectRows('#workbookResultsBoard tbody tr'),
          payoutRows: collectRows('#payoutBoard tbody tr'),
          reportText: (document.getElementById('readableReport')?.textContent || '').replace(/\\s+/g, ' ').trim(),
        };
      })()`);

      expect(results, pageState.tripRows.length === totals.length, 'Tin Cup trip board row count matches API totals', `${pageState.tripRows.length}/${totals.length}`);
      expect(results, pageState.strokeRows.length === totals.length, 'Tin Cup stroke board row count matches API totals', `${pageState.strokeRows.length}/${totals.length}`);
      expect(results, pageState.scrambleRows.length === scrambleTeams.length, 'Tin Cup scramble board row count matches API teams', `${pageState.scrambleRows.length}/${scrambleTeams.length}`);
      expect(results, pageState.workbookRows.length === workbookRows.length, 'Tin Cup workbook board row count matches API audit rows', `${pageState.workbookRows.length}/${workbookRows.length}`);
      expect(results, pageState.payoutRows.length === payoutRows.length, 'Tin Cup payout board row count matches API payout rows', `${pageState.payoutRows.length}/${payoutRows.length}`);

      if (sampleTotal) {
        const tripRow = pageState.tripRows.find((row) => normalizeText(row[1]) === normalizeText(sampleTotal.name));
        const strokeRow = pageState.strokeRows.find((row) => normalizeText(row[0]) === normalizeText(sampleTotal.name));
        expect(results, Boolean(tripRow), 'Tin Cup trip board includes API sample player', sampleTotal.name);
        expect(results, Boolean(strokeRow), 'Tin Cup stroke board includes API sample player', sampleTotal.name);
        if (tripRow) {
          expect(results, tripRow[0] === String(sampleTotal.position), 'Tin Cup trip board position matches API', `${tripRow[0]} vs ${sampleTotal.position}`);
          expect(results, tripRow[2] === String(sampleTotal.match1), 'Tin Cup trip board Day 1 match points match API', `${tripRow[2]} vs ${sampleTotal.match1}`);
          expect(results, tripRow[3] === String(sampleTotal.stroke1), 'Tin Cup trip board Day 1 stroke points match API', `${tripRow[3]} vs ${sampleTotal.stroke1}`);
          expect(results, tripRow[4] === String(sampleTotal.match2), 'Tin Cup trip board Day 2 match points match API', `${tripRow[4]} vs ${sampleTotal.match2}`);
          expect(results, tripRow[5] === String(sampleTotal.match3), 'Tin Cup trip board Day 3 match points match API', `${tripRow[5]} vs ${sampleTotal.match3}`);
          expect(results, tripRow[6] === String(sampleTotal.stroke3), 'Tin Cup trip board Day 3 stroke points match API', `${tripRow[6]} vs ${sampleTotal.stroke3}`);
          expect(results, tripRow[7] === String(sampleTotal.scramble), 'Tin Cup trip board scramble points match API', `${tripRow[7]} vs ${sampleTotal.scramble}`);
          expect(results, tripRow[8] === Number(sampleTotal.penaltyTotal || 0).toFixed(2), 'Tin Cup trip board penalty total matches API', `${tripRow[8]} vs ${Number(sampleTotal.penaltyTotal || 0).toFixed(2)}`);
          expect(results, tripRow[9].includes(String(sampleTotal.day4Points || 0)), 'Tin Cup trip board Day 4 points match API', `${tripRow[9]} vs ${sampleTotal.day4Points}`);
          if (Number.isInteger(sampleTotal.day4Rank)) {
            expect(results, tripRow[9].includes(`r${sampleTotal.day4Rank}`), 'Tin Cup trip board Day 4 rank matches API', `${tripRow[9]} vs r${sampleTotal.day4Rank}`);
          }
          expect(results, tripRow[10] === String(sampleTotal.total), 'Tin Cup trip board total matches API', `${tripRow[10]} vs ${sampleTotal.total}`);
        }
        if (strokeRow) {
          expect(results, strokeRow[1] === (sampleTotal.day1Gross === null || sampleTotal.day1Gross === undefined ? '-' : String(sampleTotal.day1Gross)), 'Tin Cup stroke board Day 1 gross matches API', `${strokeRow[1]} vs ${sampleTotal.day1Gross}`);
          expect(results, strokeRow[2] === (sampleTotal.day1Net === null || sampleTotal.day1Net === undefined ? '-' : String(sampleTotal.day1Net)), 'Tin Cup stroke board Day 1 net matches API', `${strokeRow[2]} vs ${sampleTotal.day1Net}`);
          expect(results, strokeRow[3] === (sampleTotal.day3Gross === null || sampleTotal.day3Gross === undefined ? '-' : String(sampleTotal.day3Gross)), 'Tin Cup stroke board Day 3 gross matches API', `${strokeRow[3]} vs ${sampleTotal.day3Gross}`);
          expect(results, strokeRow[4] === (sampleTotal.day3Net === null || sampleTotal.day3Net === undefined ? '-' : String(sampleTotal.day3Net)), 'Tin Cup stroke board Day 3 net matches API', `${strokeRow[4]} vs ${sampleTotal.day3Net}`);
          expect(results, strokeRow[5] === (sampleTotal.day4Gross === null || sampleTotal.day4Gross === undefined ? '-' : String(sampleTotal.day4Gross)), 'Tin Cup stroke board Day 4 gross matches API', `${strokeRow[5]} vs ${sampleTotal.day4Gross}`);
          expect(results, strokeRow[6] === (sampleTotal.day4Net === null || sampleTotal.day4Net === undefined ? '-' : String(sampleTotal.day4Net)), 'Tin Cup stroke board Day 4 net matches API', `${strokeRow[6]} vs ${sampleTotal.day4Net}`);
          expect(results, strokeRow[7] === String(sampleTotal.scramble), 'Tin Cup stroke board scramble points match API', `${strokeRow[7]} vs ${sampleTotal.scramble}`);
          expect(results, strokeRow[8] === String(sampleTotal.stroke1), 'Tin Cup stroke board Day 1 stroke points match API', `${strokeRow[8]} vs ${sampleTotal.stroke1}`);
          expect(results, strokeRow[9] === String(sampleTotal.stroke3), 'Tin Cup stroke board Day 3 stroke points match API', `${strokeRow[9]} vs ${sampleTotal.stroke3}`);
          expect(results, strokeRow[10].includes(String(sampleTotal.day4Points || 0)), 'Tin Cup stroke board Day 4 points match API', `${strokeRow[10]} vs ${sampleTotal.day4Points}`);
        }
      }

      if (sampleScramble) {
        const scrambleRow = pageState.scrambleRows.find((row) => normalizeText(row[0]) === normalizeText(sampleScramble.label));
        expect(results, Boolean(scrambleRow), 'Tin Cup scramble board includes API sample team', sampleScramble.label);
        if (scrambleRow) {
          expect(results, scrambleRow[1] === normalizeText((sampleScramble.players || []).join(', ')), 'Tin Cup scramble board players match API', `${scrambleRow[1]} vs ${(sampleScramble.players || []).join(', ')}`);
          expect(results, scrambleRow[2] === String(sampleScramble.played || 0), 'Tin Cup scramble board played-hole count matches API', `${scrambleRow[2]} vs ${sampleScramble.played}`);
          expect(results, scrambleRow[3] === (sampleScramble.total === null || sampleScramble.total === undefined ? '-' : String(sampleScramble.total)), 'Tin Cup scramble board total matches API', `${scrambleRow[3]} vs ${sampleScramble.total}`);
          expect(results, scrambleRow[4] === (sampleScramble.rank ? String(sampleScramble.rank) : '-'), 'Tin Cup scramble board rank matches API', `${scrambleRow[4]} vs ${sampleScramble.rank}`);
          expect(results, scrambleRow[5] === String(sampleScramble.points || 0), 'Tin Cup scramble board points match API', `${scrambleRow[5]} vs ${sampleScramble.points}`);
        }
      }

      if (sampleWorkbook) {
        const workbookRow = pageState.workbookRows.find((row) => normalizeText(row[1]).includes(normalizeText(sampleWorkbook.name)));
        expect(results, Boolean(workbookRow), 'Tin Cup workbook audit includes API sample player', sampleWorkbook.name);
        if (workbookRow) {
          expect(results, workbookRow[0] === String(sampleWorkbook.tripPosition), 'Tin Cup workbook trip position matches API', `${workbookRow[0]} vs ${sampleWorkbook.tripPosition}`);
          expect(results, workbookRow[2] === String(sampleWorkbook.tripTotal), 'Tin Cup workbook trip total matches API', `${workbookRow[2]} vs ${sampleWorkbook.tripTotal}`);
          expect(results, workbookRow[4] === (sampleWorkbook.netRank ? String(sampleWorkbook.netRank) : '-'), 'Tin Cup workbook net rank matches API', `${workbookRow[4]} vs ${sampleWorkbook.netRank}`);
          expect(results, workbookRow[9] === Number(sampleWorkbook.finalPrize || 0).toFixed(2), 'Tin Cup workbook final prize matches API', `${workbookRow[9]} vs ${Number(sampleWorkbook.finalPrize || 0).toFixed(2)}`);
          expect(results, workbookRow[10] === Number(sampleWorkbook.payoutTotal || 0).toFixed(2), 'Tin Cup workbook payout total matches API', `${workbookRow[10]} vs ${Number(sampleWorkbook.payoutTotal || 0).toFixed(2)}`);
        }
      }

      if (samplePayout) {
        const payoutRow = pageState.payoutRows.find((row) => normalizeText(row[0]) === normalizeText(samplePayout.name));
        expect(results, Boolean(payoutRow), 'Tin Cup payout board includes API sample player', samplePayout.name);
        if (payoutRow) {
          expect(results, payoutRow[1] === String(Math.round(Number(samplePayout.finalPrize || 0))), 'Tin Cup payout final prize matches API', `${payoutRow[1]} vs ${samplePayout.finalPrize}`);
          expect(results, payoutRow[2] === String(Math.round(Number(samplePayout.ctp || 0))), 'Tin Cup payout CTP matches API', `${payoutRow[2]} vs ${samplePayout.ctp}`);
          expect(results, payoutRow[3] === String(Math.round(Number(samplePayout.longDrive || 0))), 'Tin Cup payout long-drive matches API', `${payoutRow[3]} vs ${samplePayout.longDrive}`);
          expect(results, payoutRow[4] === String(Math.round(Number(samplePayout.longPutt || 0))), 'Tin Cup payout long-putt matches API', `${payoutRow[4]} vs ${samplePayout.longPutt}`);
          expect(results, payoutRow[5] === String(Math.round(Number(samplePayout.secretSnowman || 0))), 'Tin Cup payout Secret Snowman matches API', `${payoutRow[5]} vs ${samplePayout.secretSnowman}`);
          expect(results, payoutRow[6] === String(Math.round(Number(samplePayout.skins || 0))), 'Tin Cup payout skins matches API', `${payoutRow[6]} vs ${samplePayout.skins}`);
          expect(results, payoutRow[7] === String(Math.round(Number(samplePayout.loser || 0))), 'Tin Cup payout loser amount matches API', `${payoutRow[7]} vs ${samplePayout.loser}`);
          expect(results, payoutRow[8] === String(Math.round(Number(samplePayout.total || 0))), 'Tin Cup payout total matches API', `${payoutRow[8]} vs ${samplePayout.total}`);
        }
      }

      if (sampleTotal) {
        expect(results, pageState.reportText.includes(normalizeText(sampleTotal.name)), 'Tin Cup readable report references the API leader', sampleTotal.name);
      }
      if (samplePayout) {
        expect(results, pageState.reportText.includes(normalizeText(samplePayout.name)), 'Tin Cup readable report references the API payout leader', samplePayout.name);
      }

      removeLoad();
      removeException();
      removeLog();
      removeConsole();
      expect(results, errors.length === 0, 'Tin Cup linked results console clean', errors.join(' | ') || 'no errors');
      return { errors };
    });
  } finally {
    await closeTarget(target.id);
  }
}

async function main() {
  const results = [];
  const browserPath = resolveBrowserPath();
  if (!browserPath) {
    expect(results, false, 'Browser available', 'No Edge/Chrome binary found');
    console.log(JSON.stringify({ summary: { passed: 0, failed: 1, total: 1 }, results }, null, 2));
    process.exit(1);
  }
  expect(results, true, 'Browser available', browserPath);

  const userDataDir = fs.mkdtempSync(path.join(os.tmpdir(), 'tee-time-tin-cup-browser-'));
  const server = app.listen(Number(process.env.E2E_PORT || 0));
  await new Promise((resolve) => server.once('listening', resolve));
  const port = server.address().port;
  BASE = `http://127.0.0.1:${port}`;
  const browser = spawn(browserPath, [
    '--headless=new',
    '--disable-gpu',
    '--disable-extensions',
    '--disable-component-extensions-with-background-pages',
    '--no-first-run',
    '--no-default-browser-check',
    `--remote-debugging-port=${DEBUG_PORT}`,
    `--user-data-dir=${userDataDir}`,
    'about:blank',
  ], { stdio: 'ignore' });

  try {
    const booted = await waitForBoot();
    expect(results, booted, 'Server boot', booted ? `Listening on ${port}` : `Failed to boot on ${port}`);
    if (!booted) throw new Error(`Server failed to boot on ${port}`);
    await waitForJsonVersion();
    expect(results, true, 'Browser DevTools endpoint', `Listening on ${DEBUG_PORT}`);

    await runTinCupLiveFlow(results);
    await runTinCupLeaderboardFlow(results);
    await runTinCupRemoteResultsConsistency(results);
  } finally {
    if (browser.exitCode === null && !browser.killed) browser.kill('SIGTERM');
    setTimeout(() => {
      if (browser.exitCode === null && !browser.killed) browser.kill('SIGKILL');
    }, 1200);
    server.closeAllConnections?.();
    await new Promise((resolve) => server.close(resolve));
    await mongoose.connection.close().catch(() => {});
    const secondary = getSecondaryConn();
    if (secondary) await secondary.close().catch(() => {});
    try {
      fs.rmSync(userDataDir, { recursive: true, force: true });
    } catch {}
  }

  const passed = results.filter((r) => r.ok).length;
  const failed = results.filter((r) => !r.ok).length;
  console.log(JSON.stringify({ summary: { passed, failed, total: results.length }, results }, null, 2));
  process.exit(failed ? 1 : 0);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
