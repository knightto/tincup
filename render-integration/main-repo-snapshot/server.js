// Alert for nearly full tee times (4 days out or less, >50% full)
async function alertNearlyFullTeeTimes(groupSlug = DEFAULT_SITE_GROUP_SLUG) {
  const now = new Date();
  const fourDaysOut = new Date(now.getTime() + 4 * 24 * 60 * 60 * 1000);
  // Find all tee-time events (not team events) within next 4 days (inclusive)
  const scopedGroupSlug = normalizeGroupSlug(groupSlug);
  const events = await Event.find({ ...groupScopeFilter(scopedGroupSlug), isTeamEvent: false, date: { $gte: now, $lte: fourDaysOut } }).lean();
  let blocks = [];
  for (const ev of events) {
    if (!Array.isArray(ev.teeTimes) || !ev.teeTimes.length) continue;
    const max = 4; // max per tee time
    const fullTeeTimes = ev.teeTimes.filter(tt => Array.isArray(tt.players) && tt.players.length / max > 0.5);
    if (fullTeeTimes.length) {
      blocks.push({
        course: ev.course || 'Course',
        dateISO: fmt.dateISO(ev.date),
        dateLong: fmt.dateLong(ev.date),
        teeTimes: fullTeeTimes.map(tt => ({
          time: fmt.tee(tt.time),
          count: tt.players.length
        })),
        total: ev.teeTimes.length
      });
    }
  }
  if (!blocks.length) return { ok: true, sent: 0, message: 'No nearly full tee times' };
  // Compose email
  const rows = blocks.map(b => {
    const list = b.teeTimes.map(t => `<li><strong>${t.time}</strong> — ${t.count} of 4 spots filled</li>`).join('');
    return `<div style="margin:12px 0;padding:12px;border:1px solid #e5e7eb;border-radius:8px">
      <p style="margin:0 0 6px 0"><strong>${esc(b.course)}</strong> — ${esc(b.dateLong)} (${esc(b.dateISO)})</p>
      <p style="margin:0 0 6px 0">Tee times more than 50% full:</p>
      <ul style="margin:0 0 0 18px">${list}</ul>
      <p style="color:#b91c1c;"><strong>Consider calling the clubhouse to request an additional tee time if needed.</strong></p>
    </div>`;
  }).join('');
  const html = frame('Tee Times Nearly Full', `<p>The following tee times are more than 50% full (4 days out or less):</p>${rows}${btn('Go to Sign-up Page', buildSiteEventUrl(scopedGroupSlug))}`);
  const res = await sendEmailToAll('Alert: Tee Times Nearly Full', html, { groupSlug: scopedGroupSlug });
  return { ok: true, sent: res.sent, blocks, groupSlug: scopedGroupSlug };
}
/* server.js v3.13 — daily 5pm empty-tee reminder + manual trigger */
const fs = require('fs');
const fsp = fs.promises;
const path = require('path');
const zlib = require('zlib');
require('dotenv').config();
const express = require('express');
const mongoose = require('mongoose');
const compression = require('compression');
const { EJSON } = require('bson');
const XLSX = require('xlsx');
// Secondary connection for Myrtle Trip (kept in separate module to avoid circular requires)
const { initSecondaryConn, getSecondaryConn } = require('./secondary-conn');
initSecondaryConn();
const cors = require('cors');
const rateLimit = require('express-rate-limit');
const multer = require('multer');
const upload = multer({ storage: multer.memoryStorage(), limits: { fileSize: 5 * 1024 * 1024 } });
const MastersPool = require('./models/MastersPool');
const { importHandicapsFromCsv, parseCsv } = require('./services/handicapImportService');
const {
  buildGroupRoutePaths,
  buildTeeTimesSiteDeploymentProfile,
  buildTeeTimesSiteTemplatePackage,
} = require('./services/siteTemplateService');
const {
  inferInboundGroupRouting,
  isAllowedInboundRecipient,
} = require('./utils/inboundGroupRouting');
const { requestContext } = require('./middleware/requestContext');
const { cacheJson, clearCacheByPrefix } = require('./middleware/responseCache');
const { validateBody, validateCreateEvent, validateAddPlayer } = require('./middleware/validate');
const { buildSystemRouter } = require('./routes/system');

// Polyfill fetch for Node < 18
const fetch = global.fetch || require('node-fetch');

const app = express();
app.set('trust proxy', 1);
app.use(express.json());
const PORT = process.env.PORT || 5000;
const ADMIN_DELETE_CODE = process.env.ADMIN_DELETE_CODE || '';
const SITE_ADMIN_WRITE_CODE = process.env.SITE_ADMIN_WRITE_CODE || '123';
const ADMIN_DESTRUCTIVE_CODE = process.env.ADMIN_DESTRUCTIVE_CODE || ADMIN_DELETE_CODE;
const ADMIN_DESTRUCTIVE_CONFIRM_CODE = process.env.ADMIN_DESTRUCTIVE_CONFIRM_CODE || '';
const SITE_URL = (process.env.SITE_URL || 'https://tee-time-brs.onrender.com/').replace(/\/$/, '') + '/';
const APP_ORIGIN = (() => {
  try {
    return new URL(SITE_URL).origin;
  } catch (_) {
    return 'http://localhost';
  }
})();
const LOCAL_TZ = process.env.LOCAL_TZ || 'America/New_York';
const DEFAULT_SITE_GROUP_SLUG = String(process.env.DEFAULT_SITE_GROUP_SLUG || 'main').trim().toLowerCase() || 'main';
const SKINS_POPS_FORCE_READY = false;
const RESEND_INBOUND_BASE_ADDRESS = 'teetime@xenailexou.resend.app';
const GROUP_SLUG_ALIASES = Object.freeze({
  'thursday-seniors-group': 'seniors',
});
const GROUP_REFERENCE_OVERRIDES = Object.freeze({
  [DEFAULT_SITE_GROUP_SLUG]: 'BRS Group',
  seniors: 'Thursday Seniors',
});
const GROUP_PROFILE_ISOLATION_OVERRIDES = Object.freeze({
  seniors: Object.freeze({
    features: Object.freeze({
      includeHandicaps: false,
      includeTrips: false,
      includeOutings: false,
      includeBackups: false,
    }),
  }),
});
const GROUP_SITE_ADMIN_CODE_OVERRIDES = Object.freeze({
  seniors: '000',
});
const GROUP_CONTACT_TARGET_OVERRIDES = Object.freeze({
  seniors: Object.freeze({
    clubEmail: 'brian.jones@blueridgeshadows.com',
    clubLabel: 'Brian Jones',
  }),
});
const CALENDAR_EVENT_DURATION_MINUTES = Math.max(30, Number(process.env.CALENDAR_EVENT_DURATION_MINUTES || 270) || 270);
const BACKUP_ROOT = path.join(__dirname, 'backups');
const SITE_BACKUP_TARGETS = [
  'public',
  'routes',
  'services',
  'models',
  'middleware',
  'utils',
  'docs',
  'scripts',
  'server.js',
  'secondary-conn.js',
  'package.json',
  'package-lock.json',
  'README.md',
  '.env.example',
];
const processedEmailIds = new Map(); // simple idempotency guard for inbound emails
let backupJobPromise = null;
let restoreJobPromise = null;

async function findPreferredMastersSeasonPool(season) {
  const pools = await MastersPool.find({ season }).sort({ createdAt: -1 }).lean();
  if (!pools.length) return null;
  const nonDemoPools = pools.filter((pool) => !/demo/i.test(String(pool.slug || '')) && !/demo/i.test(String(pool.name || '')));
  const source = nonDemoPools.length ? nonDemoPools : pools;
  return source.find((pool) => pool.status === 'live')
    || source.find((pool) => pool.status === 'complete')
    || source[0];
}

function parseIcsReminderMinutes(input = '') {
  const parsed = String(input || '')
    .split(',')
    .map((value) => Number(String(value).trim()))
    .filter((n) => Number.isInteger(n) && n > 0 && n <= 60 * 24 * 30);
  const unique = Array.from(new Set(parsed));
  unique.sort((a, b) => b - a);
  return unique;
}

const REQUIRED_ICS_REMINDER_MINUTES = [4320, 1440]; // 3 days, 1 day
const ICS_REMINDER_MINUTES = Array.from(new Set([
  ...REQUIRED_ICS_REMINDER_MINUTES,
  ...parseIcsReminderMinutes(process.env.ICS_REMINDER_MINUTES || ''),
])).sort((a, b) => b - a);

app.use(cors({ origin: process.env.CORS_ORIGIN ? process.env.CORS_ORIGIN.split(',') : true }));
app.use(rateLimit({ windowMs: 60 * 1000, max: 200 }));
app.use(compression());
app.use(requestContext);

// Prevent intermediary/browser caches from serving stale API data on mobile resumes.
app.use('/api', (_req, res, next) => {
  res.setHeader('Cache-Control', 'no-store, no-cache, must-revalidate, proxy-revalidate');
  res.setHeader('Pragma', 'no-cache');
  res.setHeader('Expires', '0');
  next();
});

function applyNoStoreHeaders(res) {
  res.setHeader('Cache-Control', 'no-store, no-cache, must-revalidate, proxy-revalidate');
  res.setHeader('Pragma', 'no-cache');
  res.setHeader('Expires', '0');
}

function buildCanonicalGroupQueryRedirectPath(req, pathname = req.path || '/') {
  const rawGroup = String(req.query && req.query.group || '').trim();
  if (!rawGroup) return '';
  const sanitizedGroup = sanitizeGroupSlug(rawGroup);
  const canonicalGroup = normalizeGroupSlug(rawGroup);
  if (!sanitizedGroup || sanitizedGroup === canonicalGroup) return '';
  const target = new URL(buildAbsoluteSiteUrl(pathname));
  Object.entries(req.query || {}).forEach(([key, rawValue]) => {
    const value = Array.isArray(rawValue) ? rawValue[rawValue.length - 1] : rawValue;
    if (value === undefined || value === null || value === '') return;
    if (key === 'group') target.searchParams.set('group', canonicalGroup);
    else target.searchParams.set(key, String(value));
  });
  return `${target.pathname}${target.search}${target.hash}`;
}

// Define routes before static middleware to ensure they take precedence
app.get('/healthz', (_req, res) => {
  res.type('text/plain').status(200).send('ok');
});

app.get('/api/health', (_req, res) => {
  res.type('application/json').status(200).json({ ok: true });
});

app.get('/', (req, res) => {
  const canonicalRedirect = buildCanonicalGroupQueryRedirectPath(req, '/');
  if (canonicalRedirect) return res.redirect(302, canonicalRedirect);
  applyNoStoreHeaders(res);
  return res.sendFile(path.join(__dirname, 'public', 'index.html'));
});
app.get('/admin.html', (req, res) => {
  const canonicalRedirect = buildCanonicalGroupQueryRedirectPath(req, '/admin.html');
  if (canonicalRedirect) return res.redirect(302, canonicalRedirect);
  const groupSlug = getGroupSlug(req);
  if (groupSlug !== DEFAULT_SITE_GROUP_SLUG) {
    return res.redirect(302, buildRedirectWithGroupPath('/group-admin-lite.html', groupSlug, req.query));
  }
  applyNoStoreHeaders(res);
  return res.sendFile(path.join(__dirname, 'public', 'admin.html'));
});
app.get('/group-admin-lite.html', (req, res) => {
  const canonicalRedirect = buildCanonicalGroupQueryRedirectPath(req, '/group-admin-lite.html');
  if (canonicalRedirect) return res.redirect(302, canonicalRedirect);
  const groupSlug = getGroupSlug(req);
  if (groupSlug === DEFAULT_SITE_GROUP_SLUG) {
    return res.redirect(302, buildRedirectWithGroupPath('/admin.html', DEFAULT_SITE_GROUP_SLUG, req.query));
  }
  applyNoStoreHeaders(res);
  return res.sendFile(path.join(__dirname, 'public', 'group-admin-lite.html'));
});
app.get('/masters', (_req, res) => {
  applyNoStoreHeaders(res);
  return res.sendFile(path.join(__dirname, 'public', 'masters', 'index.html'));
});
app.get('/majors', (_req, res) => {
  applyNoStoreHeaders(res);
  return res.sendFile(path.join(__dirname, 'public', 'majors', 'index.html'));
});
app.get('/majors/2026', (_req, res) => {
  applyNoStoreHeaders(res);
  return res.sendFile(path.join(__dirname, 'public', 'majors', 'index.html'));
});
app.get('/masters/create', (_req, res) => {
  applyNoStoreHeaders(res);
  return res.sendFile(path.join(__dirname, 'public', 'masters', 'create.html'));
});
app.get('/masters/join', (_req, res) => {
  applyNoStoreHeaders(res);
  return res.sendFile(path.join(__dirname, 'public', 'masters', 'join.html'));
});
app.get('/masters/live', (_req, res) => {
  applyNoStoreHeaders(res);
  return res.sendFile(path.join(__dirname, 'public', 'masters', 'live.html'));
});
app.get('/masters/rules', (_req, res) => {
  applyNoStoreHeaders(res);
  return res.sendFile(path.join(__dirname, 'public', 'masters', 'rules.html'));
});
app.get('/masters/admin', (_req, res) => {
  applyNoStoreHeaders(res);
  return res.sendFile(path.join(__dirname, 'public', 'masters', 'admin.html'));
});
app.get('/masters/results', (_req, res) => {
  applyNoStoreHeaders(res);
  return res.sendFile(path.join(__dirname, 'public', 'masters', 'results.html'));
});
app.get(/^\/masters\/(\d{4})$/, async (req, res) => {
  try {
    const season = Number(req.params[0]);
    const pool = await findPreferredMastersSeasonPool(season);
    if (!pool) return res.redirect(302, '/masters');
    return res.redirect(302, `/masters/live?poolId=${encodeURIComponent(String(pool._id))}`);
  } catch (_error) {
    return res.redirect(302, '/masters');
  }
});
app.get(/^\/masters\/(\d{4})\/admin$/, async (req, res) => {
  try {
    const season = Number(req.params[0]);
    const pool = await findPreferredMastersSeasonPool(season);
    if (!pool) return res.redirect(302, '/masters');
    return res.redirect(302, `/masters/admin?poolId=${encodeURIComponent(String(pool._id))}`);
  } catch (_error) {
    return res.redirect(302, '/masters');
  }
});
app.get(/^\/masters\/(\d{4})\/join$/, async (req, res) => {
  try {
    const season = Number(req.params[0]);
    const pool = await findPreferredMastersSeasonPool(season);
    if (!pool) return res.redirect(302, '/masters');
    return res.redirect(302, `/masters/join?poolId=${encodeURIComponent(String(pool._id))}`);
  } catch (_error) {
    return res.redirect(302, '/masters');
  }
});
app.get('/manifest.json', async (req, res) => {
  try {
    const scopedGroupSlug = String(req.query.group || '').trim()
      ? getGroupSlug(req)
      : inferGroupSlugFromReferrer(req, DEFAULT_SITE_GROUP_SLUG);
    const profile = toPublicSiteProfile(await getSiteProfile(scopedGroupSlug));
    const links = buildGroupDeploymentLinks(profile.groupSlug);
    const startUrl = profile.groupSlug === DEFAULT_SITE_GROUP_SLUG
      ? '/?source=pwa'
      : `${links.sitePath}?source=pwa`;
    const iconPath = profile.iconPath || '/icons/icon-512.png';
    const shortcuts = [
      {
        name: `${profile.shortTitle || profile.siteTitle} Tee Times`,
        short_name: 'Tee Times',
        url: links.sitePath,
        icons: [{ src: iconPath, sizes: '192x192', type: 'image/png' }],
      },
    ];
    if (links.adminLitePath && profile.groupSlug !== 'seniors') {
      shortcuts.push({
        name: 'Group Admin',
        short_name: 'Group Admin',
        url: links.adminPath,
        icons: [{ src: iconPath, sizes: '192x192', type: 'image/png' }],
      });
    }
    const manifest = {
      id: startUrl,
      name: profile.siteTitle || 'Tee Times',
      short_name: profile.shortTitle || profile.siteTitle || 'Tee Time',
      description: `Mobile-friendly tee times, calendar, and group admin tools for ${profile.groupName || profile.siteTitle || 'Tee Times'}.`,
      start_url: startUrl,
      scope: '/',
      display: 'standalone',
      display_override: ['standalone', 'minimal-ui', 'browser'],
      background_color: '#112417',
      theme_color: profile.themeColor || '#173224',
      orientation: 'any',
      categories: ['sports', 'productivity', 'utilities'],
      icons: [
        { src: iconPath, sizes: '192x192', type: 'image/png', purpose: 'any maskable' },
        { src: iconPath, sizes: '512x512', type: 'image/png', purpose: 'any maskable' },
      ],
      shortcuts,
    };
    res.setHeader('Content-Type', 'application/manifest+json; charset=utf-8');
    res.setHeader('Cache-Control', 'no-cache');
    return res.send(JSON.stringify(manifest, null, 2));
  } catch (error) {
    return res.status(500).json({ error: error && error.message ? error.message : 'Failed to build manifest' });
  }
});

app.get('/groups/:groupSlug', (req, res) => (
  res.redirect(302, buildRedirectWithGroupPath('/', req.params.groupSlug, req.query))
));
app.get('/groups/:groupSlug/admin', (req, res) => {
  const groupSlug = normalizeGroupSlug(req.params.groupSlug);
  if (groupSlug === DEFAULT_SITE_GROUP_SLUG) {
    return res.redirect(302, buildRedirectWithGroupPath('/admin.html', DEFAULT_SITE_GROUP_SLUG, req.query));
  }
  return res.redirect(302, buildRedirectWithGroupPath('/group-admin-lite.html', groupSlug, req.query));
});
app.get('/groups/:groupSlug/admin-lite', (req, res) => {
  const groupSlug = normalizeGroupSlug(req.params.groupSlug);
  if (groupSlug === DEFAULT_SITE_GROUP_SLUG) {
    return res.redirect(302, buildRedirectWithGroupPath('/admin.html', DEFAULT_SITE_GROUP_SLUG, req.query));
  }
  return res.redirect(302, buildRedirectWithGroupPath('/group-admin-lite.html', groupSlug, req.query));
});
// --- Myrtle Beach Trip Tracker API ---
// Debug endpoint: Query all trips and participants from secondary DB
app.get('/api/debug/secondary-trips', async (req, res) => {
  try {
    const secondaryConn = getSecondaryConn();
    if (!secondaryConn) return res.status(500).json({ error: 'No secondary connection' });
    const Trip = secondaryConn.model('Trip', require('./models/Trip').schema);
    const TripParticipant = secondaryConn.model('TripParticipant', require('./models/TripParticipant').schema);
    const trips = await Trip.find().lean();
    const participants = await TripParticipant.find().lean();
    res.json({ trips, participants });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});
app.use('/api/trips', require('./routes/trips'));
app.use('/api/masters-pools', require('./routes/mastersPools'));
app.use('/api/outings', cacheJson(15 * 1000), require('./routes/outings'));
app.use('/api/valley', require('./routes/valley'));
app.use('/api/events', (req, res, next) => {
  if (req.method === 'GET') return next();
  res.on('finish', () => {
    if (res.statusCode >= 200 && res.statusCode < 400) {
      clearCacheByPrefix('/api/events');
    }
  });
  next();
});
// Handicap tracking removed

// --- Handicap directory (manual list) ---
app.get('/api/handicaps', async (_req, res) => {
  try {
    if (!Handicap) return res.status(500).json({ error: 'Handicap model unavailable' });
    const list = await Handicap.find().sort({ name: 1 }).lean();
    const scrubbed = list.map((doc) => {
      const rest = { ...doc };
      delete rest.ownerCode;
      return rest;
    });
    res.json(scrubbed);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});
app.post('/api/handicaps', async (req, res) => {
  try {
    if (!Handicap) return res.status(500).json({ error: 'Handicap model unavailable' });
    const isAdminUser = isSiteAdmin(req);
    const { name, ghinNumber, handicapIndex, notes, ownerCode, clubName } = req.body || {};
    if (!name) return res.status(400).json({ error: 'name required' });
    if (!isAdminUser && !ownerCode) return res.status(400).json({ error: 'ownerCode required' });
    const payload = {
      name: String(name).trim(),
      clubName: clubName ? String(clubName).trim() : '',
      notes: notes ? String(notes).trim() : '',
      handicapIndex: handicapIndex === '' || handicapIndex === null || handicapIndex === undefined ? null : Number(handicapIndex)
    };
    const ghin = ghinNumber ? String(ghinNumber).trim() : '';
    if (ghin) payload.ghinNumber = ghin;
    if (isAdminUser) {
      payload.ownerCode = ownerCode ? String(ownerCode).trim() : payload.ownerCode;
    } else {
      payload.ownerCode = String(ownerCode || '').trim();
    }
    if (!payload.ownerCode) return res.status(400).json({ error: 'ownerCode required' });
    const created = await Handicap.create(payload);
    const rest = created.toObject();
    delete rest.ownerCode;
    res.status(201).json(rest);
  } catch (err) {
    if (err && err.code === 11000) {
      return res.status(409).json({ error: 'duplicate ghinNumber' });
    }
    res.status(500).json({ error: err.message });
  }
});
app.put('/api/handicaps/:id', async (req, res) => {
  try {
    if (!Handicap) return res.status(500).json({ error: 'Handicap model unavailable' });
    const isAdminUser = isSiteAdmin(req);
    const h = await Handicap.findById(req.params.id);
    if (!h) return res.status(404).json({ error: 'Not found' });
    if (!isAdminUser) {
      const provided = String((req.body && req.body.ownerCode) || '').trim();
      if (!provided || provided !== (h.ownerCode || '')) return res.status(403).json({ error: 'Forbidden' });
    }
    const { name, ghinNumber, handicapIndex, notes, ownerCode, clubName } = req.body || {};
    if (name !== undefined) h.name = String(name).trim();
    if (notes !== undefined) h.notes = String(notes).trim();
    if (clubName !== undefined) h.clubName = String(clubName || '').trim();
    if (handicapIndex !== undefined) {
      h.handicapIndex = handicapIndex === '' || handicapIndex === null ? null : Number(handicapIndex);
    }
    if (ghinNumber !== undefined) {
      const ghin = String(ghinNumber || '').trim();
      h.ghinNumber = ghin || undefined;
    }
    if (ownerCode !== undefined && (isAdminUser || ownerCode)) {
      h.ownerCode = String(ownerCode || '').trim();
    }
    await h.save();
    const rest = h.toObject();
    delete rest.ownerCode;
    res.json(rest);
  } catch (err) {
    if (err && err.code === 11000) {
      return res.status(409).json({ error: 'duplicate ghinNumber' });
    }
    res.status(500).json({ error: err.message });
  }
});
app.delete('/api/handicaps/:id', async (req, res) => {
  try {
    if (!Handicap) return res.status(500).json({ error: 'Handicap model unavailable' });
    const isAdminUser = isSiteAdmin(req);
    const h = await Handicap.findById(req.params.id);
    if (!h) return res.status(404).json({ error: 'Not found' });
    if (!isAdminUser) {
      const provided = String((req.body && req.body.ownerCode) || req.query.ownerCode || '').trim();
      if (!provided || provided !== (h.ownerCode || '')) return res.status(403).json({ error: 'Forbidden' });
    }
    await h.deleteOne();
    res.json({ ok: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Golfer list with current handicap from latest snapshot
app.get('/api/clubs/:clubId/golfers', async (req, res) => {
  try {
    if (!Golfer || !HandicapSnapshot) return res.status(500).json({ error: 'Handicap models unavailable' });
    const clubId = req.params.clubId;
    const golfers = await Golfer.find({ clubId }).lean();
    const ids = golfers.map((g) => g._id);
    const snaps = await HandicapSnapshot.find({ golferId: { $in: ids } }).sort({ asOfDate: -1, importedAt: -1 }).lean();
    const latestByGolfer = new Map();
    for (const snap of snaps) {
      const key = String(snap.golferId);
      if (!latestByGolfer.has(key)) latestByGolfer.set(key, snap);
    }
    const output = golfers.map((g) => {
      const latest = latestByGolfer.get(String(g._id));
      return {
        ...g,
        current_handicap_index: latest ? latest.handicapIndex : null,
        current_as_of_date: latest ? latest.asOfDate : null
      };
    });
    res.json(output);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Admin import (CSV upload)
app.post('/api/admin/clubs/:clubId/handicaps/import', upload.single('file'), async (req, res) => {
  try {
    if (!isMainSiteAdminRequest(req)) return res.status(403).json({ error: 'Forbidden' });
    if (!Golfer || !HandicapSnapshot || !ImportBatch) return res.status(500).json({ error: 'Handicap models unavailable' });
    const clubId = req.params.clubId;
    const dryRun = String(req.query.dryRun || '').toLowerCase() === 'true';
    if (!req.file) return res.status(400).json({ error: 'file required' });
    const csvText = req.file.buffer.toString('utf8');
    const result = await importHandicapsFromCsv({
      csvText,
      clubId,
      dryRun,
      importedBy: 'admin',
      fileName: req.file.originalname,
      models: { Golfer, HandicapSnapshot, ImportBatch }
    });
    res.json(result);
  } catch (err) {
    console.error('Handicap import error', err);
    res.status(500).json({ error: err.message });
  }
});

app.get('/api/admin/import-batches', async (req, res) => {
  try {
    if (!isMainSiteAdminRequest(req)) return res.status(403).json({ error: 'Forbidden' });
    if (!ImportBatch) return res.status(500).json({ error: 'ImportBatch model unavailable' });
    const q = {};
    if (req.query.clubId) q.clubId = req.query.clubId;
    const list = await ImportBatch.find(q).sort({ createdAt: -1 }).limit(200).lean();
    res.json(list);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.get('/api/admin/import-batches/:batchId', async (req, res) => {
  try {
    if (!isMainSiteAdminRequest(req)) return res.status(403).json({ error: 'Forbidden' });
    if (!ImportBatch) return res.status(500).json({ error: 'ImportBatch model unavailable' });
    const batch = await ImportBatch.findById(req.params.batchId).lean();
    if (!batch) return res.status(404).json({ error: 'Not found' });
    res.json(batch);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});


// --- Resend inbound webhook for email.received events ---
app.post('/webhooks/resend', async (req, res) => {
  try {
    const event = req.body;
    console.log('[webhook] Incoming event:', JSON.stringify(event));

    // Only handle inbound email events
    if (!event || event.type !== 'email.received') {
      return res.status(200).send('Ignored: not an email.received event');
    }

    const emailId = event.data && event.data.email_id;
    if (!emailId) {
      console.warn('[webhook] No email_id in event data');
      return res.status(200).send('No email_id');
    }
    // Idempotency guard: ignore repeat webhook deliveries for the same email_id (Resend can retry)
    const nowMs = Date.now();
    for (const [id, ts] of [...processedEmailIds.entries()]) {
      if (nowMs - ts > 10 * 60 * 1000) processedEmailIds.delete(id); // expire after 10 minutes
    }
    const markProcessed = () => processedEmailIds.set(emailId, Date.now());
    if (processedEmailIds.has(emailId)) {
      console.log('[webhook] Skipping already-processed email', emailId);
      return res.status(200).send('Already processed');
    }

    // Restrict to your expected sender and recipient for now
    const fromAddress = event.data.from || '';
    const toList = event.data.to || [];
    const inboundBaseAddress = RESEND_INBOUND_BASE_ADDRESS;
    const allowedFrom = ['tommy.knight@gmail.com', 'no-reply@foreupsoftware.com'];

    const toAllowed =
      Array.isArray(toList) &&
      toList.some(
        (addr) =>
          typeof addr === 'string' &&
          isAllowedInboundRecipient(addr, inboundBaseAddress)
      );

    const fromAllowed = allowedFrom.some(
      (allowed) => fromAddress.toLowerCase() === allowed.toLowerCase()
    );

    if (!toAllowed || !fromAllowed) {
      console.log('[webhook] Ignored: to/from not allowed', {
        from: fromAddress,
        to: toList,
      });
      markProcessed();
      return res.status(200).send('Ignored: to/from not allowed');
    }

    if (!process.env.RESEND_API_KEY) {
      console.warn('[webhook] RESEND_API_KEY not configured');
      markProcessed();
      return res.status(200).send('Resend not configured');
    }

    // Fetch full email content from Resend Receiving API via HTTP
    try {
      const emailRes = await fetch(
        `https://api.resend.com/emails/receiving/${emailId}`,
        {
          method: 'GET',
          headers: {
            Authorization: `Bearer ${process.env.RESEND_API_KEY}`,
            'Content-Type': 'application/json',
          },
        }
      );

      if (!emailRes.ok) {
        const text = await emailRes.text();
        console.error('[webhook] Resend API error:', emailRes.status, text);
        return res.status(500).send('Error fetching email');
      }

      const email = await emailRes.json();

      console.log('[webhook] Email meta:', {
        from: email.from,
        to: email.to,
        subject: email.subject,
      });
      const inboundRouting = inferInboundGroupRouting({
        eventTo: event.data && event.data.to,
        emailTo: email.to,
        subject: email.subject,
        bodyText: email.text || '',
        bodyHtml: email.html || '',
        baseAddress: inboundBaseAddress,
      });
      const requestGroupSlug = normalizeGroupSlug(inboundRouting.groupSlug || getGroupSlug(req));
      if (!(await isManagedGroupSlug(requestGroupSlug))) {
        console.warn('[webhook] Ignored: unknown group alias', requestGroupSlug, inboundRouting.marker || '');
        markProcessed();
        return res.status(200).send('Ignored: unknown group alias');
      }
      console.log('[webhook] Inbound group routing:', inboundRouting.source, requestGroupSlug, inboundRouting.marker || '');

      const textBody = email.text || '';
      const textPreview = textBody.slice(0, 400);
      console.log('[webhook] Email text preview:', textPreview);

      // Parse the email body for reservation details
      const { parseTeeTimeEmail } = require('./utils/parseTeeTimeEmail');
      const { buildInboundTeeTimeEmailLogEntry, buildInboundTeeTimeEmailLogResultUpdate } = require('./utils/inboundTeeTimeEmailLog');
      const parsed = parseTeeTimeEmail(textBody, email.subject);
      if (!parsed || !parsed.action) {
        console.warn('[webhook] No valid tee time action found');
        return res.status(200).send('No valid tee time data');
      }

      // Extract additional details from the email body (Facility, TTID, etc.)
      let facility = '';
      let notes = '';
      for (const line of (parsed.rawLines || [])) {
        if (/^facility:/i.test(line)) facility = line.replace(/^facility:/i, '').trim();
        if (/^details:/i.test(line)) notes = line.replace(/^details:/i, '').trim();
      }
      // Fallback: try to extract facility from the first lines if not found
      if (!facility && parsed.rawLines && parsed.rawLines.length > 0) {
        const facIdx = parsed.rawLines.findIndex(l => /facility/i.test(l));
        if (facIdx >= 0 && parsed.rawLines[facIdx + 1]) {
          facility = parsed.rawLines[facIdx + 1].trim();
        }
      }

      // Tag event with source email note for traceability
      const sourceEmail = email.from || fromAddress || '';
      const sourceNote = sourceEmail ? `Email source: ${sourceEmail}` : '';
      const combinedNotes = [notes, sourceNote].filter(Boolean).join(' | ');

      // Normalize date to YYYY-MM-DD and time to HH:MM 24h
      function normalizeDate(dateStr) {
        // Accept MM/DD/YY or MM/DD/YYYY and convert to YYYY-MM-DD
        if (/^\d{1,2}\/\d{1,2}\/\d{2,4}$/.test(dateStr)) {
          const [m, d, y] = dateStr.split('/');
          const year = y.length === 2 ? '20' + y : y;
          return `${year}-${m.padStart(2, '0')}-${d.padStart(2, '0')}`;
        }
        return dateStr;
      }
      function normalizeTime(timeStr) {
        // Accept 8:18am or 8:18 am, return 08:18 (24h)
        const m = timeStr.match(/(\d{1,2}):(\d{2})\s*(am|pm)?/i);
        if (!m) return timeStr;
        let h = parseInt(m[1], 10);
        const min = m[2];
        const ap = m[3] ? m[3].toLowerCase() : '';
        if (ap === 'pm' && h < 12) h += 12;
        if (ap === 'am' && h === 12) h = 0;
        return `${String(h).padStart(2, '0')}:${min}`;
      }

      const normalizedDate = normalizeDate(parsed.dateStr || '');
      const normalizedTime = normalizeTime(parsed.timeStr || '');
      const eventDateObj = asUTCDate(normalizedDate);
      if (isNaN(eventDateObj)) {
        console.warn('[webhook] Invalid date parsed from email, skipping');
        markProcessed();
        return res.status(200).send('Invalid date in email');
      }

      // Derive number of tee times from golfers count (4 per tee)
      const teeTimeCount = (typeof parsed.players === 'number' && parsed.players > 0)
        ? Math.max(1, Math.ceil(parsed.players / 4))
        : null;
      const teeTimesFromCount = teeTimeCount ? genTeeTimes(normalizedTime, teeTimeCount, 9) : undefined;
      let inboundEmailLogId = '';
      async function updateInboundEmailLog(result = {}) {
        if (!InboundTeeTimeEmailLog) return;
        const payload = buildInboundTeeTimeEmailLogResultUpdate(result);
        try {
          if (inboundEmailLogId) {
            await InboundTeeTimeEmailLog.updateOne({ _id: inboundEmailLogId }, { $set: payload });
            return;
          }
          const sourceEmailId = String(event.data && event.data.email_id || email.id || '').trim();
          if (sourceEmailId) {
            await InboundTeeTimeEmailLog.updateOne(
              { groupSlug: requestGroupSlug, sourceEmailId },
              { $set: payload }
            );
          }
        } catch (logError) {
          console.error('[webhook] Failed to update inbound tee-time email log:', logError);
        }
      }
      if (InboundTeeTimeEmailLog) {
        const inboundLogEntry = buildInboundTeeTimeEmailLogEntry({
          groupSlug: requestGroupSlug,
          parsed,
          facility,
          email,
          eventData: event.data || {},
          normalizedDate,
          normalizedTime,
          generatedTeeTimes: Array.isArray(teeTimesFromCount)
            ? teeTimesFromCount.map((slot) => slot && slot.time).filter(Boolean)
            : (normalizedTime ? [normalizedTime] : []),
        });
        try {
          if (inboundLogEntry.sourceEmailId) {
            await InboundTeeTimeEmailLog.updateOne(
              { groupSlug: requestGroupSlug, sourceEmailId: inboundLogEntry.sourceEmailId },
              { $setOnInsert: inboundLogEntry },
              { upsert: true }
            );
            const existing = await InboundTeeTimeEmailLog.findOne(
              { groupSlug: requestGroupSlug, sourceEmailId: inboundLogEntry.sourceEmailId },
              { _id: 1 }
            ).lean();
            inboundEmailLogId = existing ? String(existing._id) : '';
          } else {
            const createdLog = await InboundTeeTimeEmailLog.create(inboundLogEntry);
            inboundEmailLogId = String(createdLog._id);
          }
        } catch (logError) {
          console.error('[webhook] Failed to persist inbound tee-time email log:', logError);
        }
      }
      const dedupeKey = buildDedupeKey(
        eventDateObj,
        teeTimesFromCount || [{ time: normalizedTime }],
        false,
        facility || parsed.course || email.subject || 'Unknown Course'
      );

      // Compose event payload as expected by /api/events (UI form)
      const eventPayload = {
        course: facility || parsed.course || email.subject || 'Unknown Course',
        date: normalizedDate,
        notes: combinedNotes,
        isTeamEvent: false,
        teamSizeMax: 4,
        teeTime: normalizedTime, // UI expects 'teeTime' for first tee time
        teeTimes: teeTimesFromCount,
        dedupeKey
      };
      console.log('[webhook] Event payload to be created:', JSON.stringify(eventPayload));

      const escapeRegex = (s = '') => s.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
      const findMatchingEvents = async () => {
        // Prefer exact course + time match, then time-only match on same date
        const queries = [];
        const scopedGroupQuery = groupScopeFilter(requestGroupSlug);
        if (eventPayload.teeTime) {
          if (eventPayload.dedupeKey) {
            queries.push({ ...scopedGroupQuery, dedupeKey: eventPayload.dedupeKey });
          }
          if (eventPayload.course) {
            queries.push({
              ...scopedGroupQuery,
              date: eventDateObj,
              'teeTimes.time': eventPayload.teeTime,
              course: new RegExp(`^${escapeRegex(eventPayload.course)}$`, 'i')
            });
          }
          queries.push({ ...scopedGroupQuery, date: eventDateObj, 'teeTimes.time': eventPayload.teeTime });
        } else {
          queries.push({ ...scopedGroupQuery, date: eventDateObj });
        }
        for (const q of queries) {
          const found = await Event.find(q).sort({ createdAt: 1 });
          if (found.length) return found;
        }
        return [];
      };

      const updateEventFromPayload = async (ev) => {
        const beforeAudit = captureEventAuditSnapshot(ev);
        ev.course = eventPayload.course || ev.course;
        ev.notes = eventPayload.notes || ev.notes || '';
        ev.date = eventDateObj;
        ev.isTeamEvent = false;
        ev.teamSizeMax = 4;
        ev.courseInfo = enrichCourseInfo(ev.course, ev.courseInfo || {});

        const hasPlayers = Array.isArray(ev.teeTimes) && ev.teeTimes.some((tt) => Array.isArray(tt.players) && tt.players.length);
        if (Array.isArray(eventPayload.teeTimes) && eventPayload.teeTimes.length && !hasPlayers) {
          ev.teeTimes = eventPayload.teeTimes;
        } else if (eventPayload.teeTime) {
          if (Array.isArray(ev.teeTimes) && ev.teeTimes.length) {
            ev.teeTimes[0].time = eventPayload.teeTime;
          } else {
            ev.teeTimes = [{ time: eventPayload.teeTime, players: [] }];
          }
        }
        assignWeatherToEvent(ev, await fetchWeatherForEvent(ev));
        const updated = await ev.save();
        const afterAudit = captureEventAuditSnapshot(updated);
        await logAudit(updated._id, 'update_event', 'SYSTEM', {
          ...auditContextFromEvent(updated),
          message: buildEventUpdateAuditMessage(beforeAudit, afterAudit),
          details: {
            before: beforeAudit,
            after: afterAudit,
            source: 'inbound_email',
          },
        });
        return updated;
      };

      const createEventThroughApi = async (reason = 'CREATE') => {
        const body = { ...eventPayload, date: normalizedDate };
        const fetchRes = await fetch(`${SITE_URL}api/events?group=${encodeURIComponent(requestGroupSlug)}`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(body)
        });
        if (!fetchRes.ok) {
          const text = await fetchRes.text();
          throw new Error(`API ${reason} create failed: ${fetchRes.status} ${text}`);
        }
        const created = await fetchRes.json();
        console.log(`[webhook] Event created from email (${reason}) via API:`, created._id);
        return created;
      };

      if ((parsed.action === 'CREATE' || parsed.action === 'UPDATE') && eventPayload.course && eventPayload.date && eventPayload.teeTime) {
        try {
          const matches = await findMatchingEvents();
          if (matches.length) {
            const updated = await updateEventFromPayload(matches[0]);
            console.log('[webhook] Event matched existing, updated instead of creating new', { id: updated._id });
            await updateInboundEmailLog({
              processingResult: 'updated',
              processingNote: 'Matched an existing event and refreshed its tee-time details.',
              matchedEventId: updated && updated._id ? String(updated._id) : '',
            });
            markProcessed();
            return res.status(200).json({ ok: true, eventId: updated._id, updated: true, deduped: 0 });
          }

          const created = await createEventThroughApi(parsed.action);
          await updateInboundEmailLog({
            processingResult: 'created',
            processingNote: 'Created a new event from the inbound tee-time email.',
            createdEventId: created && created._id ? String(created._id) : '',
          });
          // Send notification email to all subscribers (same as /api/events) for brand new events only
          try {
            const eventUrl = buildSiteEventUrl(created.groupSlug || requestGroupSlug, created._id);
            await sendSubscriberChangeEmail(`New Event: ${created.course} (${fmt.dateISO(created.date)})`,
              frame('A New Golf Event Has Been Scheduled!',
                `<p>The following event is now open for sign-up:</p>
                 <p><strong>Event:</strong> ${esc(fmt.dateShortTitle(created.date))}</p>
                 <p><strong>Course:</strong> ${esc(created.course||'')}</p>
                 <p><strong>Date:</strong> ${esc(fmt.dateLong(created.date))}</p>
                 ${(!created.isTeamEvent && created.teeTimes?.[0]?.time) ? `<p><strong>First Tee Time:</strong> ${esc(fmt.tee(created.teeTimes[0].time))}</p>`:''}
                 <p>Please <a href="${eventUrl}" style="color:#166534;text-decoration:underline">click here to view this event directly</a> or visit the sign-up page to secure your spot!</p>${btn('Go to Sign-up Page', eventUrl)}`)
            , { groupSlug: created.groupSlug || requestGroupSlug });
          } catch (e) {
            console.error('[webhook] Failed to send notification email:', e);
          }
          markProcessed();
          return res.status(201).json({ ok: true, eventId: created._id, created: true });
        } catch (err) {
          console.error('[webhook] Error creating/updating event via email:', err);
          await updateInboundEmailLog({
            processingResult: 'failed',
            processingNote: err && err.message ? err.message : 'Error creating or updating event via API.',
          });
          return res.status(500).send('Error creating/updating event via API');
        }
      } else if (parsed.action === 'CANCEL' && eventPayload.course && eventPayload.date && eventPayload.teeTime) {
        try {
          const matches = await findMatchingEvents();
          if (matches.length) {
            const primary = matches[0];
            await archiveDeletedEvent(primary, {
              deletedBy: 'SYSTEM',
              deleteSource: 'inbound_email',
              notes: `Inbound tee-time cancellation email for ${eventPayload.teeTime || 'tee time'}.`,
            });
            await primary.deleteOne();
            await logAudit(primary._id, 'delete_event', 'SYSTEM', {
              ...auditContextFromEvent(primary),
              message: `Deleted event ${primary.course || 'event'} on ${fmt.dateISO(primary.date) || 'the selected date'} from an inbound tee-time cancellation email.`,
              details: {
                source: 'inbound_email',
                matchedTeeTime: eventPayload.teeTime || '',
              },
            });
            console.log('[webhook] Event cancelled from email (removed match):', primary._id);
            await updateInboundEmailLog({
              processingResult: 'cancelled',
              processingNote: 'Cancelled the matched event from the inbound tee-time email.',
              matchedEventId: primary && primary._id ? String(primary._id) : '',
            });
            // Notify subscribers about the cancellation (non-blocking)
            const teeMatch = (primary.teeTimes || []).find(tt => tt && tt.time === eventPayload.teeTime);
            const teeLabel = teeMatch && teeMatch.time ? fmt.tee(teeMatch.time) : null;
            sendSubscriberChangeEmail(
              `Event Cancelled: ${primary.course || 'Event'} (${fmt.dateISO(primary.date)})`,
              frame('Golf Event Cancelled',
                `<p>The following event has been cancelled:</p>
                 <p><strong>Event:</strong> ${esc(fmt.dateShortTitle(primary.date))}</p>
                 <p><strong>Course:</strong> ${esc(primary.course||'')}</p>
                 <p><strong>Date:</strong> ${esc(fmt.dateLong(primary.date))}</p>
                 ${teeLabel ? `<p><strong>Tee Time:</strong> ${esc(teeLabel)}</p>` : ''}
                 <p>We apologize for any inconvenience.</p>${btn('View Other Events')}`))
              , { groupSlug: primary.groupSlug }
              .catch(err => console.error('[webhook] Failed to send cancellation email:', err));
            markProcessed();
            return res.status(200).json({ ok: true, cancelled: true, eventIds: [primary._id] });
          } else {
            console.warn('[webhook] Cancel: no matching event found');
            await updateInboundEmailLog({
              processingResult: 'ignored',
              processingNote: 'Cancel email received, but no matching event was found.',
            });
            markProcessed();
            return res.status(200).send('Cancel: no matching event found');
          }
        } catch (err) {
          console.error('[webhook] Error cancelling event:', err);
          await updateInboundEmailLog({
            processingResult: 'failed',
            processingNote: err && err.message ? err.message : 'Error cancelling event.',
          });
          return res.status(500).send('Error cancelling event');
        }
      } else {
        console.log('[webhook] Ignoring non-create/cancel email action:', parsed.action);
        await updateInboundEmailLog({
          processingResult: 'ignored',
          processingNote: `No event action taken for parsed action ${String(parsed.action || 'unknown').toLowerCase()}.`,
        });
        markProcessed();
        return res.status(200).send(`No event created or cancelled (action=${parsed.action || 'unknown'})`);
      }
    } catch (err) {
      console.error('[webhook] Error fetching email content from Resend:', err);
      return res.status(500).send('Error fetching email');
    }
  } catch (err) {
    console.error('[webhook] Internal error handling webhook:', err);
    return res.status(500).send('Internal server error');
  }
});

const LEGACY_STATIC_REDIRECTS = new Map([
  ['/myrtle-trip-2026.html', '/myrtle/trip-2026.html'],
  ['/tin-cup-trip-2026.html', '/tin-cup/trip-2026.html'],
  ['/tin-cup-live-score-entry.html', '/tin-cup/live-score-entry.html'],
  ['/tin-cup-leaderboard-2026.html', '/tin-cup/leaderboard-2026.html'],
  ['/tin-cup-guests-lodging.html', '/tin-cup/guests-lodging.html'],
]);

app.get(Array.from(LEGACY_STATIC_REDIRECTS.keys()), (req, res) => {
  const target = LEGACY_STATIC_REDIRECTS.get(req.path);
  if (!target) return res.status(404).end();
  const suffix = `${req.url.includes('?') ? `?${req.url.split('?')[1]}` : ''}`;
  return res.redirect(302, `${target}${suffix}`);
});

app.use(express.static(path.join(__dirname, 'public'), {
  maxAge: '1h',
  etag: true,
  setHeaders: (res, filePath) => {
    if (/(^|[\\/])(service-worker\.js|sw-assets\.js)$/i.test(filePath)) {
      applyNoStoreHeaders(res);
    } else if (/\.(js|css|png|svg|ico|webp|json)$/i.test(filePath)) {
      res.setHeader('Cache-Control', 'public, max-age=3600, stale-while-revalidate=300');
    } else if (/\.html$/i.test(filePath)) {
      applyNoStoreHeaders(res);
    }
  },
}));

const skipMongoConnect = process.env.SKIP_MONGO_CONNECT === '1';
const mongoUri = process.env.MONGO_URI || 'mongodb://127.0.0.1:27017/teetimes';
if (!skipMongoConnect) {
  mongoose.connect(mongoUri, { dbName: process.env.MONGO_DB || undefined })
    .then(async () => {
      console.log(JSON.stringify({ t:new Date().toISOString(), level:'info', msg:'Mongo connected', uri:mongoUri }));
      await ensureScopedSettingsIndexes();
      await ensureScopedSubscriberIndexes();
      await ensureEventIndexes();
      await ensureTeeTimeAuditIndexes();
      await ensureCanonicalGroupAliasData();
      await ensureGroupProfileIsolationDefaults();
      await ensureGroupAccessControlCache();
    })
    .catch((e) => { console.error('Mongo connection error', e); process.exit(1); });
} else {
  console.log(JSON.stringify({ t:new Date().toISOString(), level:'info', msg:'Mongo connect skipped for test mode' }));
}

let Event; try { Event = require('./models/Event'); } catch { Event = require('./Event'); }
let Subscriber; try { Subscriber = require('./models/Subscriber'); } catch { Subscriber = null; }
let AuditLog; try { AuditLog = require('./models/AuditLog'); } catch { AuditLog = null; }
let Settings; try { Settings = require('./models/Settings'); } catch { Settings = null; }
let Handicap; try { Handicap = require('./models/Handicap'); } catch { Handicap = null; }
let Golfer; try { Golfer = require('./models/Golfer'); } catch { Golfer = null; }
let SeniorsGolfer; try { SeniorsGolfer = require('./models/SeniorsGolfer'); } catch { SeniorsGolfer = null; }
let HandicapSnapshot; try { HandicapSnapshot = require('./models/HandicapSnapshot'); } catch { HandicapSnapshot = null; }
let ImportBatch; try { ImportBatch = require('./models/ImportBatch'); } catch { ImportBatch = null; }
let TeeTimeLog; try { TeeTimeLog = require('./models/TeeTimeLog'); } catch { TeeTimeLog = null; }
let DeletedTeeTimeArchive; try { DeletedTeeTimeArchive = require('./models/DeletedTeeTimeArchive'); } catch { DeletedTeeTimeArchive = null; }
let InboundTeeTimeEmailLog; try { InboundTeeTimeEmailLog = require('./models/InboundTeeTimeEmailLog'); } catch { InboundTeeTimeEmailLog = null; }
const TEE_TIME_AUDIT_RETENTION_DAYS = 30;
const TEE_TIME_AUDIT_RETENTION_MS = TEE_TIME_AUDIT_RETENTION_DAYS * 24 * 60 * 60 * 1000;
const TEE_TIME_AUDIT_RETENTION_SECONDS = TEE_TIME_AUDIT_RETENTION_DAYS * 24 * 60 * 60;

async function ensureScopedSettingsIndexes() {
  if (!Settings || !Settings.collection) return;
  try {
    const mainGroupSlug = DEFAULT_SITE_GROUP_SLUG;
    const legacySettings = await Settings.find({
      $or: [
        { groupSlug: { $exists: false } },
        { groupSlug: null },
        { groupSlug: '' },
      ],
    }).sort({ updatedAt: -1, _id: -1 }).lean();

    for (const legacySetting of legacySettings) {
      const key = String(legacySetting && legacySetting.key || '').trim();
      if (!key) continue;
      const existingScoped = await Settings.findOne({ groupSlug: mainGroupSlug, key }).sort({ updatedAt: -1, _id: -1 }).lean();
      if (existingScoped) {
        const legacyUpdatedAt = legacySetting && legacySetting.updatedAt ? new Date(legacySetting.updatedAt).getTime() : 0;
        const scopedUpdatedAt = existingScoped && existingScoped.updatedAt ? new Date(existingScoped.updatedAt).getTime() : 0;
        if (legacyUpdatedAt > scopedUpdatedAt) {
          await Settings.updateOne({ _id: existingScoped._id }, { $set: { value: legacySetting.value } });
        }
        await Settings.deleteOne({ _id: legacySetting._id });
        continue;
      }
      await Settings.updateOne({ _id: legacySetting._id }, { $set: { groupSlug: mainGroupSlug } });
    }

    if (legacySettings.length) {
      console.log(JSON.stringify({
        t: new Date().toISOString(),
        level: 'info',
        msg: 'settings-group-backfill',
        migrated: legacySettings.length,
        groupSlug: mainGroupSlug,
      }));
    }

    const indexes = await Settings.collection.indexes();
    const legacyKeyIndex = indexes.find((index) => index && index.name === 'key_1' && index.unique);
    if (legacyKeyIndex) {
      await Settings.collection.dropIndex('key_1');
      console.log(JSON.stringify({ t:new Date().toISOString(), level:'info', msg:'settings-index-migrated', dropped:'key_1' }));
    }
    await Settings.collection.createIndex({ groupSlug: 1, key: 1 }, { unique: true, name: 'groupSlug_1_key_1' });
  } catch (error) {
    console.error('Failed to ensure scoped settings indexes:', error);
  }
}

async function ensureScopedSubscriberIndexes() {
  if (!Subscriber || !Subscriber.collection) return;
  try {
    const mainGroupSlug = DEFAULT_SITE_GROUP_SLUG;
    const legacySubscribers = await Subscriber.find({
      $or: [
        { groupSlug: { $exists: false } },
        { groupSlug: null },
        { groupSlug: '' },
      ],
    }).lean();

    if (legacySubscribers.length) {
      await Subscriber.updateMany(
        {
          $or: [
            { groupSlug: { $exists: false } },
            { groupSlug: null },
            { groupSlug: '' },
          ],
        },
        { $set: { groupSlug: mainGroupSlug } }
      );
      console.log(JSON.stringify({
        t: new Date().toISOString(),
        level: 'info',
        msg: 'subscriber-group-backfill',
        migrated: legacySubscribers.length,
        groupSlug: mainGroupSlug,
      }));
    }

    const indexes = await Subscriber.collection.indexes();
    const legacyEmailIndex = indexes.find((index) => index && index.name === 'email_1' && index.unique);
    if (legacyEmailIndex) {
      await Subscriber.collection.dropIndex('email_1');
      console.log(JSON.stringify({ t:new Date().toISOString(), level:'info', msg:'subscriber-index-migrated', dropped:'email_1' }));
    }
    await Subscriber.collection.createIndex({ groupSlug: 1, email: 1 }, { unique: true, name: 'groupSlug_1_email_1' });
  } catch (error) {
    console.error('Failed to ensure scoped subscriber indexes:', error);
  }
}

async function ensureCanonicalGroupAliasData() {
  const aliasEntries = Object.entries(GROUP_SLUG_ALIASES);
  if (!aliasEntries.length) return;
  try {
    for (const [legacySlug, canonicalSlug] of aliasEntries) {
      if (Settings) {
        const legacySettings = await Settings.find({ groupSlug: legacySlug }).sort({ updatedAt: -1, _id: -1 });
        for (const legacySetting of legacySettings) {
          const key = String(legacySetting && legacySetting.key || '').trim();
          if (!key) continue;
          const existingScoped = await Settings.findOne({ groupSlug: canonicalSlug, key }).sort({ updatedAt: -1, _id: -1 });
          if (existingScoped) {
            const legacyUpdatedAt = legacySetting && legacySetting.updatedAt ? new Date(legacySetting.updatedAt).getTime() : 0;
            const scopedUpdatedAt = existingScoped && existingScoped.updatedAt ? new Date(existingScoped.updatedAt).getTime() : 0;
            if (legacyUpdatedAt > scopedUpdatedAt) {
              existingScoped.value = legacySetting.value;
              await existingScoped.save();
            }
            await Settings.deleteOne({ _id: legacySetting._id });
          } else {
            legacySetting.groupSlug = canonicalSlug;
            await legacySetting.save();
          }
        }
      }

      if (Subscriber) {
        const legacySubscribers = await Subscriber.find({ groupSlug: legacySlug });
        for (const legacySubscriber of legacySubscribers) {
          const normalizedEmail = String(legacySubscriber.email || '').trim().toLowerCase();
          if (!normalizedEmail) {
            await Subscriber.deleteOne({ _id: legacySubscriber._id });
            continue;
          }
          const existingScoped = await Subscriber.findOne({ groupSlug: canonicalSlug, email: normalizedEmail });
          if (existingScoped) {
            if (!existingScoped.unsubscribeToken && legacySubscriber.unsubscribeToken) {
              existingScoped.unsubscribeToken = legacySubscriber.unsubscribeToken;
              await existingScoped.save();
            }
            await Subscriber.deleteOne({ _id: legacySubscriber._id });
          } else {
            legacySubscriber.groupSlug = canonicalSlug;
            await legacySubscriber.save();
          }
        }
      }

      if (Event) {
        await Event.updateMany({ groupSlug: legacySlug }, { $set: { groupSlug: canonicalSlug } });
      }
      if (TeeTimeLog) {
        await TeeTimeLog.updateMany({ groupSlug: legacySlug }, { $set: { groupSlug: canonicalSlug } });
      }
    }
  } catch (error) {
    console.error('Failed to canonicalize aliased group data:', error);
  }
}

async function ensureGroupProfileIsolationDefaults() {
  if (!Settings) return;
  try {
    for (const groupSlug of Object.keys(GROUP_PROFILE_ISOLATION_OVERRIDES)) {
      const setting = await Settings.findOne(scopedSettingQuery(groupSlug, 'siteProfile'));
      if (!setting || !setting.value || typeof setting.value !== 'object') continue;
      const nextProfile = applyGroupProfileIsolation(groupSlug, setting.value);
      if (JSON.stringify(setting.value) === JSON.stringify(nextProfile)) continue;
      setting.value = nextProfile;
      await setting.save();
    }
  } catch (error) {
    console.error('Failed to enforce group profile isolation defaults:', error);
  }
}

async function ensureGroupAccessControlCache() {
  groupAccessControlCache.clear();
  groupAccessControlCache.set(DEFAULT_SITE_GROUP_SLUG, normalizeStoredGroupAccessConfig(DEFAULT_SITE_GROUP_SLUG, {}));
  if (!Settings) return;
  try {
    const profiles = await Settings.find({ key: 'siteProfile' }).lean();
    for (const entry of profiles) {
      const profile = entry && entry.value && typeof entry.value === 'object' ? entry.value : {};
      setGroupAccessControlCache(entry.groupSlug || profile.groupSlug || DEFAULT_SITE_GROUP_SLUG, profile);
    }
  } catch (error) {
    console.error('Failed to populate group access control cache:', error);
  }
}

app.use('/api', buildSystemRouter({
  mongoose,
  getSecondaryConn,
  getFeatures: () => ({
    hasResendKey: !!process.env.RESEND_API_KEY,
    hasResendFrom: !!process.env.RESEND_FROM,
    hasSubscriberModel: !!Subscriber,
    hasHandicapModels: !!(Golfer && HandicapSnapshot && ImportBatch),
  }),
  port: PORT,
}));

/* ---------------- Admin Configuration ---------------- */
const ADMIN_EMAILS = (process.env.ADMIN_EMAILS || 'tommy.knight@gmail.com,jvhyers@gmail.com').split(',').map(e => e.trim()).filter(Boolean);
const CLUB_EMAIL = process.env.CLUB_CANCEL_EMAIL || 'Brian.Jones@blueridgeshadows.com';
const BRS_TEE_RETURN_CC_EMAILS = uniqueEmailList([
  String(process.env.BRS_TEE_RETURN_CC || '').split(','),
  'tommy.knight@gmail.com',
]);
const BRS_TEE_TIME_CHANGE_ALERT_EMAILS = uniqueEmailList([
  String(process.env.BRS_TEE_TIME_CHANGE_ALERT_EMAILS || '').split(','),
  'tommy.knight@gmail.com',
]);
const SCHEDULER_ENV_DISABLED = process.env.ENABLE_SCHEDULER === '0';
const SCHEDULED_EMAIL_RULE_DEFAULTS = Object.freeze({
  brianTomorrowEmptyClubAlert: true,
  reminder48Hour: true,
  reminder24Hour: true,
  nearlyFullTeeTimes: true,
  adminEmptyTeeAlerts: true,
});
const SCHEDULED_EMAIL_RULE_KEYS = Object.keys(SCHEDULED_EMAIL_RULE_DEFAULTS);

/* ---------------- Tee time change logging ---------------- */
async function logTeeTimeChange(entry = {}) {
  if (!TeeTimeLog) return;
  try {
    await TeeTimeLog.create({
      groupSlug: normalizeGroupSlug(entry.groupSlug),
      eventId: entry.eventId,
      teeId: entry.teeId,
      action: entry.action,
      labelBefore: entry.labelBefore || '',
      labelAfter: entry.labelAfter || '',
      isTeamEvent: !!entry.isTeamEvent,
      course: entry.course || '',
      dateISO: entry.dateISO || '',
      notifyClub: !!entry.notifyClub,
      mailMethod: entry.mailMethod || null,
      mailError: entry.mailError || null,
    });
  } catch (err) {
    console.error('[tee-time] Failed to log tee time change', err.message);
  }
}

function deepCloneArchiveValue(value) {
  if (value === undefined) return null;
  try {
    return JSON.parse(JSON.stringify(value));
  } catch (_error) {
    return null;
  }
}

function stripArchiveMetadata(value) {
  if (Array.isArray(value)) return value.map((entry) => stripArchiveMetadata(entry));
  if (!value || typeof value !== 'object') return value;
  return Object.entries(value).reduce((acc, [key, entryValue]) => {
    if (key === '__v') return acc;
    acc[key] = stripArchiveMetadata(entryValue);
    return acc;
  }, {});
}

function normalizeArchivedPlayerSnapshot(player = {}) {
  if (!player || typeof player !== 'object') return null;
  const normalized = {
    name: String(player.name || '').trim(),
    checkedIn: !!player.checkedIn,
    isFifth: !!player.isFifth,
  };
  if (!normalized.name) return null;
  if (player._id) normalized._id = player._id;
  return normalized;
}

function normalizeArchivedSlotSnapshot(slot = {}) {
  if (!slot || typeof slot !== 'object') return null;
  const normalized = {
    players: Array.isArray(slot.players)
      ? slot.players.map((player) => normalizeArchivedPlayerSnapshot(player)).filter(Boolean)
      : [],
  };
  if (slot._id) normalized._id = slot._id;
  const time = String(slot.time || '').trim();
  const name = String(slot.name || '').trim();
  if (time) normalized.time = time;
  if (name) normalized.name = name;
  return normalized;
}

function normalizeArchivedRegistrationSnapshot(registration = {}) {
  if (!registration || typeof registration !== 'object') return null;
  const normalized = {
    name: String(registration.name || '').trim(),
    email: String(registration.email || '').trim(),
    phone: String(registration.phone || '').trim(),
    ghinNumber: String(registration.ghinNumber || '').trim(),
    handicapIndex: Number.isFinite(Number(registration.handicapIndex)) ? Number(registration.handicapIndex) : null,
    golferId: registration.golferId || null,
    createdAt: registration.createdAt || null,
  };
  if (!normalized.name) return null;
  if (registration._id) normalized._id = registration._id;
  return normalized;
}

function normalizeArchivedSkinsPopsSnapshot(input = {}) {
  const sharedHoles = Array.isArray(input.sharedHoles)
    ? input.sharedHoles.map((value) => Number(value)).filter((value) => Number.isInteger(value))
    : [];
  const bonusHoles = Array.isArray(input.bonusHoles)
    ? input.bonusHoles.map((value) => Number(value)).filter((value) => Number.isInteger(value))
    : [];
  return {
    sharedHoles,
    bonusHoles,
    generatedAt: input.generatedAt || null,
  };
}

function normalizeArchivedWeatherSnapshot(input = {}) {
  return {
    condition: toNullableString(input.condition),
    icon: toNullableString(input.icon),
    temp: toFiniteNumber(input.temp),
    tempLow: toFiniteNumber(input.tempLow),
    tempHigh: toFiniteNumber(input.tempHigh),
    rainChance: toFiniteNumber(input.rainChance),
    description: toNullableString(input.description),
    lastFetched: input.lastFetched || null,
  };
}

function normalizeArchivedEventSnapshot(snapshot = {}, fallbackGroupSlug = DEFAULT_SITE_GROUP_SLUG) {
  const cloned = stripArchiveMetadata(deepCloneArchiveValue(snapshot) || {});
  if (!cloned || typeof cloned !== 'object') return null;
  const normalized = {
    groupSlug: normalizeGroupSlug(cloned.groupSlug || fallbackGroupSlug),
    course: String(cloned.course || '').trim(),
    date: cloned.date || null,
    notes: String(cloned.notes || ''),
    isTeamEvent: !!cloned.isTeamEvent,
    seniorsEventType: String(cloned.seniorsEventType || '').trim(),
    seniorsRegistrationMode: String(cloned.seniorsRegistrationMode || '').trim(),
    teamSizeMax: Number.isFinite(Number(cloned.teamSizeMax)) ? Number(cloned.teamSizeMax) : 4,
    courseInfo: normalizeCourseInfo(cloned.courseInfo || {}),
    teeTimes: Array.isArray(cloned.teeTimes)
      ? cloned.teeTimes.map((slot) => normalizeArchivedSlotSnapshot(slot)).filter(Boolean)
      : [],
    seniorsRegistrations: Array.isArray(cloned.seniorsRegistrations)
      ? cloned.seniorsRegistrations.map((registration) => normalizeArchivedRegistrationSnapshot(registration)).filter(Boolean)
      : [],
    maybeList: Array.isArray(cloned.maybeList)
      ? cloned.maybeList.map((name) => String(name || '').trim()).filter(Boolean)
      : [],
    skinsPops: normalizeArchivedSkinsPopsSnapshot(cloned.skinsPops || {}),
    weather: normalizeArchivedWeatherSnapshot(cloned.weather || {}),
  };
  if (cloned._id) normalized._id = cloned._id;
  if (cloned.createdAt) normalized.createdAt = cloned.createdAt;
  if (cloned.updatedAt) normalized.updatedAt = cloned.updatedAt;
  normalized.dedupeKey = buildEventStorageDedupeKey(
    normalized.date,
    normalized.teeTimes,
    normalized.isTeamEvent,
    normalized.groupSlug,
    normalized.seniorsRegistrationMode,
    normalized._id,
    normalized.course
  ) || undefined;
  if (!normalized.course || !normalized.date) return null;
  return normalized;
}

function captureEventRestoreSnapshot(ev = {}) {
  const raw = ev && typeof ev.toObject === 'function'
    ? ev.toObject({ depopulate: true })
    : ev;
  return normalizeArchivedEventSnapshot(raw, ev.groupSlug);
}

function captureDeletedSlotArchiveEntry(ev = {}, teeTime = {}) {
  const snapshot = normalizeArchivedSlotSnapshot(
    teeTime && typeof teeTime.toObject === 'function'
      ? teeTime.toObject({ depopulate: true })
      : teeTime
  );
  if (!snapshot) return null;
  const slotId = String(teeTime && teeTime._id || snapshot._id || '').trim();
  const slotIndex = Array.isArray(ev.teeTimes)
    ? ev.teeTimes.findIndex((entry) => String(entry && entry._id || '') === slotId)
    : -1;
  return {
    snapshot,
    slotIndex: slotIndex >= 0 ? slotIndex : null,
    slotLabel: ev && ev.isTeamEvent ? String(snapshot.name || '').trim() : String(snapshot.time || '').trim(),
  };
}

async function archiveDeletedEvent(ev = {}, options = {}) {
  if (!DeletedTeeTimeArchive) throw new Error('Delete archive model not available');
  const eventSnapshot = captureEventRestoreSnapshot(ev);
  if (!eventSnapshot) throw new Error('Unable to capture deleted event snapshot');
  return DeletedTeeTimeArchive.create({
    groupSlug: normalizeGroupSlug(ev.groupSlug),
    archiveType: 'event',
    originalEventId: String(ev && ev._id || '').trim(),
    originalTeeId: '',
    eventCourse: String(ev.course || '').trim(),
    eventDateISO: fmt.dateISO(ev.date),
    isTeamEvent: !!ev.isTeamEvent,
    slotIndex: null,
    slotLabel: '',
    snapshot: eventSnapshot,
    eventSnapshot,
    deleteSource: String(options.deleteSource || '').trim().toLowerCase(),
    deletedBy: String(options.deletedBy || 'SYSTEM').trim() || 'SYSTEM',
    notes: String(options.notes || '').trim(),
    deletedAt: new Date(),
  });
}

async function archiveDeletedTeeTime(ev = {}, teeTime = {}, options = {}) {
  if (!DeletedTeeTimeArchive) throw new Error('Delete archive model not available');
  const eventSnapshot = captureEventRestoreSnapshot(ev);
  const slotArchive = captureDeletedSlotArchiveEntry(ev, teeTime);
  if (!eventSnapshot || !slotArchive || !slotArchive.snapshot) {
    throw new Error('Unable to capture deleted tee-time snapshot');
  }
  return DeletedTeeTimeArchive.create({
    groupSlug: normalizeGroupSlug(ev.groupSlug),
    archiveType: 'tee_time',
    originalEventId: String(ev && ev._id || '').trim(),
    originalTeeId: String(teeTime && teeTime._id || slotArchive.snapshot._id || '').trim(),
    eventCourse: String(ev.course || '').trim(),
    eventDateISO: fmt.dateISO(ev.date),
    isTeamEvent: !!ev.isTeamEvent,
    slotIndex: slotArchive.slotIndex,
    slotLabel: slotArchive.slotLabel,
    snapshot: slotArchive.snapshot,
    eventSnapshot,
    deleteSource: String(options.deleteSource || '').trim().toLowerCase(),
    deletedBy: String(options.deletedBy || 'SYSTEM').trim() || 'SYSTEM',
    notes: String(options.notes || '').trim(),
    deletedAt: new Date(),
  });
}

function parseYearMonthRange(value = '') {
  const match = /^(\d{4})-(\d{2})$/.exec(String(value || '').trim());
  if (!match) return null;
  const year = Number(match[1]);
  const month = Number(match[2]);
  if (!Number.isInteger(year) || !Number.isInteger(month) || month < 1 || month > 12) return null;
  const start = new Date(Date.UTC(year, month - 1, 1, 0, 0, 0, 0));
  const end = new Date(Date.UTC(year, month, 1, 0, 0, 0, 0));
  return {
    month: `${match[1]}-${match[2]}`,
    start,
    end,
  };
}

function isPastYearMonth(value = '', now = new Date()) {
  const range = parseYearMonthRange(value);
  if (!range) return false;
  const currentMonthStart = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), 1, 0, 0, 0, 0));
  return range.start.getTime() < currentMonthStart.getTime();
}

function findArchivedSlotConflict(ev = {}, slotSnapshot = {}, isTeamEvent = false) {
  const wantedId = String(slotSnapshot && slotSnapshot._id || '').trim();
  const wantedTime = String(slotSnapshot && slotSnapshot.time || '').trim();
  const wantedName = String(slotSnapshot && slotSnapshot.name || '').trim().toLowerCase();
  return (Array.isArray(ev && ev.teeTimes) ? ev.teeTimes : []).find((slot) => {
    if (!slot) return false;
    if (wantedId && String(slot._id || '').trim() === wantedId) return true;
    if (!isTeamEvent && wantedTime && String(slot.time || '').trim() === wantedTime) return true;
    if (isTeamEvent && wantedName && String(slot.name || '').trim().toLowerCase() === wantedName) return true;
    return false;
  }) || null;
}

function insertArchivedSlotIntoEvent(ev = {}, slotSnapshot = {}, slotIndex = null) {
  const normalizedSlot = normalizeArchivedSlotSnapshot(slotSnapshot);
  if (!normalizedSlot) throw new Error('Archived tee time not available');
  if (!Array.isArray(ev.teeTimes)) ev.teeTimes = [];
  const conflict = findArchivedSlotConflict(ev, normalizedSlot, !!ev.isTeamEvent);
  if (conflict) {
    return { inserted: false, slot: conflict };
  }
  const targetIndex = Number.isInteger(slotIndex)
    ? Math.max(0, Math.min(slotIndex, ev.teeTimes.length))
    : ev.teeTimes.length;
  ev.teeTimes.splice(targetIndex, 0, normalizedSlot);
  if (!ev.isTeamEvent) {
    ev.teeTimes.sort((left, right) => {
      const leftMinutes = parseHHMMToMinutes(left && left.time);
      const rightMinutes = parseHHMMToMinutes(right && right.time);
      if (leftMinutes === null && rightMinutes === null) return 0;
      if (leftMinutes === null) return 1;
      if (rightMinutes === null) return -1;
      return leftMinutes - rightMinutes;
    });
  }
  return { inserted: true, slot: normalizedSlot };
}

async function restoreEventFromArchivedSnapshot(snapshot = {}, fallbackGroupSlug = DEFAULT_SITE_GROUP_SLUG) {
  const normalized = normalizeArchivedEventSnapshot(snapshot, fallbackGroupSlug);
  if (!normalized) throw new Error('Archived event snapshot is unavailable');
  if (normalized._id) {
    const existing = await Event.findOne({ ...groupScopeFilter(normalized.groupSlug), _id: normalized._id });
    if (existing) return { event: existing, created: false, alreadyExisted: true };
  }
  const restored = await Event.create(normalized);
  return { event: restored, created: true, alreadyExisted: false };
}

function buildTeeTimeRecoveryEntries({ archives = [], activeEvents = [] } = {}) {
  const activeById = new Map((activeEvents || []).map((entry) => [String(entry && entry._id || '').trim(), entry]));
  return (archives || [])
    .map((archive) => {
      const archiveType = String(archive && archive.archiveType || '').trim().toLowerCase();
      const originalEventId = String(archive && archive.originalEventId || '').trim();
      const restoredEventId = String(archive && archive.restoredEventId || '').trim();
      const liveEvent = activeById.get(originalEventId) || activeById.get(restoredEventId) || null;
      const slotSnapshot = archive && archive.snapshot && typeof archive.snapshot === 'object' ? archive.snapshot : {};
      const slotLabelRaw = String(
        archive && archive.slotLabel
          || slotSnapshot.name
          || slotSnapshot.time
          || ''
      ).trim();
      const slotConflict = archiveType === 'tee_time' && liveEvent
        ? findArchivedSlotConflict(liveEvent, slotSnapshot, !!(archive && archive.isTeamEvent))
        : null;
      let canRestore = true;
      let restoreHint = archiveType === 'event' ? 'Restores the full event.' : 'Restores the deleted tee time.';
      if (archiveType === 'event' && liveEvent) {
        canRestore = false;
        restoreHint = 'This event already exists in live data.';
      } else if (archiveType === 'tee_time') {
        if (slotConflict) {
          canRestore = false;
          restoreHint = 'This tee time already exists in live data.';
        } else if (!liveEvent) {
          restoreHint = 'Original event is gone. Restoring will recreate the event snapshot first.';
        }
      }
      if (archive && archive.restoredAt && !canRestore) {
        restoreHint = 'Already restored.';
      }
      return {
        archiveId: String(archive && archive._id || '').trim(),
        archiveType,
        originalEventId,
        originalTeeId: String(archive && archive.originalTeeId || '').trim(),
        eventCourse: String(archive && archive.eventCourse || '').trim(),
        eventDateISO: String(archive && archive.eventDateISO || '').trim(),
        isTeamEvent: !!(archive && archive.isTeamEvent),
        slotLabel: slotLabelRaw,
        slotDisplay: formatAuditSlotLabel(slotLabelRaw, !!(archive && archive.isTeamEvent)),
        deletedAt: archive && archive.deletedAt ? new Date(archive.deletedAt) : null,
        restoredAt: archive && archive.restoredAt ? new Date(archive.restoredAt) : null,
        eventExists: !!liveEvent,
        canRestore,
        restoreHint,
        restoredEventId,
      };
    })
    .filter((entry) => entry.archiveId && entry.deletedAt instanceof Date && !Number.isNaN(entry.deletedAt.getTime()))
    .sort((left, right) => right.deletedAt.getTime() - left.deletedAt.getTime());
}

/* ---------------- Weather helpers ---------------- */
// Default location (Richmond, VA area - adjust for your region)
const DEFAULT_LAT = process.env.DEFAULT_LAT || '37.5407';
const DEFAULT_LON = process.env.DEFAULT_LON || '-77.4360';
const weatherCache = new Map(); // key: `${dateISO}|${lat}|${lon}` -> { data, ts }
const WEATHER_TTL_MS = 3 * 60 * 60 * 1000; // 3 hours
const weatherGeocodeCache = new Map(); // key: normalized query -> { data, ok, ts }
const WEATHER_GEOCODE_TTL_MS = 7 * 24 * 60 * 60 * 1000; // 7 days
const WEATHER_GEOCODE_NEG_TTL_MS = 60 * 60 * 1000; // 1 hour

function getWeatherIcon(weatherCode, isDay = true) {
  // WMO Weather interpretation codes
  // https://open-meteo.com/en/docs
  if (weatherCode === 0) return { icon: '☀️', condition: 'sunny', desc: 'Clear sky' };
  if (weatherCode === 1) return { icon: isDay ? '🌤️' : '🌙', condition: 'mostly-sunny', desc: 'Mainly clear' };
  if (weatherCode === 2) return { icon: '⛅', condition: 'partly-cloudy', desc: 'Partly cloudy' };
  if (weatherCode === 3) return { icon: '☁️', condition: 'cloudy', desc: 'Overcast' };
  if (weatherCode >= 45 && weatherCode <= 48) return { icon: '🌫️', condition: 'foggy', desc: 'Foggy' };
  if (weatherCode >= 51 && weatherCode <= 67) return { icon: '🌧️', condition: 'rainy', desc: 'Rainy' };
  if (weatherCode >= 71 && weatherCode <= 77) return { icon: '🌨️', condition: 'snowy', desc: 'Snow' };
  if (weatherCode >= 80 && weatherCode <= 82) return { icon: '🌦️', condition: 'showers', desc: 'Rain showers' };
  if (weatherCode >= 95) return { icon: '⛈️', condition: 'stormy', desc: 'Thunderstorm' };
  return { icon: '🌤️', condition: 'unknown', desc: 'Unknown' };
}

function toNullableString(value) {
  const raw = String(value || '').trim();
  return raw || null;
}

function toFiniteNumber(value) {
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

function toLatitude(value) {
  const n = toFiniteNumber(value);
  if (n === null) return null;
  if (n < -90 || n > 90) return null;
  return n;
}

function toLongitude(value) {
  const n = toFiniteNumber(value);
  if (n === null) return null;
  if (n < -180 || n > 180) return null;
  return n;
}

function normalizeCourseInfo(input = {}) {
  const courseInfo = (input && typeof input === 'object') ? input : {};
  const out = {};
  const city = toNullableString(courseInfo.city);
  const state = toNullableString(courseInfo.state);
  const phone = toNullableString(courseInfo.phone);
  const website = toNullableString(courseInfo.website);
  const imageUrl = toNullableString(courseInfo.imageUrl);
  const address = toNullableString(courseInfo.address);
  const fullAddress = toNullableString(courseInfo.fullAddress);
  const holesRaw = toFiniteNumber(courseInfo.holes);
  const parRaw = toFiniteNumber(courseInfo.par);
  const latitude = toLatitude(courseInfo.latitude ?? courseInfo.lat);
  const longitude = toLongitude(courseInfo.longitude ?? courseInfo.lon ?? courseInfo.lng);

  if (city) out.city = city;
  if (state) out.state = state;
  if (phone) out.phone = phone;
  if (website) out.website = website;
  if (imageUrl) out.imageUrl = imageUrl;
  if (address) out.address = address;
  if (fullAddress) out.fullAddress = fullAddress;
  if (holesRaw !== null && holesRaw > 0) out.holes = Math.round(holesRaw);
  if (parRaw !== null && parRaw > 0) out.par = Math.round(parRaw);
  if (latitude !== null) out.latitude = latitude;
  if (longitude !== null) out.longitude = longitude;
  return out;
}

function uniqueLocationQueries(queries = []) {
  const out = [];
  const seen = new Set();
  for (const q of queries) {
    const value = String(q || '').trim().replace(/\s+/g, ' ');
    if (!value) continue;
    const key = value.toLowerCase();
    if (seen.has(key)) continue;
    seen.add(key);
    out.push(value);
  }
  return out;
}

async function geocodeLocationQuery(query) {
  const normalizedQuery = String(query || '').trim().replace(/\s+/g, ' ');
  if (!normalizedQuery) return null;

  const cacheKey = normalizedQuery.toLowerCase();
  const cached = weatherGeocodeCache.get(cacheKey);
  if (cached) {
    const ttl = cached.ok ? WEATHER_GEOCODE_TTL_MS : WEATHER_GEOCODE_NEG_TTL_MS;
    if (Date.now() - cached.ts < ttl) return cached.data;
  }

  try {
    const url = `https://geocoding-api.open-meteo.com/v1/search?name=${encodeURIComponent(normalizedQuery)}&count=5&language=en&format=json`;
    const response = await fetch(url);
    if (!response.ok) throw new Error(`Geocoding HTTP ${response.status}`);

    const payload = await response.json();
    const results = Array.isArray(payload && payload.results) ? payload.results : [];
    const findValid = (list = []) => list.find((entry) => toLatitude(entry.latitude) !== null && toLongitude(entry.longitude) !== null);
    const usMatch = findValid(results.filter((entry) => String(entry.country_code || '').toUpperCase() === 'US'));
    const best = usMatch || findValid(results);

    const geocoded = best
      ? {
          lat: Number(best.latitude),
          lon: Number(best.longitude),
          source: 'geocoded',
          query: normalizedQuery,
        }
      : null;

    weatherGeocodeCache.set(cacheKey, { data: geocoded, ok: !!geocoded, ts: Date.now() });
    return geocoded;
  } catch (err) {
    console.warn('Weather geocode error:', err.message, '(query:', normalizedQuery, ')');
    weatherGeocodeCache.set(cacheKey, { data: null, ok: false, ts: Date.now() });
    return null;
  }
}

async function resolveWeatherCoordinates(eventLike = {}) {
  const courseInfo = (eventLike && eventLike.courseInfo && typeof eventLike.courseInfo === 'object')
    ? eventLike.courseInfo
    : {};
  const storedLat = toLatitude(courseInfo.latitude ?? courseInfo.lat);
  const storedLon = toLongitude(courseInfo.longitude ?? courseInfo.lon ?? courseInfo.lng);
  if (storedLat !== null && storedLon !== null) {
    return { lat: storedLat, lon: storedLon, source: 'course-info' };
  }

  const course = toNullableString(eventLike.course);
  const city = toNullableString(courseInfo.city);
  const state = toNullableString(courseInfo.state);
  const address = toNullableString(courseInfo.address || courseInfo.fullAddress);

  const queries = uniqueLocationQueries([
    [course, address, city, state].filter(Boolean).join(', '),
    [course, city, state].filter(Boolean).join(', '),
    [address, city, state].filter(Boolean).join(', '),
    [course, city].filter(Boolean).join(', '),
    [course, state].filter(Boolean).join(', '),
    [city, state].filter(Boolean).join(', '),
    course || '',
  ]);

  for (const query of queries) {
    const geocoded = await geocodeLocationQuery(query);
    if (geocoded) return geocoded;
  }

  const fallbackLat = toLatitude(DEFAULT_LAT);
  const fallbackLon = toLongitude(DEFAULT_LON);
  return {
    lat: fallbackLat !== null ? fallbackLat : 37.5407,
    lon: fallbackLon !== null ? fallbackLon : -77.4360,
    source: 'default',
  };
}

async function fetchWeatherForEvent(eventLike = {}) {
  const date = eventLike && eventLike.date;
  const coords = await resolveWeatherCoordinates(eventLike);
  return fetchWeatherForecast(date, coords.lat, coords.lon);
}

function weatherFromTextForecast(text = '') {
  const raw = String(text || '').trim();
  const lower = raw.toLowerCase();
  if (!raw) return { icon: '🌤️', condition: 'unknown', desc: 'Unknown' };
  if (/thunder|storm/.test(lower)) return { icon: '⛈️', condition: 'stormy', desc: raw };
  if (/snow|sleet|flurr/.test(lower)) return { icon: '🌨️', condition: 'snowy', desc: raw };
  if (/shower|rain|drizzle/.test(lower)) return { icon: '🌧️', condition: 'rainy', desc: raw };
  if (/fog|mist|haze/.test(lower)) return { icon: '🌫️', condition: 'foggy', desc: raw };
  if (/overcast|cloudy/.test(lower)) return { icon: '☁️', condition: 'cloudy', desc: raw };
  if (/partly|mostly sunny|mostly clear/.test(lower)) return { icon: '⛅', condition: 'partly-cloudy', desc: raw };
  if (/clear|sunny|fair/.test(lower)) return { icon: '☀️', condition: 'sunny', desc: raw };
  return { icon: '🌤️', condition: 'unknown', desc: raw };
}

async function fetchNwsWeatherForecast(date, lat, lon) {
  const dateStr = date.toISOString().split('T')[0];
  const pointsRes = await fetch(`https://api.weather.gov/points/${lat},${lon}`, {
    headers: {
      'Accept': 'application/geo+json',
      'User-Agent': 'tee-time-brs/1.0 weather fallback',
    },
  });
  if (!pointsRes.ok) throw new Error(`NWS points HTTP ${pointsRes.status}`);
  const pointsJson = await pointsRes.json();
  const forecastUrl = pointsJson && pointsJson.properties && pointsJson.properties.forecast;
  if (!forecastUrl) throw new Error('NWS forecast URL unavailable');

  const forecastRes = await fetch(forecastUrl, {
    headers: {
      'Accept': 'application/geo+json',
      'User-Agent': 'tee-time-brs/1.0 weather fallback',
    },
  });
  if (!forecastRes.ok) throw new Error(`NWS forecast HTTP ${forecastRes.status}`);
  const forecastJson = await forecastRes.json();
  const periods = Array.isArray(forecastJson && forecastJson.properties && forecastJson.properties.periods)
    ? forecastJson.properties.periods
    : [];
  const matchingPeriods = periods.filter((period) => {
    const start = String(period && period.startTime || '').trim();
    return start && start.slice(0, 10) === dateStr;
  });
  if (!matchingPeriods.length) throw new Error('NWS forecast not available for date');

  const temps = matchingPeriods
    .map((period) => Number(period && period.temperature))
    .filter((value) => Number.isFinite(value));
  const rainChance = matchingPeriods
    .map((period) => Number(period && period.probabilityOfPrecipitation && period.probabilityOfPrecipitation.value))
    .filter((value) => Number.isFinite(value))
    .reduce((max, value) => Math.max(max, value), 0);
  const dayPeriod = matchingPeriods.find((period) => period && period.isDaytime) || matchingPeriods[0];
  const description = String(dayPeriod && (dayPeriod.shortForecast || dayPeriod.detailedForecast) || '').trim();
  const info = weatherFromTextForecast(description);
  const tempHigh = temps.length ? Math.max(...temps) : null;
  const tempLow = temps.length ? Math.min(...temps) : null;
  const temp = tempHigh !== null && tempLow !== null
    ? Math.round((tempHigh + tempLow) / 2)
    : (tempHigh !== null ? tempHigh : tempLow);

  return {
    success: true,
    condition: info.condition,
    icon: info.icon,
    temp: Number.isFinite(Number(temp)) ? Number(temp) : null,
    tempLow: Number.isFinite(Number(tempLow)) ? Number(tempLow) : null,
    tempHigh: Number.isFinite(Number(tempHigh)) ? Number(tempHigh) : null,
    rainChance: Number.isFinite(Number(rainChance)) ? Number(rainChance) : null,
    description: info.desc,
    lastFetched: new Date(),
  };
}

async function fetchWeatherForecast(date, lat = DEFAULT_LAT, lon = DEFAULT_LON) {
  try {
    if (!date || !(date instanceof Date) || isNaN(date.getTime())) {
      console.error('Weather fetch error: Invalid or missing date', date);
      return {
        success: false,
        condition: 'error',
        icon: '🌤️',
        temp: null,
        tempLow: null,
        tempHigh: null,
        rainChance: null,
        description: 'Invalid or missing event date',
        lastFetched: null
      };
    }
    const dateStr = date.toISOString().split('T')[0]; // YYYY-MM-DD
    const cacheKey = `${dateStr}|${lat}|${lon}`;
    const cached = weatherCache.get(cacheKey);
    if (cached && Date.now() - cached.ts < WEATHER_TTL_MS) {
      return cached.data;
    }
    const today = new Date();
    const daysAhead = Math.ceil((date - today) / (1000 * 60 * 60 * 24));
    // Open-Meteo provides forecasts up to 16 days ahead
    if (daysAhead > 16) {
      console.log(`Weather: Event is ${daysAhead} days ahead (max 16), returning placeholder`);
      return {
        success: false,
        condition: 'unknown',
        icon: '🌤️',
        temp: null,
        tempLow: null,
        tempHigh: null,
        rainChance: null,
        description: 'Forecast not yet available',
        lastFetched: null
      };
    }
    const url = `https://api.open-meteo.com/v1/forecast?latitude=${lat}&longitude=${lon}&daily=weather_code,temperature_2m_max,temperature_2m_min,precipitation_probability_max&temperature_unit=fahrenheit&timezone=auto&start_date=${dateStr}&end_date=${dateStr}`;
    let response = null;
    let lastWeatherError = null;
    for (let attempt = 1; attempt <= 2; attempt += 1) {
      response = await fetch(url);
      if (response.ok) break;
      lastWeatherError = new Error(`Weather API HTTP ${response.status}`);
      console.error(`Weather API HTTP error: ${response.status} ${response.statusText} (attempt ${attempt})`);
      if (response.status < 500 || attempt === 2) break;
      await new Promise((resolve) => setTimeout(resolve, 350 * attempt));
    }
    if (!response || !response.ok) {
      if (toLatitude(lat) !== null && toLongitude(lon) !== null) {
        try {
          return await fetchNwsWeatherForecast(date, lat, lon);
        } catch (fallbackError) {
          console.error('Weather fallback error:', fallbackError.message, '(Date:', dateStr, 'Lat:', lat, 'Lon:', lon, ')');
        }
      }
      throw lastWeatherError || new Error('Weather API unavailable');
    }
    const data = await response.json();
    if (!data.daily || !data.daily.weather_code || data.daily.weather_code[0] === undefined) {
      console.error('Weather API returned incomplete data:', JSON.stringify(data));
      throw new Error('No weather data available');
    }
    const weatherCode = data.daily.weather_code[0];
    const tempMax = data.daily.temperature_2m_max[0];
    const tempMin = data.daily.temperature_2m_min[0];
    const precipMax = data.daily.precipitation_probability_max ? data.daily.precipitation_probability_max[0] : null;
    const avgTemp = Math.round((tempMax + tempMin) / 2);
    const weatherInfo = getWeatherIcon(weatherCode, true);
    const roundedLow = Number.isFinite(Number(tempMin)) ? Math.round(Number(tempMin)) : null;
    const roundedHigh = Number.isFinite(Number(tempMax)) ? Math.round(Number(tempMax)) : null;
    const rainChance = Number.isFinite(Number(precipMax)) ? Math.round(Number(precipMax)) : null;
    const out = {
      success: true,
      condition: weatherInfo.condition,
      icon: weatherInfo.icon,
      temp: avgTemp,
      tempLow: roundedLow,
      tempHigh: roundedHigh,
      rainChance,
      description: weatherInfo.desc,
      lastFetched: new Date()
    };
    weatherCache.set(cacheKey, { data: out, ts: Date.now() });
    return out;
  } catch (e) {
    let dateStr = 'undefined';
    if (date && date instanceof Date && !isNaN(date.getTime())) {
      dateStr = date.toISOString().split('T')[0];
    }
    console.error('Weather fetch error:', e.message, '(Date:', dateStr, 'Lat:', lat, 'Lon:', lon, ')');
    return {
      success: false,
      condition: 'error',
      icon: '🌧️',
      temp: null,
      tempLow: null,
      tempHigh: null,
      rainChance: null,
      description: 'Weather unavailable',
      lastFetched: null
    };
  }
}

function assignWeatherToEvent(ev, weatherData = {}) {
  if (!ev.weather) ev.weather = {};
  ev.weather.condition = weatherData.condition || null;
  ev.weather.icon = weatherData.icon || null;
  ev.weather.temp = Number.isFinite(Number(weatherData.temp)) ? Number(weatherData.temp) : null;
  ev.weather.tempLow = Number.isFinite(Number(weatherData.tempLow)) ? Number(weatherData.tempLow) : null;
  ev.weather.tempHigh = Number.isFinite(Number(weatherData.tempHigh)) ? Number(weatherData.tempHigh) : null;
  ev.weather.rainChance = Number.isFinite(Number(weatherData.rainChance)) ? Number(weatherData.rainChance) : null;
  ev.weather.description = weatherData.description || null;
  ev.weather.lastFetched = weatherData.lastFetched || null;
}

/* ---------------- Email helpers ---------------- */
const nodemailer = require('nodemailer');
let transporter = null;

async function ensureTransporter() {
  if (transporter || !process.env.RESEND_API_KEY) return transporter;
  
  // Use Resend SMTP
  transporter = nodemailer.createTransport({
    host: 'smtp.resend.com',
    port: 465,
    secure: true,
    auth: {
      user: 'resend',
      pass: process.env.RESEND_API_KEY
    }
  });
  
  return transporter;
}

function normalizeEmailSubject(subject = '') {
  const raw = String(subject || '').trim();
  if (process.env.E2E_TEST_MODE === '1' && !/^THIS IS A TEST\b/i.test(raw)) {
    return `THIS IS A TEST - ${raw}`;
  }
  return raw;
}

function isE2ETestMode() {
  return process.env.E2E_TEST_MODE === '1';
}

async function sendEmail(to, subject, html) {
  if (isE2ETestMode()) {
    return { ok: true, simulated: true, data: { to, subject: normalizeEmailSubject(subject), bytes: String(html || '').length } };
  }
  const mailer = await ensureTransporter();
  if (!mailer || !process.env.RESEND_FROM) {
    console.warn(JSON.stringify({ level:'warn', msg:'Email disabled', reason:'missing key/from' }));
    return { ok:false, disabled:true };
  }
  
  try {
    const normalizedSubject = normalizeEmailSubject(subject);
    const info = await mailer.sendMail({
      from: process.env.RESEND_FROM,
      to: to,
      subject: normalizedSubject,
      html: html
    });
    return { ok: true, data: { id: info.messageId } };
  } catch (err) {
    return { ok: false, error: { message: err.message } };
  }
}

// HTTP fallback to Resend API (avoids SMTP egress issues)
async function sendEmailViaResendApi(to, subject, html, options = {}) {
  if (isE2ETestMode()) {
    const normalizedSubject = normalizeEmailSubject(subject);
    return { ok: true, simulated: true, data: { to, cc: options && options.cc, subject: normalizedSubject, bytes: String(html || '').length } };
  }
  if (!process.env.RESEND_API_KEY || !process.env.RESEND_FROM) {
    return { ok: false, error: { message: 'Resend API key/from not configured' } };
  }
  const normalizedSubject = normalizeEmailSubject(subject);
  const payload = {
    from: process.env.RESEND_FROM,
    to: Array.isArray(to) ? to : [to],
    subject: normalizedSubject,
    html,
  };
  if (options.cc) payload.cc = Array.isArray(options.cc) ? options.cc : [options.cc];
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), 8000);
  try {
    const resp = await fetch('https://api.resend.com/emails', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        Authorization: `Bearer ${process.env.RESEND_API_KEY}`,
      },
      body: JSON.stringify(payload),
      signal: controller.signal,
    });
    clearTimeout(timer);
    if (!resp.ok) {
      const text = await resp.text();
      return { ok: false, error: { message: `Resend HTTP ${resp.status}: ${text}` } };
    }
    const data = await resp.json();
    return { ok: true, data };
  } catch (err) {
    clearTimeout(timer);
    return { ok: false, error: { message: err.message } };
  }
}

/* Helper to check if notifications are globally enabled */
async function areNotificationsEnabled(groupSlug = DEFAULT_SITE_GROUP_SLUG) {
  if (!Settings) return true; // Default to enabled if Settings model not available
  try {
    const setting = await Settings.findOne({ ...groupScopeFilter(groupSlug), key: 'notificationsEnabled' });
    return setting ? setting.value !== false : true; // Default to true if not set
  } catch (e) {
    console.error('Error checking notification settings:', e);
    return true; // Fail open - allow notifications
  }
}

async function ensureEventIndexes() {
  if (!Event || !Event.collection) return;
  try {
    const indexes = await Event.collection.indexes();
    const dedupeIndex = indexes.find((index) => index && index.name === 'groupSlug_1_dedupeKey_1');
    const needsMigration = !dedupeIndex
      || !dedupeIndex.unique
      || !dedupeIndex.partialFilterExpression
      || JSON.stringify(dedupeIndex.partialFilterExpression) !== JSON.stringify({ dedupeKey: { $type: 'string' } });
    if (needsMigration && dedupeIndex) {
      await Event.collection.dropIndex('groupSlug_1_dedupeKey_1');
      console.log(JSON.stringify({ t:new Date().toISOString(), level:'info', msg:'event-index-migrated', dropped:'groupSlug_1_dedupeKey_1' }));
    }
    if (needsMigration) {
      await Event.collection.createIndex(
        { groupSlug: 1, dedupeKey: 1 },
        {
          unique: true,
          name: 'groupSlug_1_dedupeKey_1',
          partialFilterExpression: { dedupeKey: { $type: 'string' } },
        }
      );
      console.log(JSON.stringify({ t:new Date().toISOString(), level:'info', msg:'event-index-migrated', created:'groupSlug_1_dedupeKey_1' }));
    }
  } catch (error) {
    console.error('Failed to ensure event indexes:', error);
  }
}

function getTeeTimeAuditWindowStart(now = new Date()) {
  return new Date(now.getTime() - TEE_TIME_AUDIT_RETENTION_MS);
}

function sameIndexKeyPattern(index = {}, key = {}) {
  return JSON.stringify(index && index.key || {}) === JSON.stringify(key);
}

async function ensureTtlIndex(model, key, name, expireAfterSeconds) {
  if (!model || !model.collection) return;
  const indexes = await model.collection.indexes();
  const existing = indexes.find((index) => index && (index.name === name || sameIndexKeyPattern(index, key)));
  const matches = existing
    && sameIndexKeyPattern(existing, key)
    && Number(existing.expireAfterSeconds) === Number(expireAfterSeconds);
  if (matches) return;
  if (existing && existing.name) await model.collection.dropIndex(existing.name);
  await model.collection.createIndex(key, { name, expireAfterSeconds });
}

async function ensureTeeTimeAuditIndexes() {
  try {
    await ensureTtlIndex(AuditLog, { timestamp: 1 }, 'timestamp_1', TEE_TIME_AUDIT_RETENTION_SECONDS);
    await ensureTtlIndex(TeeTimeLog, { createdAt: 1 }, 'createdAt_1', TEE_TIME_AUDIT_RETENTION_SECONDS);
    await ensureTtlIndex(DeletedTeeTimeArchive, { deletedAt: 1 }, 'deletedAt_1', TEE_TIME_AUDIT_RETENTION_SECONDS);
  } catch (error) {
    console.error('Failed to ensure tee-time audit indexes:', error);
  }
}

async function areSubscriberChangeNotificationsEnabled(groupSlug = DEFAULT_SITE_GROUP_SLUG) {
  if (!Settings) return true;
  try {
    const setting = await Settings.findOne({ ...groupScopeFilter(groupSlug), key: 'subscriberChangeNotificationsEnabled' });
    return setting ? setting.value !== false : true;
  } catch (e) {
    console.error('Error checking subscriber change notification settings:', e);
    return true;
  }
}

async function areTeeTimeEventLifecycleNotificationsEnabled(groupSlug = DEFAULT_SITE_GROUP_SLUG) {
  if (!Settings) return false;
  try {
    const setting = await Settings.findOne({ ...groupScopeFilter(groupSlug), key: 'teeTimeEventLifecycleNotificationsEnabled' });
    return setting ? setting.value === true : false;
  } catch (e) {
    console.error('Error checking tee-time event lifecycle notification settings:', e);
    return false;
  }
}

const SCHEDULER_SETTINGS_CACHE_TTL_MS = 30 * 1000;
let schedulerEnabledCache = new Map();
let scheduledEmailRulesCache = new Map();
const BACKUP_SETTINGS_DEFAULTS = Object.freeze({
  monthlyEnabled: true,
  monthlyDay: 1,
  monthlyHour: 2,
  monthlyMinute: 15,
  weeklyEnabled: false,
  weeklyDay: 0,
  weeklyHour: 2,
  weeklyMinute: 15,
  dailyEnabled: false,
  dailyHour: 2,
  dailyMinute: 15,
  activeSeasonOnly: false,
  activeSeasonStartMonth: 3,
  activeSeasonEndMonth: 10,
  retainCount: 12,
  offsiteCopyEnabled: false,
  offsiteLocation: '',
});
let backupSettingsCache = { value: { ...BACKUP_SETTINGS_DEFAULTS }, ts: 0 };
const BACKUP_STATUS_DEFAULTS = Object.freeze({
  lastSuccessfulBackupAt: null,
  lastSuccessfulBackupId: '',
  lastSuccessfulBackupBytes: 0,
  lastFailureAt: null,
  lastFailureMessage: '',
});
let backupStatusCache = { value: { ...BACKUP_STATUS_DEFAULTS }, ts: 0 };
const groupAccessControlCache = new Map();

async function areSchedulerJobsEnabled(groupSlug = DEFAULT_SITE_GROUP_SLUG) {
  if (SCHEDULER_ENV_DISABLED) return false;
  const cacheKey = normalizeGroupSlug(groupSlug);
  const now = Date.now();
  const cached = schedulerEnabledCache.get(cacheKey);
  if (cached && (now - cached.ts < SCHEDULER_SETTINGS_CACHE_TTL_MS)) {
    return cached.value;
  }
  let enabled = true;
  if (Settings) {
    try {
      const setting = await Settings.findOne({ ...groupScopeFilter(cacheKey), key: 'schedulerEnabled' });
      enabled = setting ? setting.value !== false : true;
    } catch (e) {
      console.error('Error checking scheduler settings:', e);
      enabled = true;
    }
  }
  schedulerEnabledCache.set(cacheKey, { value: enabled, ts: now });
  return enabled;
}

function normalizeScheduledEmailRules(rawValue) {
  const normalized = { ...SCHEDULED_EMAIL_RULE_DEFAULTS };
  if (!rawValue || typeof rawValue !== 'object') return normalized;
  for (const key of SCHEDULED_EMAIL_RULE_KEYS) {
    if (typeof rawValue[key] === 'boolean') {
      normalized[key] = rawValue[key];
    }
  }
  return normalized;
}

async function getScheduledEmailRules(groupSlug = DEFAULT_SITE_GROUP_SLUG) {
  const cacheKey = normalizeGroupSlug(groupSlug);
  const now = Date.now();
  const cached = scheduledEmailRulesCache.get(cacheKey);
  if (cached && (now - cached.ts < SCHEDULER_SETTINGS_CACHE_TTL_MS)) {
    return cached.value;
  }
  let rules = { ...SCHEDULED_EMAIL_RULE_DEFAULTS };
  if (Settings) {
    try {
      const setting = await Settings.findOne({ ...groupScopeFilter(cacheKey), key: 'scheduledEmailRules' });
      rules = normalizeScheduledEmailRules(setting && setting.value);
    } catch (e) {
      console.error('Error checking scheduled email rule settings:', e);
    }
  }
  scheduledEmailRulesCache.set(cacheKey, { value: rules, ts: now });
  return rules;
}

function buildScheduledJobClaimKey(jobKey = '', scope = '') {
  return `scheduledJobClaim:${String(jobKey || '').trim()}:${String(scope || '').trim()}`;
}

async function claimScheduledJobRunOnce(groupSlug = DEFAULT_SITE_GROUP_SLUG, jobKey = '', scope = '') {
  if (!Settings) return true;
  const normalizedGroupSlug = normalizeGroupSlug(groupSlug);
  const key = buildScheduledJobClaimKey(jobKey, scope);
  try {
    const result = await Settings.updateOne(
      { groupSlug: normalizedGroupSlug, key },
      {
        $setOnInsert: {
          groupSlug: normalizedGroupSlug,
          key,
          value: {
            claimedAt: new Date().toISOString(),
            jobKey: String(jobKey || '').trim(),
            scope: String(scope || '').trim(),
          },
        },
      },
      { upsert: true }
    );
    return !!(result && result.upsertedCount);
  } catch (error) {
    if (error && error.code === 11000) return false;
    console.error('Error claiming scheduled job run:', error);
    return false;
  }
}

function normalizeBackupSettings(rawValue) {
  const base = { ...BACKUP_SETTINGS_DEFAULTS };
  if (!rawValue || typeof rawValue !== 'object') return base;
  if (typeof rawValue.monthlyEnabled === 'boolean') base.monthlyEnabled = rawValue.monthlyEnabled;
  const monthlyDay = Number(rawValue.monthlyDay);
  const monthlyHour = Number(rawValue.monthlyHour);
  const monthlyMinute = Number(rawValue.monthlyMinute);
  const weeklyDay = Number(rawValue.weeklyDay);
  const weeklyHour = Number(rawValue.weeklyHour);
  const weeklyMinute = Number(rawValue.weeklyMinute);
  const dailyHour = Number(rawValue.dailyHour);
  const dailyMinute = Number(rawValue.dailyMinute);
  const activeSeasonStartMonth = Number(rawValue.activeSeasonStartMonth);
  const activeSeasonEndMonth = Number(rawValue.activeSeasonEndMonth);
  const retainCount = Number(rawValue.retainCount);
  if (Number.isInteger(monthlyDay) && monthlyDay >= 1 && monthlyDay <= 28) base.monthlyDay = monthlyDay;
  if (Number.isInteger(monthlyHour) && monthlyHour >= 0 && monthlyHour <= 23) base.monthlyHour = monthlyHour;
  if (Number.isInteger(monthlyMinute) && monthlyMinute >= 0 && monthlyMinute <= 59) base.monthlyMinute = monthlyMinute;
  if (typeof rawValue.weeklyEnabled === 'boolean') base.weeklyEnabled = rawValue.weeklyEnabled;
  if (Number.isInteger(weeklyDay) && weeklyDay >= 0 && weeklyDay <= 6) base.weeklyDay = weeklyDay;
  if (Number.isInteger(weeklyHour) && weeklyHour >= 0 && weeklyHour <= 23) base.weeklyHour = weeklyHour;
  if (Number.isInteger(weeklyMinute) && weeklyMinute >= 0 && weeklyMinute <= 59) base.weeklyMinute = weeklyMinute;
  if (typeof rawValue.dailyEnabled === 'boolean') base.dailyEnabled = rawValue.dailyEnabled;
  if (Number.isInteger(dailyHour) && dailyHour >= 0 && dailyHour <= 23) base.dailyHour = dailyHour;
  if (Number.isInteger(dailyMinute) && dailyMinute >= 0 && dailyMinute <= 59) base.dailyMinute = dailyMinute;
  if (typeof rawValue.activeSeasonOnly === 'boolean') base.activeSeasonOnly = rawValue.activeSeasonOnly;
  if (Number.isInteger(activeSeasonStartMonth) && activeSeasonStartMonth >= 1 && activeSeasonStartMonth <= 12) base.activeSeasonStartMonth = activeSeasonStartMonth;
  if (Number.isInteger(activeSeasonEndMonth) && activeSeasonEndMonth >= 1 && activeSeasonEndMonth <= 12) base.activeSeasonEndMonth = activeSeasonEndMonth;
  if (Number.isInteger(retainCount) && retainCount >= 1 && retainCount <= 120) base.retainCount = retainCount;
  if (typeof rawValue.offsiteCopyEnabled === 'boolean') base.offsiteCopyEnabled = rawValue.offsiteCopyEnabled;
  if (typeof rawValue.offsiteLocation === 'string') base.offsiteLocation = rawValue.offsiteLocation.trim().slice(0, 200);
  return base;
}

async function getBackupSettings() {
  const now = Date.now();
  if (now - backupSettingsCache.ts < SCHEDULER_SETTINGS_CACHE_TTL_MS) {
    return backupSettingsCache.value;
  }
  let settings = { ...BACKUP_SETTINGS_DEFAULTS };
  if (Settings) {
    try {
      const setting = await Settings.findOne(scopedSettingQuery(DEFAULT_SITE_GROUP_SLUG, 'backupSettings'));
      settings = normalizeBackupSettings(setting && setting.value);
    } catch (e) {
      console.error('Error checking backup settings:', e);
    }
  }
  backupSettingsCache = { value: settings, ts: now };
  return settings;
}

function normalizeBackupStatus(rawValue) {
  const base = { ...BACKUP_STATUS_DEFAULTS };
  if (!rawValue || typeof rawValue !== 'object') return base;
  if (rawValue.lastSuccessfulBackupAt) base.lastSuccessfulBackupAt = String(rawValue.lastSuccessfulBackupAt);
  if (rawValue.lastSuccessfulBackupId) base.lastSuccessfulBackupId = String(rawValue.lastSuccessfulBackupId);
  if (Number.isFinite(Number(rawValue.lastSuccessfulBackupBytes))) base.lastSuccessfulBackupBytes = Number(rawValue.lastSuccessfulBackupBytes);
  if (rawValue.lastFailureAt) base.lastFailureAt = String(rawValue.lastFailureAt);
  if (rawValue.lastFailureMessage) base.lastFailureMessage = String(rawValue.lastFailureMessage).slice(0, 500);
  return base;
}

async function getBackupStatus() {
  const now = Date.now();
  if (now - backupStatusCache.ts < SCHEDULER_SETTINGS_CACHE_TTL_MS) {
    return backupStatusCache.value;
  }
  let status = { ...BACKUP_STATUS_DEFAULTS };
  if (Settings) {
    try {
      const setting = await Settings.findOne(scopedSettingQuery(DEFAULT_SITE_GROUP_SLUG, 'backupStatus'));
      status = normalizeBackupStatus(setting && setting.value);
    } catch (e) {
      console.error('Error checking backup status:', e);
    }
  }
  backupStatusCache = { value: status, ts: now };
  return status;
}

async function saveBackupStatus(nextStatus) {
  const status = normalizeBackupStatus(nextStatus);
  if (Settings) {
    await Settings.findOneAndUpdate(
      scopedSettingQuery(DEFAULT_SITE_GROUP_SLUG, 'backupStatus'),
      { groupSlug: DEFAULT_SITE_GROUP_SLUG, key: 'backupStatus', value: status },
      { upsert: true, new: true }
    );
  }
  backupStatusCache = { value: status, ts: Date.now() };
  return status;
}

async function updateBackupStatus(patch = {}) {
  const current = await getBackupStatus();
  return saveBackupStatus({ ...current, ...patch });
}

async function recordBackupFailure(error) {
  const message = error && error.message ? error.message : String(error || 'Unknown backup error');
  return updateBackupStatus({
    lastFailureAt: new Date().toISOString(),
    lastFailureMessage: message,
  });
}

async function sendEmailToAll(subject, html, options = {}) {
  if (!Subscriber) return { ok:false, reason:'no model' };
  const groupSlug = normalizeGroupSlug(options.groupSlug);
  // Check if notifications are globally enabled
  const notifEnabled = await areNotificationsEnabled(groupSlug);
  if (!notifEnabled) {
    console.log(JSON.stringify({ t:new Date().toISOString(), level:'info', msg:'Notifications disabled for group, skipping email', groupSlug }));
    return { ok:true, sent:0, disabled:true };
  }
  const subs = await Subscriber.find({ ...groupScopeFilter(groupSlug) }).lean();
  if (!subs.length) return { ok:true, sent:0 };
  let sent = 0;
  for (const s of subs) {
    try {
      // Add personalized unsubscribe link
      const unsubLink = `${SITE_URL}api/unsubscribe/${s.unsubscribeToken}`;
      
      // Add unsubscribe link to the HTML
      const htmlWithUnsub = html.replace(
        /You received this because you subscribed to tee time updates\./,
        `You received this because you subscribed to tee time updates. <a href="${unsubLink}" style="color:#6b7280;text-decoration:underline">Unsubscribe</a>`
      );
      await sendEmail(s.email, subject, htmlWithUnsub);
      sent++; 
    } catch {}
  }
  return { ok:true, sent };
}

async function sendSubscriberChangeEmail(subject, html, options = {}) {
  const groupSlug = normalizeGroupSlug(options.groupSlug);
  const changeNotificationsEnabled = await areSubscriberChangeNotificationsEnabled(groupSlug);
  if (!changeNotificationsEnabled) {
    console.log(JSON.stringify({ t:new Date().toISOString(), level:'info', msg:'Subscriber change notifications disabled for group, skipping email', groupSlug }));
    return { ok: true, sent: 0, disabled: true, changeNotificationsDisabled: true };
  }
  return sendEmailToAll(subject, html, { ...options, groupSlug });
}

async function sendTeeTimeEventLifecycleEmail(subject, html, options = {}) {
  const groupSlug = normalizeGroupSlug(options.groupSlug);
  const lifecycleNotificationsEnabled = await areTeeTimeEventLifecycleNotificationsEnabled(groupSlug);
  if (!lifecycleNotificationsEnabled) {
    console.log(JSON.stringify({ t:new Date().toISOString(), level:'info', msg:'tee-time event lifecycle notifications disabled for group, skipping email', groupSlug }));
    return { ok: true, sent: 0, disabled: true, teeTimeEventLifecycleDisabled: true };
  }
  return sendSubscriberChangeEmail(subject, html, { ...options, groupSlug });
}

/* ---------------- Formatting + dates ---------------- */
function esc(s=''){ return String(s).replace(/[&<>"]/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c])); }
function asUTCDate(x){
  if (!x) return new Date(NaN);
  if (x instanceof Date) return new Date(Date.UTC(x.getUTCFullYear(), x.getUTCMonth(), x.getUTCDate(), 12, 0, 0));
  const s = String(x).trim();
  if (/^\d{4}-\d{2}-\d{2}$/.test(s)) return new Date(s + 'T12:00:00Z');
  const d = new Date(s);
  return isNaN(d) ? new Date(NaN) : new Date(Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate(), 12, 0, 0));
}
const fmt = {
  dateISO(x){ const d = asUTCDate(x); return isNaN(d) ? '' : d.toISOString().slice(0,10); },
  dateLong(x){ const d = asUTCDate(x); return isNaN(d) ? '' : d.toLocaleDateString(undefined,{ weekday:'long', month:'long', day:'numeric', year:'numeric', timeZone:'UTC' }); },
  dateShortTitle(x){ const d = asUTCDate(x); return isNaN(d) ? '' : d.toLocaleDateString(undefined,{ weekday:'short', month:'numeric', day:'numeric', timeZone:'UTC' }); },
  tee(t){ if(!t) return ''; const m=/^(\d{1,2}):(\d{2})$/.exec(t); if(!m) return t; const H=+m[1], M=m[2]; const ap=H>=12?'PM':'AM'; const h=(H%12)||12; return `${h}:${M} ${ap}`; }
};

function parseHHMMToMinutes(rawTime = '') {
  const match = /^(\d{1,2}):(\d{2})$/.exec(String(rawTime).trim());
  if (!match) return null;
  const hours = Number(match[1]);
  const minutes = Number(match[2]);
  if (!Number.isInteger(hours) || !Number.isInteger(minutes) || hours < 0 || hours > 23 || minutes < 0 || minutes > 59) return null;
  return (hours * 60) + minutes;
}

function parseTimeZoneOffsetMinutes(label = 'GMT') {
  const match = /^GMT(?:(\+|-)(\d{1,2})(?::?(\d{2}))?)?$/.exec(String(label || '').trim());
  if (!match) return 0;
  const sign = match[1] === '-' ? -1 : 1;
  const hours = Number(match[2] || 0);
  const minutes = Number(match[3] || 0);
  return sign * ((hours * 60) + minutes);
}

function timeZoneOffsetMinutesAt(date = new Date(), timeZone = LOCAL_TZ) {
  try {
    const parts = new Intl.DateTimeFormat('en-US', {
      timeZone,
      timeZoneName: 'shortOffset',
      hour: '2-digit',
      minute: '2-digit',
      hour12: false,
    }).formatToParts(date);
    const label = parts.find((part) => part.type === 'timeZoneName')?.value || 'GMT';
    return parseTimeZoneOffsetMinutes(label);
  } catch (_) {
    return 0;
  }
}

function eventLocalDateTimeToUtc(dateValue, rawTime, timeZone = LOCAL_TZ) {
  const dateISO = fmt.dateISO(dateValue);
  const minutes = parseHHMMToMinutes(rawTime);
  if (!dateISO || minutes === null) return null;
  const [year, month, day] = dateISO.split('-').map(Number);
  const offsetMinutes = timeZoneOffsetMinutesAt(asUTCDate(dateISO), timeZone);
  return new Date(Date.UTC(year, month - 1, day, 0, minutes - offsetMinutes, 0, 0));
}

function weekendGameEligibleEvent(ev = {}) {
  if (!ev) return false;
  if (normalizeGroupSlug(ev.groupSlug) !== DEFAULT_SITE_GROUP_SLUG) return false;
  if (ev.isTeamEvent) return false;
  const dateISO = fmt.dateISO(ev.date);
  if (!dateISO) return false;
  return Array.isArray(ev.teeTimes) && ev.teeTimes.some((slot) => parseHHMMToMinutes(slot && slot.time) !== null);
}

function skinsPopsUnlockAt(ev = {}) {
  return eventLocalDateTimeToUtc(ev && ev.date, '00:00', LOCAL_TZ);
}

function buildWeekendSkinsPopsDraw() {
  const crypto = require('crypto');
  const remaining = Array.from({ length: 17 }, (_, index) => index + 1);
  const pickUnique = (count) => {
    const picks = [];
    for (let i = 0; i < count && remaining.length; i += 1) {
      const index = crypto.randomInt(remaining.length);
      picks.push(remaining.splice(index, 1)[0]);
    }
    picks.sort((a, b) => a - b);
    return picks;
  };
  return {
    sharedHoles: pickUnique(4),
    bonusHoles: pickUnique(2),
    generatedAt: new Date(),
  };
}

function calendarDateParts(dateVal) {
  const d = asUTCDate(dateVal);
  if (Number.isNaN(d.getTime())) return null;
  return {
    year: d.getUTCFullYear(),
    month: d.getUTCMonth() + 1,
    day: d.getUTCDate(),
    iso: d.toISOString().slice(0, 10),
  };
}

function eventCalendarTiming(ev, durationMinutes = CALENDAR_EVENT_DURATION_MINUTES) {
  const parts = calendarDateParts(ev && ev.date);
  if (!parts) return null;

  let startMinutes = null;
  for (const tt of (ev && ev.teeTimes) || []) {
    const mins = parseHHMMToMinutes(tt && tt.time);
    if (mins === null) continue;
    if (startMinutes === null || mins < startMinutes) startMinutes = mins;
  }

  if (startMinutes === null) {
    const startDate = new Date(Date.UTC(parts.year, parts.month - 1, parts.day, 0, 0, 0));
    const endDate = new Date(startDate.getTime() + (24 * 60 * 60 * 1000));
    return { allDay: true, dateISO: parts.iso, startDate, endDate };
  }

  const start = new Date(Date.UTC(parts.year, parts.month - 1, parts.day, Math.floor(startMinutes / 60), startMinutes % 60, 0));
  const end = new Date(start.getTime() + (durationMinutes * 60 * 1000));
  return { allDay: false, dateISO: parts.iso, start, end };
}

function twoDigits(n) {
  return String(n).padStart(2, '0');
}

function formatIcsUtcStamp(date) {
  return `${date.getUTCFullYear()}${twoDigits(date.getUTCMonth() + 1)}${twoDigits(date.getUTCDate())}T${twoDigits(date.getUTCHours())}${twoDigits(date.getUTCMinutes())}${twoDigits(date.getUTCSeconds())}Z`;
}

function formatIcsFloatingDateTime(date) {
  return `${date.getUTCFullYear()}${twoDigits(date.getUTCMonth() + 1)}${twoDigits(date.getUTCDate())}T${twoDigits(date.getUTCHours())}${twoDigits(date.getUTCMinutes())}${twoDigits(date.getUTCSeconds())}`;
}

function formatIcsDateValue(date) {
  return `${date.getUTCFullYear()}${twoDigits(date.getUTCMonth() + 1)}${twoDigits(date.getUTCDate())}`;
}

function escapeIcsText(value = '') {
  return String(value)
    .replace(/\\/g, '\\\\')
    .replace(/\r?\n/g, '\\n')
    .replace(/,/g, '\\,')
    .replace(/;/g, '\\;');
}

function foldIcsLine(line) {
  const maxLen = 74;
  if (line.length <= maxLen) return line;
  const chunks = [];
  for (let i = 0; i < line.length; i += maxLen) {
    chunks.push(i === 0 ? line.slice(i, i + maxLen) : ` ${line.slice(i, i + maxLen)}`);
  }
  return chunks.join('\r\n');
}

function eventCalendarSummary(ev) {
  const mode = ev && ev.isTeamEvent ? `${groupedSlotNamePrefix(ev)} Event` : 'Tee-Time Event';
  const course = ev && ev.course ? String(ev.course).trim() : 'Golf Event';
  return `${course} (${mode})`;
}

function eventCalendarDescription(ev) {
  const lines = ['Tee Time Manager Event'];
  if (ev && ev.course) lines.push(`Course: ${String(ev.course).trim()}`);
  lines.push(`Date: ${fmt.dateLong(ev && ev.date) || fmt.dateISO(ev && ev.date)}`);
  const slotPrefix = groupedSlotNamePrefix(ev);

  const slotTimes = ((ev && ev.teeTimes) || [])
    .map((tt, idx) => {
      if (tt && tt.time) {
        if (ev && ev.isTeamEvent) return `${tt.name || `${slotPrefix} ${idx + 1}`}: ${fmt.tee(tt.time)}`;
        return `Tee ${idx + 1}: ${fmt.tee(tt.time)}`;
      }
      if (ev && ev.isTeamEvent) return tt && tt.name ? String(tt.name) : `${slotPrefix} ${idx + 1}`;
      return '';
    })
    .filter(Boolean);
  if (slotTimes.length) lines.push(`${ev && ev.isTeamEvent ? `${slotPrefix}s` : 'Tee Times'}: ${slotTimes.join(', ')}`);

  if (ev && ev.notes) lines.push(`Notes: ${String(ev.notes).trim()}`);
  if (ev && ev._id) lines.push(`Event Link: ${SITE_URL}?event=${String(ev._id)}`);
  return lines.join('\n');
}

function eventCalendarFileName(ev) {
  const dateISO = fmt.dateISO(ev && ev.date) || 'event';
  const courseSlug = String((ev && ev.course) || 'golf-event')
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '') || 'golf-event';
  return `tee-time-${dateISO}-${courseSlug}.ics`;
}

function buildIcsEventLines(ev, stampDate = new Date()) {
  const timing = eventCalendarTiming(ev);
  if (!timing) return null;

  const uid = `${(ev && ev._id) ? String(ev._id) : Date.now()}@tee-time-brs`;
  const summary = eventCalendarSummary(ev);
  const description = eventCalendarDescription(ev);
  const location = ev && ev.course ? String(ev.course).trim() : 'Golf Course';
  const url = `${SITE_URL}?event=${(ev && ev._id) ? String(ev._id) : ''}`;
  const alarms = ICS_REMINDER_MINUTES.flatMap((minutes) => ([
    'BEGIN:VALARM',
    `TRIGGER:-PT${minutes}M`,
    'ACTION:DISPLAY',
    `DESCRIPTION:${escapeIcsText(`Tee time reminder: ${summary}`)}`,
    'END:VALARM',
  ]));

  return [
    'BEGIN:VEVENT',
    `UID:${escapeIcsText(uid)}`,
    `DTSTAMP:${formatIcsUtcStamp(stampDate)}`,
    timing.allDay
      ? `DTSTART;VALUE=DATE:${formatIcsDateValue(timing.startDate)}`
      : `DTSTART:${formatIcsFloatingDateTime(timing.start)}`,
    timing.allDay
      ? `DTEND;VALUE=DATE:${formatIcsDateValue(timing.endDate)}`
      : `DTEND:${formatIcsFloatingDateTime(timing.end)}`,
    `SUMMARY:${escapeIcsText(summary)}`,
    `DESCRIPTION:${escapeIcsText(description)}`,
    `LOCATION:${escapeIcsText(location)}`,
    `URL:${escapeIcsText(url)}`,
    'STATUS:CONFIRMED',
    ...alarms,
    'END:VEVENT',
  ];
}

function buildEventIcs(ev) {
  const eventLines = buildIcsEventLines(ev);
  if (!eventLines) return null;
  const lines = [
    'BEGIN:VCALENDAR',
    'VERSION:2.0',
    'PRODID:-//Tee Time Manager//EN',
    'CALSCALE:GREGORIAN',
    'METHOD:PUBLISH',
    ...eventLines,
    'END:VCALENDAR',
  ];
  return `${lines.map(foldIcsLine).join('\r\n')}\r\n`;
}

function buildEventsIcs(events = [], opts = {}) {
  const calName = String(opts.calName || 'Tee Time Events').trim();
  const calDesc = String(opts.calDesc || 'Golf events from Tee Time Manager').trim();
  const stampDate = opts.stampDate instanceof Date ? opts.stampDate : new Date();
  const sorted = Array.isArray(events) ? events.slice() : [];
  sorted.sort((a, b) => {
    const ta = eventCalendarTiming(a);
    const tb = eventCalendarTiming(b);
    const aStamp = ta ? (ta.allDay ? ta.startDate.getTime() : ta.start.getTime()) : Number.MAX_SAFE_INTEGER;
    const bStamp = tb ? (tb.allDay ? tb.startDate.getTime() : tb.start.getTime()) : Number.MAX_SAFE_INTEGER;
    return aStamp - bStamp;
  });

  const lines = [
    'BEGIN:VCALENDAR',
    'VERSION:2.0',
    'PRODID:-//Tee Time Manager//EN',
    'CALSCALE:GREGORIAN',
    'METHOD:PUBLISH',
    `X-WR-CALNAME:${escapeIcsText(calName)}`,
    `X-WR-CALDESC:${escapeIcsText(calDesc)}`,
  ];
  for (const ev of sorted) {
    const eventLines = buildIcsEventLines(ev, stampDate);
    if (eventLines) lines.push(...eventLines);
  }
  lines.push(
    'END:VCALENDAR',
  );
  return `${lines.map(foldIcsLine).join('\r\n')}\r\n`;
}

function getScopedAdminCode(req) {
  return String(req.headers['x-admin-code'] || req.query.code || req.body?.code || '').trim();
}

function sanitizeGroupSlug(value = '') {
  const normalized = String(value || '')
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9-]+/g, '-')
    .replace(/-+/g, '-')
    .replace(/^-+|-+$/g, '');
  return normalized || DEFAULT_SITE_GROUP_SLUG;
}

function getGroupSlugVariants(groupSlug = DEFAULT_SITE_GROUP_SLUG) {
  const canonicalGroupSlug = normalizeGroupSlug(groupSlug);
  const variants = new Set([canonicalGroupSlug]);
  Object.entries(GROUP_SLUG_ALIASES).forEach(([legacySlug, targetSlug]) => {
    if (targetSlug === canonicalGroupSlug) variants.add(legacySlug);
  });
  return Array.from(variants);
}

function getAllowedSiteAdminCodes(groupSlug = DEFAULT_SITE_GROUP_SLUG) {
  const normalizedGroupSlug = normalizeGroupSlug(groupSlug);
  const allowedCodes = [];
  const storedConfig = getGroupAccessControlConfig(normalizedGroupSlug);
  const storedAdminCode = cleanAccessCode(storedConfig.adminCode || '');
  const overrideCode = cleanAccessCode(GROUP_SITE_ADMIN_CODE_OVERRIDES[normalizedGroupSlug] || '');
  if (normalizedGroupSlug === DEFAULT_SITE_GROUP_SLUG && SITE_ADMIN_WRITE_CODE) {
    allowedCodes.push(cleanAccessCode(SITE_ADMIN_WRITE_CODE));
  }
  if (storedAdminCode) allowedCodes.push(storedAdminCode);
  else if (overrideCode) allowedCodes.push(overrideCode);
  else if (SITE_ADMIN_WRITE_CODE) {
    // Backward-compatible fallback for older scoped groups created before dedicated group codes were stored.
    allowedCodes.push(cleanAccessCode(SITE_ADMIN_WRITE_CODE));
  }
  return Array.from(new Set(allowedCodes.filter(Boolean)));
}

function normalizeGroupSlug(value = '') {
  const sanitized = sanitizeGroupSlug(value);
  return GROUP_SLUG_ALIASES[sanitized] || sanitized || DEFAULT_SITE_GROUP_SLUG;
}

function getGroupSlug(req, fallback = DEFAULT_SITE_GROUP_SLUG) {
  return normalizeGroupSlug(
    req.headers['x-site-group']
      || req.query.group
      || req.body?.group
      || req.body?.groupSlug
      || fallback
  );
}

function groupScopeFilter(groupSlug = DEFAULT_SITE_GROUP_SLUG, fieldName = 'groupSlug') {
  const normalizedGroupSlug = normalizeGroupSlug(groupSlug);
  if (normalizedGroupSlug === DEFAULT_SITE_GROUP_SLUG) {
    return {
      $or: [
        { [fieldName]: normalizedGroupSlug },
        { [fieldName]: { $exists: false } },
        { [fieldName]: null },
        { [fieldName]: '' },
      ],
    };
  }
  const variants = getGroupSlugVariants(normalizedGroupSlug);
  if (variants.length === 1) return { [fieldName]: normalizedGroupSlug };
  return { [fieldName]: { $in: variants } };
}

function scopeQuery(req, extra = {}) {
  return { ...groupScopeFilter(getGroupSlug(req)), ...extra };
}

function scopedSettingQuery(groupSlug, key) {
  return { ...groupScopeFilter(groupSlug), key: String(key || '').trim() };
}

async function getAllManagedGroupSlugs() {
  const groups = new Set([DEFAULT_SITE_GROUP_SLUG]);
  const loaders = [];
  if (Event) loaders.push(Event.distinct('groupSlug'));
  if (Subscriber) loaders.push(Subscriber.distinct('groupSlug'));
  if (Settings) loaders.push(Settings.distinct('groupSlug'));
  const results = await Promise.allSettled(loaders);
  for (const result of results) {
    if (result.status !== 'fulfilled' || !Array.isArray(result.value)) continue;
    for (const rawGroup of result.value) {
      groups.add(normalizeGroupSlug(rawGroup));
    }
  }
  return Array.from(groups);
}

async function findScopedEventById(req, eventId, options = {}) {
  const query = Event.findOne({ ...groupScopeFilter(getGroupSlug(req)), _id: eventId });
  if (options.lean) query.lean();
  return query;
}

function buildGroupAwarePath(pathname = '', groupSlug = DEFAULT_SITE_GROUP_SLUG) {
  const slug = normalizeGroupSlug(groupSlug);
  if (slug === DEFAULT_SITE_GROUP_SLUG) return `${pathname}`;
  const sep = pathname.includes('?') ? '&' : '?';
  return `${pathname}${sep}group=${encodeURIComponent(slug)}`;
}

function titleFromGroupSlug(groupSlug = DEFAULT_SITE_GROUP_SLUG) {
  return normalizeGroupSlug(groupSlug)
    .split('-')
    .filter(Boolean)
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join(' ') || 'Tee Times';
}

function isBlueRidgeShadowsCourseName(value = '') {
  const normalized = String(value || '').trim().toLowerCase();
  if (!normalized) return false;
  return normalized.includes('blue ridge shadows');
}

function resolveGroupReference(groupSlug = DEFAULT_SITE_GROUP_SLUG, profile = {}) {
  const normalizedGroupSlug = normalizeGroupSlug(groupSlug || profile.groupSlug || DEFAULT_SITE_GROUP_SLUG);
  const explicit = String(profile.groupReference || '').trim();
  if (explicit) return explicit;
  if (GROUP_REFERENCE_OVERRIDES[normalizedGroupSlug]) return GROUP_REFERENCE_OVERRIDES[normalizedGroupSlug];
  return String(profile.groupName || profile.siteTitle || titleFromGroupSlug(normalizedGroupSlug)).trim()
    || titleFromGroupSlug(normalizedGroupSlug);
}

function uniqueEmailList(values = []) {
  const flattened = Array.isArray(values) ? values.flat() : [values];
  return Array.from(new Set(flattened
    .map((value) => String(value || '').trim().toLowerCase())
    .filter(Boolean)));
}

function cleanAccessCode(value = '') {
  return String(value || '').trim().replace(/\s+/g, '');
}

function normalizeStoredGroupAccessConfig(groupSlug = DEFAULT_SITE_GROUP_SLUG, profile = {}) {
  const normalizedGroupSlug = normalizeGroupSlug(groupSlug || profile.groupSlug || DEFAULT_SITE_GROUP_SLUG);
  const overrideCode = cleanAccessCode(GROUP_SITE_ADMIN_CODE_OVERRIDES[normalizedGroupSlug] || '');
  const adminCode = cleanAccessCode(profile.adminCode || '') || overrideCode;
  const deleteCode = cleanAccessCode(profile.deleteCode || '') || adminCode;
  const confirmCode = cleanAccessCode(profile.confirmCode || '');
  return { adminCode, deleteCode, confirmCode };
}

function setGroupAccessControlCache(groupSlug = DEFAULT_SITE_GROUP_SLUG, profile = {}) {
  const normalizedGroupSlug = normalizeGroupSlug(groupSlug || profile.groupSlug || DEFAULT_SITE_GROUP_SLUG);
  const config = normalizeStoredGroupAccessConfig(normalizedGroupSlug, profile);
  groupAccessControlCache.set(normalizedGroupSlug, config);
  return config;
}

function getGroupAccessControlConfig(groupSlug = DEFAULT_SITE_GROUP_SLUG) {
  const normalizedGroupSlug = normalizeGroupSlug(groupSlug);
  if (!groupAccessControlCache.has(normalizedGroupSlug)) {
    return setGroupAccessControlCache(normalizedGroupSlug, {});
  }
  return groupAccessControlCache.get(normalizedGroupSlug) || { adminCode: '', deleteCode: '', confirmCode: '' };
}

function applyGroupProfileIsolation(groupSlug = DEFAULT_SITE_GROUP_SLUG, profile = {}) {
  const normalizedGroupSlug = normalizeGroupSlug(groupSlug || profile.groupSlug || DEFAULT_SITE_GROUP_SLUG);
  const override = GROUP_PROFILE_ISOLATION_OVERRIDES[normalizedGroupSlug];
  const accessConfig = normalizeStoredGroupAccessConfig(normalizedGroupSlug, profile);
  const defaultInboundAlias = normalizedGroupSlug === DEFAULT_SITE_GROUP_SLUG
    ? RESEND_INBOUND_BASE_ADDRESS
    : `teetime+${normalizedGroupSlug}@${RESEND_INBOUND_BASE_ADDRESS.split('@')[1]}`;
  const inboundEmailAlias = String(profile.inboundEmailAlias || defaultInboundAlias).trim().toLowerCase();
  if (!override) {
    return {
      ...(profile && typeof profile === 'object' ? profile : {}),
      groupSlug: normalizedGroupSlug,
      adminCode: accessConfig.adminCode,
      deleteCode: accessConfig.deleteCode,
      confirmCode: accessConfig.confirmCode,
      inboundEmailAlias,
    };
  }
  return {
    ...(profile && typeof profile === 'object' ? profile : {}),
    ...override,
    groupSlug: normalizedGroupSlug,
    adminCode: accessConfig.adminCode,
    deleteCode: accessConfig.deleteCode,
    confirmCode: accessConfig.confirmCode,
    inboundEmailAlias,
    features: {
      ...(((profile && typeof profile === 'object' && profile.features) || {})),
      ...((override && override.features) || {}),
    },
  };
}

function buildDefaultSiteProfileInput(groupSlug = DEFAULT_SITE_GROUP_SLUG) {
  const normalizedGroupSlug = normalizeGroupSlug(groupSlug);
  const adminEmails = uniqueEmailList(ADMIN_EMAILS);
  const defaultLabel = titleFromGroupSlug(normalizedGroupSlug);
  const isMainGroup = normalizedGroupSlug === DEFAULT_SITE_GROUP_SLUG;
  const siteTitle = isMainGroup ? 'Tee Times' : `${defaultLabel} Tee Times`;
  const clubName = isMainGroup ? 'Blue Ridge Shadows' : defaultLabel;
  const groupName = isMainGroup ? 'Knight Group Tee Times' : defaultLabel;
  return applyGroupProfileIsolation(normalizedGroupSlug, {
    groupSlug: normalizedGroupSlug,
    packageSlug: normalizedGroupSlug,
    siteTitle,
    shortTitle: isMainGroup ? 'Tee Time' : defaultLabel,
    groupName,
    groupReference: resolveGroupReference(normalizedGroupSlug, { groupName, siteTitle }),
    clubName,
    clubRequestLabel: isMainGroup ? 'Request a Tee Time for Blue Ridge Shadows' : `Request a Tee Time for ${clubName}`,
    primaryContactEmail: adminEmails[0] || '',
    secondaryContactEmail: adminEmails[1] || '',
    clubRequestEmail: CLUB_EMAIL,
    replyToEmail: adminEmails[0] || CLUB_EMAIL,
    supportPhone: '',
    clubPhone: '',
    smsPhone: '',
    adminAlertPhones: [],
    adminCode: '',
    deleteCode: '',
    confirmCode: '',
    inboundEmailAlias: isMainGroup ? RESEND_INBOUND_BASE_ADDRESS : `teetime+${normalizedGroupSlug}@${RESEND_INBOUND_BASE_ADDRESS.split('@')[1]}`,
    themeColor: '#173224',
    iconAssetName: normalizedGroupSlug === 'seniors' ? 'seniors.png' : 'brs-tee-manager-logo.png',
    notes: isMainGroup ? 'Main Tee Times site profile.' : '',
    features: {
      includeHandicaps: isMainGroup,
      includeTrips: isMainGroup,
      includeOutings: isMainGroup,
      includeNotifications: true,
      includeScheduler: true,
      includeBackups: isMainGroup,
    },
  });
}

function resolveSiteIconPath(iconAssetName = '') {
  const assetName = String(iconAssetName || '').trim();
  if (!assetName) return '/icons/icon-512.png';
  if (assetName === 'knight-club-icon.png' || assetName === '/assets/knight-club-icon.png') {
    return '/assets/brs-tee-manager-logo.png';
  }
  if (assetName.startsWith('/')) return assetName;
  return `/assets/${assetName}`;
}

function buildAbsoluteSiteUrl(pathname = '/') {
  return new URL(String(pathname || '/'), SITE_URL).toString();
}

function buildGroupDeploymentLinks(groupSlug = DEFAULT_SITE_GROUP_SLUG) {
  const normalizedGroupSlug = normalizeGroupSlug(groupSlug);
  const routePaths = buildGroupRoutePaths(normalizedGroupSlug);
  const hasLiteAdmin = normalizedGroupSlug !== DEFAULT_SITE_GROUP_SLUG;
  return {
    groupSlug: normalizedGroupSlug,
    sitePath: routePaths.site,
    siteUrl: buildAbsoluteSiteUrl(routePaths.site),
    siteQueryPath: buildGroupAwarePath('/', normalizedGroupSlug),
    adminPath: routePaths.admin,
    adminUrl: buildAbsoluteSiteUrl(routePaths.admin),
    adminQueryPath: hasLiteAdmin ? '' : buildGroupAwarePath('/admin.html', normalizedGroupSlug),
    adminLitePath: hasLiteAdmin ? routePaths.adminLite : '',
    adminLiteUrl: hasLiteAdmin ? buildAbsoluteSiteUrl(routePaths.adminLite) : '',
    adminLiteQueryPath: hasLiteAdmin ? buildGroupAwarePath('/group-admin-lite.html', normalizedGroupSlug) : '',
    calendarPath: routePaths.calendar,
    calendarUrl: buildAbsoluteSiteUrl(routePaths.calendar),
    manifestPath: `/manifest.json?group=${encodeURIComponent(normalizedGroupSlug)}`,
    manifestUrl: buildAbsoluteSiteUrl(`/manifest.json?group=${encodeURIComponent(normalizedGroupSlug)}`),
  };
}

function toPublicSiteProfile(profile = {}) {
  const normalizedGroupSlug = normalizeGroupSlug(profile.groupSlug || DEFAULT_SITE_GROUP_SLUG);
  const isolatedProfile = applyGroupProfileIsolation(normalizedGroupSlug, profile);
  const routePaths = buildGroupRoutePaths(normalizedGroupSlug);
  return {
    groupSlug: normalizedGroupSlug,
    siteTitle: String(isolatedProfile.siteTitle || 'Tee Times').trim() || 'Tee Times',
    shortTitle: String(isolatedProfile.shortTitle || isolatedProfile.siteTitle || 'Tee Time').trim() || 'Tee Time',
    groupName: String(isolatedProfile.groupName || '').trim(),
    groupReference: resolveGroupReference(normalizedGroupSlug, isolatedProfile),
    clubName: String(isolatedProfile.clubName || '').trim(),
    clubRequestLabel: String(isolatedProfile.clubRequestLabel || '').trim(),
    themeColor: String(isolatedProfile.themeColor || '#173224').trim() || '#173224',
    iconAssetName: String(isolatedProfile.iconAssetName || '').trim(),
    iconPath: resolveSiteIconPath(isolatedProfile.iconAssetName),
    features: isolatedProfile.features && typeof isolatedProfile.features === 'object' ? isolatedProfile.features : buildDefaultSiteProfileInput(normalizedGroupSlug).features,
    routePaths,
    links: buildGroupDeploymentLinks(normalizedGroupSlug),
  };
}

function toAdminEditableSiteProfile(profile = {}) {
  const isolatedProfile = applyGroupProfileIsolation(profile.groupSlug || DEFAULT_SITE_GROUP_SLUG, profile);
  const safeProfile = { ...isolatedProfile };
  delete safeProfile.adminCode;
  delete safeProfile.deleteCode;
  delete safeProfile.confirmCode;
  return safeProfile;
}

async function getSiteProfile(groupSlug = DEFAULT_SITE_GROUP_SLUG) {
  const normalizedGroupSlug = normalizeGroupSlug(groupSlug);
  const defaults = buildDefaultSiteProfileInput(normalizedGroupSlug);
  if (!Settings) {
    return buildTeeTimesSiteDeploymentProfile(defaults, { preserveBlankAccessCodes: true });
  }
  try {
    const setting = await Settings.findOne(scopedSettingQuery(normalizedGroupSlug, 'siteProfile')).lean();
    const storedValue = setting && setting.value && typeof setting.value === 'object' ? setting.value : {};
    return buildTeeTimesSiteDeploymentProfile({
      ...applyGroupProfileIsolation(normalizedGroupSlug, {
        ...defaults,
        ...storedValue,
      }),
      features: {
        ...(defaults.features || {}),
        ...((storedValue && storedValue.features) || {}),
        ...(((GROUP_PROFILE_ISOLATION_OVERRIDES[normalizedGroupSlug] || {}).features) || {}),
      },
      groupSlug: normalizedGroupSlug,
      packageSlug: storedValue.packageSlug || normalizedGroupSlug,
    }, { preserveBlankAccessCodes: true });
  } catch (error) {
    console.error('Error loading site profile:', error);
    return buildTeeTimesSiteDeploymentProfile(defaults, { preserveBlankAccessCodes: true });
  }
}

async function isManagedGroupSlug(groupSlug = DEFAULT_SITE_GROUP_SLUG) {
  const normalizedGroupSlug = normalizeGroupSlug(groupSlug);
  if (normalizedGroupSlug === DEFAULT_SITE_GROUP_SLUG) return true;
  const managedGroups = await getAllManagedGroupSlugs();
  return managedGroups.includes(normalizedGroupSlug);
}

async function getSubscriptionGroupContext(groupSlug = DEFAULT_SITE_GROUP_SLUG) {
  const normalizedGroupSlug = normalizeGroupSlug(groupSlug);
  try {
    const profile = await getSiteProfile(normalizedGroupSlug);
    return {
      groupSlug: normalizedGroupSlug,
      groupReference: resolveGroupReference(normalizedGroupSlug, profile),
      siteTitle: String(profile.siteTitle || 'Tee Times').trim() || 'Tee Times',
    };
  } catch (_) {
    return {
      groupSlug: normalizedGroupSlug,
      groupReference: resolveGroupReference(normalizedGroupSlug, {}),
      siteTitle: 'Tee Times',
    };
  }
}

async function saveSiteProfile(groupSlug = DEFAULT_SITE_GROUP_SLUG, input = {}) {
  if (!Settings) throw new Error('Settings model not available');
  const normalizedGroupSlug = normalizeGroupSlug(groupSlug);
  const currentProfile = await getSiteProfile(normalizedGroupSlug);
  const nextProfile = buildTeeTimesSiteDeploymentProfile(applyGroupProfileIsolation(normalizedGroupSlug, {
    ...currentProfile,
    ...input,
    features: {
      ...(currentProfile.features || {}),
      ...((input && input.features) || {}),
    },
    groupSlug: normalizedGroupSlug,
    packageSlug: input && input.packageSlug ? input.packageSlug : (currentProfile.packageSlug || normalizedGroupSlug),
  }), { preserveBlankAccessCodes: true });

  await Settings.findOneAndUpdate(
    scopedSettingQuery(normalizedGroupSlug, 'siteProfile'),
    { groupSlug: normalizedGroupSlug, key: 'siteProfile', value: nextProfile },
    { upsert: true, new: true, setDefaultsOnInsert: true }
  );
  setGroupAccessControlCache(normalizedGroupSlug, nextProfile);

  const notificationsEnabled = nextProfile.features.includeNotifications !== false;
  const schedulerEnabled = nextProfile.features.includeScheduler !== false;

  await Settings.findOneAndUpdate(
    scopedSettingQuery(normalizedGroupSlug, 'notificationsEnabled'),
    { groupSlug: normalizedGroupSlug, key: 'notificationsEnabled', value: notificationsEnabled },
    { upsert: true, new: true, setDefaultsOnInsert: true }
  );
  await Settings.findOneAndUpdate(
    scopedSettingQuery(normalizedGroupSlug, 'schedulerEnabled'),
    { groupSlug: normalizedGroupSlug, key: 'schedulerEnabled', value: schedulerEnabled },
    { upsert: true, new: true, setDefaultsOnInsert: true }
  );
  schedulerEnabledCache.set(normalizedGroupSlug, { value: schedulerEnabled, ts: Date.now() });

  return nextProfile;
}

async function getGroupContactTargets(groupSlug = DEFAULT_SITE_GROUP_SLUG) {
  const profile = await getSiteProfile(groupSlug);
  const normalizedGroupSlug = normalizeGroupSlug(groupSlug);
  const override = GROUP_CONTACT_TARGET_OVERRIDES[normalizedGroupSlug] || {};
  const adminEmails = uniqueEmailList([
    profile.primaryContactEmail,
    profile.secondaryContactEmail,
    ADMIN_EMAILS,
  ]);
  const clubCcEmails = uniqueEmailList([
    profile.primaryContactEmail,
    profile.secondaryContactEmail,
    String(process.env.CLUB_CANCEL_CC || '').split(','),
  ]);
  return {
    profile,
    groupLabel: String(profile.groupName || profile.siteTitle || 'Tee Times').trim() || 'Tee Times',
    clubLabel: String(override.clubLabel || profile.clubName || 'the club').trim() || 'the club',
    clubEmail: String(override.clubEmail || profile.clubRequestEmail || CLUB_EMAIL).trim() || CLUB_EMAIL,
    adminEmails,
    clubCcEmails,
  };
}

function clubCancelCcRecipientsForEvent(eventLike = {}, baseCcEmails = []) {
  const normalizedGroupSlug = normalizeGroupSlug(eventLike && eventLike.groupSlug);
  const safeBaseList = uniqueEmailList(baseCcEmails);
  if (normalizedGroupSlug !== DEFAULT_SITE_GROUP_SLUG) return safeBaseList;
  if (!isBlueRidgeShadowsCourseName(eventLike && eventLike.course)) return safeBaseList;
  return uniqueEmailList([safeBaseList, BRS_TEE_RETURN_CC_EMAILS]);
}

async function buildOperationsGuidePayload() {
  const groupSlugs = await getAllManagedGroupSlugs();
  const groups = [];
  for (const groupSlug of groupSlugs) {
    const normalizedGroupSlug = normalizeGroupSlug(groupSlug);
    const profile = await getSiteProfile(normalizedGroupSlug);
    const links = buildGroupDeploymentLinks(normalizedGroupSlug);
    const contacts = await getGroupContactTargets(normalizedGroupSlug);
    groups.push({
      groupSlug: normalizedGroupSlug,
      groupReference: resolveGroupReference(normalizedGroupSlug, profile),
      siteTitle: String(profile.siteTitle || '').trim(),
      groupName: String(profile.groupName || '').trim(),
      siteUrl: buildAbsoluteSiteUrl(links.sitePath),
      adminUrl: buildAbsoluteSiteUrl(links.adminPath),
      calendarUrl: buildAbsoluteSiteUrl(links.calendarPath),
      primaryContactEmail: String(profile.primaryContactEmail || '').trim(),
      secondaryContactEmail: String(profile.secondaryContactEmail || '').trim(),
      clubRequestEmail: String(profile.clubRequestEmail || '').trim(),
      replyToEmail: String(profile.replyToEmail || '').trim(),
      inboundEmailAlias: String(profile.inboundEmailAlias || '').trim(),
      clubCcEmails: contacts.clubCcEmails,
    });
  }
  groups.sort((left, right) => {
    if (left.groupSlug === DEFAULT_SITE_GROUP_SLUG && right.groupSlug !== DEFAULT_SITE_GROUP_SLUG) return -1;
    if (left.groupSlug !== DEFAULT_SITE_GROUP_SLUG && right.groupSlug === DEFAULT_SITE_GROUP_SLUG) return 1;
    return String(left.groupReference || '').localeCompare(String(right.groupReference || ''));
  });
  const defaultInboundGroup = groups.find((group) => group.groupSlug === DEFAULT_SITE_GROUP_SLUG) || null;

  const resendInboundAddress = RESEND_INBOUND_BASE_ADDRESS;
  const resendAllowedSenders = ['tommy.knight@gmail.com', 'no-reply@foreupsoftware.com'];
  const allEmails = uniqueEmailList([
    ADMIN_EMAILS,
    CLUB_EMAIL,
    String(process.env.RESEND_FROM || '').trim(),
    resendInboundAddress,
    resendAllowedSenders,
    String(process.env.CLUB_CANCEL_CC || '').split(','),
    ...groups.flatMap((group) => [
      group.primaryContactEmail,
      group.secondaryContactEmail,
      group.clubRequestEmail,
      group.replyToEmail,
      group.inboundEmailAlias,
      group.clubCcEmails,
    ]),
  ]);

  return {
    generatedAt: new Date().toISOString(),
    hosting: {
      platform: 'Render',
      publicSiteUrl: SITE_URL,
      dashboardUrl: 'https://dashboard.render.com/',
      serviceNote: 'The live tee-time site is hosted on Render and the canonical public URL comes from SITE_URL.',
    },
    emailDelivery: {
      provider: 'Resend',
      dashboardUrl: 'https://resend.com/',
      apiUrl: 'https://api.resend.com/emails',
      smtpHost: 'smtp.resend.com',
      fromAddress: String(process.env.RESEND_FROM || '').trim(),
      inboundAddress: resendInboundAddress,
      webhookUrl: buildAbsoluteSiteUrl('/webhooks/resend'),
      allowedInboundSenders: resendAllowedSenders,
    },
    inboundRouting: {
      defaultGroupSlug: DEFAULT_SITE_GROUP_SLUG,
      defaultGroupReference: defaultInboundGroup ? defaultInboundGroup.groupReference : resolveGroupReference(DEFAULT_SITE_GROUP_SLUG, {}),
      defaultGroupName: defaultInboundGroup ? defaultInboundGroup.groupName : '',
      recipientAliasPattern: `${resendInboundAddress.replace('@', '+<group>@')}`,
      subjectTagPattern: '[group:<slug>]',
      explanation: 'Imported tee-time emails can now be routed to a specific group by sending them to a +group alias on the inbound Resend address, or by including a [group:<slug>] tag in the subject. If neither marker is present, the Render-hosted /webhooks/resend route still falls back to the default site group, so untagged emails are created under the main BRS tee-times group.',
    },
    adminEmails: ADMIN_EMAILS,
    defaultClubEmail: CLUB_EMAIL,
    clubCancelCcEmails: uniqueEmailList(String(process.env.CLUB_CANCEL_CC || '').split(',')),
    allOperationalEmails: allEmails,
    groups,
    automation: {
      timeZone: LOCAL_TZ,
      schedulerEnabledByEnv: !SCHEDULER_ENV_DISABLED,
      scheduledRules: [
        { key: 'brianTomorrowEmptyClubAlert', schedule: '4:00 PM daily', audience: 'club/admin routing email', summary: 'Sends tomorrow empty-tee alerts to the club routing address.' },
        { key: 'reminder48Hour', schedule: '5:00 PM daily', audience: 'subscribers', summary: 'Sends 48-hour empty-tee reminders to subscribers.' },
        { key: 'reminder24Hour', schedule: '5:00 PM daily', audience: 'subscribers', summary: 'Sends 24-hour empty-tee reminders to subscribers.' },
        { key: 'nearlyFullTeeTimes', schedule: '5:00 PM daily', audience: 'subscribers', summary: 'Sends nearly-full tee-time alerts when an event is more than 50% full within 4 days.' },
        { key: 'adminEmptyTeeAlerts', schedule: 'Every 6 hours at 12:00 AM, 6:00 AM, 12:00 PM, and 6:00 PM', audience: 'admin alert recipients', summary: 'Sends grouped admin empty-tee alerts.' },
      ],
    },
  };
}

function inferGroupSlugFromReferrer(req, fallback = DEFAULT_SITE_GROUP_SLUG) {
  try {
    const referrer = String(req.get('referer') || req.get('referrer') || '').trim();
    if (!referrer) return normalizeGroupSlug(fallback);
    const refUrl = new URL(referrer);
    const queryGroup = String(refUrl.searchParams.get('group') || '').trim();
    if (queryGroup) return normalizeGroupSlug(queryGroup);
    const match = refUrl.pathname.match(/^\/groups\/([^/]+)/i);
    if (match && match[1]) return normalizeGroupSlug(match[1]);
  } catch (_) {}
  return normalizeGroupSlug(fallback);
}

function buildRedirectWithGroupPath(pathname = '/', groupSlug = DEFAULT_SITE_GROUP_SLUG, query = {}) {
  const target = new URL(buildAbsoluteSiteUrl(buildGroupAwarePath(pathname, groupSlug)));
  Object.entries(query || {}).forEach(([key, rawValue]) => {
    if (key === 'group') return;
    const value = Array.isArray(rawValue) ? rawValue[rawValue.length - 1] : rawValue;
    if (value === undefined || value === null || value === '') return;
    target.searchParams.set(key, String(value));
  });
  return `${target.pathname}${target.search}${target.hash}`;
}

function getDestructiveAdminCode(req) {
  return String(
    req.headers['x-admin-delete-code']
      || req.query.deleteCode
      || req.body?.deleteCode
      || getScopedAdminCode(req)
      || ''
  ).trim();
}

function getDestructiveConfirmCode(req) {
  return String(
    req.headers['x-admin-confirm-code']
      || req.query.confirmCode
      || req.body?.confirmCode
      || ''
  ).trim();
}

function isSiteAdmin(req) {
  const code = getScopedAdminCode(req);
  return getAllowedSiteAdminCodes(getGroupSlug(req)).includes(code);
}

function isSiteAdminCode(code = '', groupSlug = DEFAULT_SITE_GROUP_SLUG) {
  return getAllowedSiteAdminCodes(groupSlug).includes(String(code || '').trim());
}

function isMainSiteAdminRequest(req) {
  return isSiteAdminCode(getScopedAdminCode(req), DEFAULT_SITE_GROUP_SLUG);
}

function getAllowedDestructiveAdminCodes(groupSlug = DEFAULT_SITE_GROUP_SLUG) {
  const normalizedGroupSlug = normalizeGroupSlug(groupSlug);
  const storedConfig = getGroupAccessControlConfig(normalizedGroupSlug);
  const storedDeleteCode = cleanAccessCode(storedConfig.deleteCode || '');
  if (normalizedGroupSlug === DEFAULT_SITE_GROUP_SLUG) {
    return ADMIN_DESTRUCTIVE_CODE ? [cleanAccessCode(ADMIN_DESTRUCTIVE_CODE)] : [];
  }
  if (storedDeleteCode) return [storedDeleteCode];
  return getAllowedSiteAdminCodes(normalizedGroupSlug);
}

function isAdminDeleteCode(code = '', groupSlug = DEFAULT_SITE_GROUP_SLUG) {
  return getAllowedDestructiveAdminCodes(groupSlug).includes(String(code || '').trim());
}

function isAdminDelete(req) {
  const code = getDestructiveAdminCode(req);
  return isAdminDeleteCode(code, getGroupSlug(req));
}

function hasDeleteActionConfirmed(req) {
  const raw = String(
    req.headers['x-delete-confirmed']
      || req.query.confirmed
      || req.body?.confirmed
      || ''
  ).trim().toLowerCase();
  return raw === '1' || raw === 'true' || raw === 'yes' || raw === 'on';
}

function hasDestructiveConfirmForGroup(req, groupSlug = DEFAULT_SITE_GROUP_SLUG) {
  const normalizedGroupSlug = normalizeGroupSlug(groupSlug);
  const storedConfig = getGroupAccessControlConfig(normalizedGroupSlug);
  const requiredCode = normalizedGroupSlug === DEFAULT_SITE_GROUP_SLUG
    ? cleanAccessCode(ADMIN_DESTRUCTIVE_CONFIRM_CODE)
    : cleanAccessCode(storedConfig.confirmCode || '');
  if (!requiredCode) return true;
  return getDestructiveConfirmCode(req) === requiredCode;
}

function requireSeniorsSiteAdminForWrite(req, res) {
  const groupSlug = getGroupSlug(req);
  if (normalizeGroupSlug(groupSlug) !== 'seniors') return false;
  if (isSiteAdmin(req) || isSeniorsAdminViewRequest(req)) return false;
  res.status(403).json({ error: 'Admin code 000 required for Seniors changes' });
  return true;
}

function isSeniorsAdminViewRequest(req) {
  if (normalizeGroupSlug(getGroupSlug(req)) !== 'seniors') return false;
  const referer = String(req.get('referer') || req.get('referrer') || '').trim();
  let refererFlag = '';
  if (referer) {
    try {
      const refererUrl = new URL(referer, APP_ORIGIN || 'http://localhost');
      refererFlag = String(
        refererUrl.searchParams.get('admin_view')
          || (/\/group-admin-lite\.html$/i.test(refererUrl.pathname) && String(refererUrl.searchParams.get('group') || '').trim().toLowerCase() === 'seniors' ? '1' : '')
      ).trim().toLowerCase();
    } catch (_) {}
  }
  const adminViewFlag = String(
    req.query?.admin_view
      || req.body?.admin_view
      || req.get('x-seniors-admin-view')
      || refererFlag
      || ''
  ).trim().toLowerCase();
  return adminViewFlag === '1' || adminViewFlag === 'true';
}

function requireSeniorsGroupAdmin(req, res) {
  if (normalizeGroupSlug(getGroupSlug(req)) !== 'seniors') {
    res.status(404).json({ error: 'Seniors roster is only available for the Seniors group' });
    return true;
  }
  if (!isSiteAdmin(req)) {
    res.status(403).json({ error: 'Admin code required' });
    return true;
  }
  return false;
}

function normalizeSeniorsGolferName(value = '') {
  return String(value || '').trim().replace(/\s+/g, ' ');
}

function normalizeOptionalRosterNumber(value) {
  if (value === '' || value === null || value === undefined) return null;
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) throw new Error('rosterNumber must be a number');
  return parsed;
}

function normalizeOptionalHandicap(value, fieldName = 'handicapIndex') {
  if (value === '' || value === null || value === undefined) return null;
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) throw new Error(`${fieldName} must be a number`);
  return parsed;
}

function normalizeSeniorsGolferInput(raw = {}) {
  const name = normalizeSeniorsGolferName(raw.name);
  if (!name) throw new Error('name required');

  const handicapGold = normalizeOptionalHandicap(raw.handicapGold, 'handicapGold');
  const handicapRed = normalizeOptionalHandicap(raw.handicapRed, 'handicapRed');
  const handicapIndex = raw.handicapIndex === '' || raw.handicapIndex === null || raw.handicapIndex === undefined
    ? (handicapGold ?? handicapRed)
    : normalizeOptionalHandicap(raw.handicapIndex, 'handicapIndex');

  const payload = {
    rosterNumber: normalizeOptionalRosterNumber(raw.rosterNumber),
    name,
    firstName: String(raw.firstName || '').trim(),
    lastName: String(raw.lastName || '').trim(),
    preferredFirstName: String(raw.preferredFirstName || '').trim(),
    preferredLastName: String(raw.preferredLastName || '').trim(),
    email: String(raw.email || '').trim().toLowerCase(),
    phone: String(raw.phone || '').trim(),
    address: String(raw.address || '').trim(),
    ghinNumber: String(raw.ghinNumber || '').trim(),
    handicapGold,
    handicapRed,
    handicapIndex,
    notes: String(raw.notes || '').trim(),
    active: raw.active === undefined ? true : !!raw.active,
  };

  return payload;
}

function normalizeSeniorsEventType(value = '') {
  const raw = String(value || '').trim().toLowerCase();
  const allowed = new Set(['regular-shotgun', 'interclub-match', 'outing', 'tee-times']);
  if (allowed.has(raw)) return raw;
  return '';
}

function seniorsEventTypeLabel(value = '') {
  switch (normalizeSeniorsEventType(value)) {
    case 'regular-shotgun': return 'Regular Shotgun';
    case 'interclub-match': return 'Interclub Match';
    case 'outing': return 'Other Outing';
    case 'tee-times': return 'Tee Times';
    default: return 'Golf Event';
  }
}

function normalizeSeniorsRegistrationMode(value = '', groupSlug = DEFAULT_SITE_GROUP_SLUG) {
  if (normalizeGroupSlug(groupSlug) !== 'seniors') return '';
  const raw = String(value || '').trim().toLowerCase();
  if (raw === 'event-only' || raw === 'tee-times') return raw;
  return '';
}

function isSeniorsEventOnlyEvent(ev = {}) {
  return normalizeGroupSlug(ev && ev.groupSlug) === 'seniors'
    && normalizeSeniorsRegistrationMode(ev && ev.seniorsRegistrationMode, ev && ev.groupSlug) === 'event-only';
}

function isSeniorsGroupedSlotEvent(ev = {}) {
  return normalizeGroupSlug(ev && ev.groupSlug) === 'seniors'
    && normalizeSeniorsEventType(ev && ev.seniorsEventType) === 'regular-shotgun'
    && !!(ev && ev.isTeamEvent);
}

function groupedSlotNamePrefix(ev = {}) {
  return isSeniorsGroupedSlotEvent(ev) ? 'Group' : 'Team';
}

function isUpcomingOrCurrentEventDate(dateValue) {
  const date = asUTCDate(dateValue);
  if (Number.isNaN(date.getTime())) return false;
  const now = new Date();
  const today = Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate());
  const eventDay = Date.UTC(date.getUTCFullYear(), date.getUTCMonth(), date.getUTCDate());
  return eventDay >= today;
}

async function getActiveSeniorsRosterMap() {
  const map = new Map();
  if (!SeniorsGolfer) return map;
  const golfers = await SeniorsGolfer.find({ groupSlug: 'seniors', active: true }).sort({ nameKey: 1 }).lean();
  golfers.forEach((golfer) => {
    const key = normalizeSeniorsGolferName(golfer && golfer.name).toLowerCase();
    if (key && !map.has(key)) map.set(key, golfer);
  });
  return map;
}

async function resolveSeniorsRosterGolferByName(name = '') {
  if (!SeniorsGolfer) return null;
  const normalizedName = normalizeSeniorsGolferName(name).toLowerCase();
  if (!normalizedName) return null;
  return SeniorsGolfer.findOne({ groupSlug: 'seniors', nameKey: normalizedName, active: true }).lean();
}

function buildManualSeniorsGolferRecord(name = '') {
  const trimmedName = normalizeSeniorsGolferName(name);
  if (!trimmedName) return null;
  return {
    _id: null,
    rosterNumber: null,
    name: trimmedName,
    firstName: '',
    lastName: '',
    preferredFirstName: '',
    preferredLastName: '',
    email: '',
    phone: '',
    address: '',
    ghinNumber: '',
    handicapGold: null,
    handicapRed: null,
    handicapIndex: null,
  };
}

async function buildSeniorsParticipantRows(ev, options = {}) {
  const participants = [];
  if (!ev || normalizeGroupSlug(ev.groupSlug) !== 'seniors') return participants;
  const includeContact = !!options.includeContact;
  if (isSeniorsEventOnlyEvent(ev)) {
    for (const registration of (ev.seniorsRegistrations || [])) {
      participants.push({
        source: 'registration',
        id: String(registration && registration._id || ''),
        name: String(registration && registration.name || '').trim(),
        email: includeContact ? String(registration && registration.email || '').trim().toLowerCase() : '',
        phone: includeContact ? String(registration && registration.phone || '').trim() : '',
        ghinNumber: includeContact ? String(registration && registration.ghinNumber || '').trim() : '',
        handicapIndex: includeContact && Number.isFinite(registration && registration.handicapIndex) ? registration.handicapIndex : null,
        slotLabel: '',
      });
    }
    return participants;
  }

  const rosterMap = includeContact ? await getActiveSeniorsRosterMap() : new Map();
  for (const teeTime of (ev.teeTimes || [])) {
    const teeLabel = ev.isTeamEvent ? String(teeTime && teeTime.name || 'Team').trim() : fmt.tee(teeTime && teeTime.time || '');
    for (const player of ((teeTime && teeTime.players) || [])) {
      const name = String(player && player.name || '').trim();
      const golfer = includeContact ? rosterMap.get(normalizeSeniorsGolferName(name).toLowerCase()) : null;
      participants.push({
        source: 'tee-time',
        id: String(player && player._id || ''),
        name,
        email: includeContact ? String(golfer && golfer.email || '').trim().toLowerCase() : '',
        phone: includeContact ? String(golfer && golfer.phone || '').trim() : '',
        ghinNumber: includeContact ? String(golfer && golfer.ghinNumber || '').trim() : '',
        handicapIndex: includeContact && Number.isFinite(golfer && golfer.handicapIndex) ? golfer.handicapIndex : null,
        slotLabel: teeLabel,
      });
    }
  }
  return participants;
}

async function sendSeniorsRegistrationConfirmationEmail(ev, golfer, slotLabel = '') {
  if (!ev || !golfer) return { ok: false, reason: 'missing-data' };
  const email = String(golfer.email || '').trim().toLowerCase();
  if (!email) return { ok: false, reason: 'no-email' };
  const typeLabel = seniorsEventTypeLabel(ev.seniorsEventType || (isSeniorsEventOnlyEvent(ev) ? 'outing' : 'tee-times'));
  const eventUrl = buildSiteEventUrl(ev.groupSlug, ev._id);
  const html = frame('Seniors Event Registration Confirmed',
    `<p>Your registration is confirmed.</p>
     <p><strong>Event Type:</strong> ${esc(typeLabel)}</p>
     <p><strong>Event:</strong> ${esc(ev.course || '')}</p>
     <p><strong>Date:</strong> ${esc(fmt.dateLong(ev.date))}</p>
     ${slotLabel ? `<p><strong>${ev.isTeamEvent ? 'Team' : 'Tee Time'}:</strong> ${esc(slotLabel)}</p>` : ''}
     <p>If you need to withdraw, please call Ken Roko.</p>${btn('View Event', eventUrl)}`);
  await sendEmail(email, `${typeLabel} Registration Confirmed: ${ev.course} (${fmt.dateISO(ev.date)})`, html);
  return { ok: true, email };
}

async function sendSeniorsTwoDayRegistrantReminders(groupSlug = 'seniors') {
  const scopedGroupSlug = normalizeGroupSlug(groupSlug);
  if (scopedGroupSlug !== 'seniors') return { ok: true, sent: 0, events: 0, skipped: true };
  const targetDate = ymdLocalPlusDays(2);
  const start = new Date(`${targetDate}T00:00:00.000Z`);
  const end = new Date(start.getTime() + 24 * 60 * 60 * 1000);
  const events = await Event.find({
    ...groupScopeFilter(scopedGroupSlug),
    date: { $gte: start, $lt: end }
  }).lean();
  let sent = 0;
  for (const ev of events) {
    const participants = await buildSeniorsParticipantRows(ev, { includeContact: true });
    const seenEmails = new Set();
    for (const participant of participants) {
      const email = String(participant.email || '').trim().toLowerCase();
      if (!email || seenEmails.has(email)) continue;
      seenEmails.add(email);
      const typeLabel = seniorsEventTypeLabel(ev.seniorsEventType || (isSeniorsEventOnlyEvent(ev) ? 'outing' : 'tee-times'));
      const eventUrl = buildSiteEventUrl(ev.groupSlug, ev._id);
      const html = frame('Seniors Event Reminder',
        `<p>This is your two-day reminder for an upcoming Seniors event.</p>
         <p><strong>Event Type:</strong> ${esc(typeLabel)}</p>
         <p><strong>Event:</strong> ${esc(ev.course || '')}</p>
         <p><strong>Date:</strong> ${esc(fmt.dateLong(ev.date))}</p>
         ${participant.slotLabel ? `<p><strong>${ev.isTeamEvent ? 'Team' : 'Tee Time'}:</strong> ${esc(participant.slotLabel)}</p>` : ''}
         <p>If you need to withdraw, please call Ken Roko.</p>${btn('View Event', eventUrl)}`);
      await sendEmail(email, `Reminder: ${ev.course} (${fmt.dateISO(ev.date)})`, html);
      sent += 1;
    }
  }
  return { ok: true, sent, events: events.length, targetDate };
}

function csvCell(value = '') {
  return String(value === null || value === undefined ? '' : value).replace(/\r?\n/g, ' ').trim();
}

function buildCsv(rows = []) {
  return rows.map((row) => row.map((value) => `"${csvCell(value).replace(/"/g, '""')}"`).join(',')).join('\n');
}

function buildWorkbookBuffer(sheetName = 'Sheet1', rows = []) {
  const workbook = XLSX.utils.book_new();
  const worksheet = XLSX.utils.aoa_to_sheet(
    rows.map((row) => Array.isArray(row) ? row.map((value) => value === null || value === undefined ? '' : value) : [])
  );
  XLSX.utils.book_append_sheet(workbook, worksheet, String(sheetName || 'Sheet1').slice(0, 31) || 'Sheet1');
  return XLSX.write(workbook, { type: 'buffer', bookType: 'xlsx' });
}

function parseSpreadsheetRows(file) {
  if (!file || !file.buffer) return [];
  const originalName = String(file.originalname || '').trim().toLowerCase();
  const mimeType = String(file.mimetype || '').trim().toLowerCase();
  const isExcelFile = /\.(xlsx|xls)$/i.test(originalName)
    || mimeType === 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    || mimeType === 'application/vnd.ms-excel';
  if (!isExcelFile) {
    return parseCsv(file.buffer.toString('utf8') || '');
  }
  const workbook = XLSX.read(file.buffer, { type: 'buffer' });
  const firstSheetName = Array.isArray(workbook.SheetNames) && workbook.SheetNames.length
    ? workbook.SheetNames[0]
    : '';
  if (!firstSheetName || !workbook.Sheets[firstSheetName]) return [];
  return XLSX.utils.sheet_to_json(workbook.Sheets[firstSheetName], {
    header: 1,
    raw: false,
    defval: '',
    blankrows: false,
  });
}

app.post('/api/admin/templates/tee-times-site-package', async (req, res) => {
  try {
    const guideOnly = String(req.query.guideOnly || req.body?.guideOnly || '').trim().toLowerCase();
    const allowGuideOnly = guideOnly === '1' || guideOnly === 'true' || guideOnly === 'yes';
    if (!isMainSiteAdminRequest(req)) {
      return res.status(403).json({ error: 'Admin code required' });
    }
    const pkg = buildTeeTimesSiteTemplatePackage(req.body || {});
    let deployment = null;
    if (!allowGuideOnly) {
      if (!Settings) return res.status(500).json({ error: 'Settings model unavailable for group deployment' });
      const groupSlug = normalizeGroupSlug(pkg.deploymentProfile && pkg.deploymentProfile.groupSlug
        ? pkg.deploymentProfile.groupSlug
        : pkg.packageSlug);
      const adminCode = cleanAccessCode(pkg.deploymentProfile && pkg.deploymentProfile.adminCode || '');
      const deleteCode = cleanAccessCode(pkg.deploymentProfile && pkg.deploymentProfile.deleteCode || '');
      const inboundEmailAlias = String(pkg.deploymentProfile && pkg.deploymentProfile.inboundEmailAlias || '').trim().toLowerCase();
      if (groupSlug !== DEFAULT_SITE_GROUP_SLUG) {
        if (!adminCode || adminCode === 'change-me') {
          return res.status(400).json({ error: 'A dedicated non-placeholder group admin code is required for live deployment' });
        }
        if (!deleteCode || deleteCode === 'change-me') {
          return res.status(400).json({ error: 'A dedicated non-placeholder group delete code is required for live deployment' });
        }
        if (!inboundEmailAlias) {
          return res.status(400).json({ error: 'A group inbound email alias is required for live deployment' });
        }
      }
      const existing = await Settings.findOne(scopedSettingQuery(groupSlug, 'siteProfile')).lean();
      const savedProfile = await saveSiteProfile(groupSlug, pkg.deploymentProfile || {});
      deployment = {
        created: !existing,
        groupSlug,
        profile: savedProfile,
        publicProfile: toPublicSiteProfile(savedProfile),
        links: buildGroupDeploymentLinks(groupSlug),
      };
    }
    return res.status(201).json({
      package: pkg,
      filename: `${pkg.packageSlug || 'tee-times-group'}-tee-times-site-package.json`,
      guideFilename: `${pkg.packageSlug || 'tee-times-group'}-deployment-guide.md`,
      deployment,
      message: allowGuideOnly
        ? 'Tee Times deployment guide created.'
        : (deployment && deployment.created ? 'Tee Times group deployed from template.' : 'Tee Times group template updated.'),
    });
  } catch (error) {
    return res.status(500).json({ error: error && error.message ? error.message : 'Failed to build site package' });
  }
});

app.get('/api/site-profile', async (req, res) => {
  try {
    const groupSlug = getGroupSlug(req);
    const profile = await getSiteProfile(groupSlug);
    return res.json({
      profile: toPublicSiteProfile(profile),
      links: buildGroupDeploymentLinks(groupSlug),
    });
  } catch (error) {
    return res.status(500).json({ error: error && error.message ? error.message : 'Failed to load site profile' });
  }
});

app.get('/api/operations-guide', async (_req, res) => {
  try {
    const payload = await buildOperationsGuidePayload();
    return res.json(payload);
  } catch (error) {
    return res.status(500).json({ error: error && error.message ? error.message : 'Failed to load operations guide' });
  }
});

app.get('/api/admin/site-profile', async (req, res) => {
  try {
    if (!isSiteAdmin(req)) return res.status(403).json({ error: 'Admin code required' });
    const groupSlug = getGroupSlug(req);
    const profile = await getSiteProfile(groupSlug);
    return res.json({
      profile: toAdminEditableSiteProfile(profile),
      publicProfile: toPublicSiteProfile(profile),
      links: buildGroupDeploymentLinks(groupSlug),
    });
  } catch (error) {
    return res.status(500).json({ error: error && error.message ? error.message : 'Failed to load admin site profile' });
  }
});

app.get('/api/admin/site-groups', async (req, res) => {
  try {
    if (!isMainSiteAdminRequest(req)) return res.status(403).json({ error: 'Admin code required' });
    const groupSlugs = await getAllManagedGroupSlugs();
    const groups = await Promise.all(groupSlugs.map(async (groupSlug) => {
      const storedProfile = await getSiteProfile(groupSlug);
      const profile = toPublicSiteProfile(storedProfile);
      const links = buildGroupDeploymentLinks(groupSlug);
      const accessConfig = normalizeStoredGroupAccessConfig(groupSlug, storedProfile);
      return {
        groupSlug: normalizeGroupSlug(groupSlug),
        isMainGroup: normalizeGroupSlug(groupSlug) === DEFAULT_SITE_GROUP_SLUG,
        groupReference: resolveGroupReference(groupSlug, profile),
        groupName: String(profile.groupName || profile.siteTitle || '').trim(),
        siteTitle: String(profile.siteTitle || '').trim(),
        inboundEmailAlias: String(storedProfile.inboundEmailAlias || '').trim(),
        hasDedicatedAdminCode: Boolean(accessConfig.adminCode),
        hasDedicatedDeleteCode: Boolean(accessConfig.deleteCode),
        links,
      };
    }));
    groups.sort((a, b) => {
      if (a.isMainGroup && !b.isMainGroup) return -1;
      if (!a.isMainGroup && b.isMainGroup) return 1;
      return String(a.groupReference || '').localeCompare(String(b.groupReference || ''))
        || String(a.groupSlug || '').localeCompare(String(b.groupSlug || ''));
    });
    return res.json({ groups });
  } catch (error) {
    return res.status(500).json({ error: error && error.message ? error.message : 'Failed to load site groups' });
  }
});

app.put('/api/admin/site-profile', async (req, res) => {
  try {
    if (!isSiteAdmin(req)) return res.status(403).json({ error: 'Admin code required' });
    const groupSlug = getGroupSlug(req);
    const incoming = { ...(req.body || {}) };
    if (normalizeGroupSlug(groupSlug) === 'seniors') {
      const currentProfile = await getSiteProfile(groupSlug);
      Object.assign(incoming, {
        siteTitle: currentProfile.siteTitle,
        groupName: currentProfile.groupName,
        groupReference: currentProfile.groupReference,
        groupSlug: currentProfile.groupSlug,
      });
    }
    const profile = await saveSiteProfile(groupSlug, incoming);
    return res.json({
      ok: true,
      profile: toAdminEditableSiteProfile(profile),
      publicProfile: toPublicSiteProfile(profile),
      links: buildGroupDeploymentLinks(groupSlug),
    });
  } catch (error) {
    return res.status(500).json({ error: error && error.message ? error.message : 'Failed to save site profile' });
  }
});

function backupIdFromDate(date = new Date()) {
  const iso = new Date(date.getTime() - (date.getTimezoneOffset() * 60000)).toISOString().replace(/\.\d{3}Z$/, 'Z');
  return `backup-${iso.replace(/[:]/g, '-').replace(/\./g, '-').replace('T', '_')}`;
}

function weekKeyInTZ(date = new Date(), timeZone = 'America/New_York') {
  const parts = new Intl.DateTimeFormat('en-US', {
    timeZone,
    year: 'numeric',
    month: 'numeric',
    day: 'numeric',
  }).formatToParts(date);
  const year = Number(parts.find((part) => part.type === 'year')?.value || 0);
  const month = Number(parts.find((part) => part.type === 'month')?.value || 1);
  const day = Number(parts.find((part) => part.type === 'day')?.value || 1);
  const dt = new Date(Date.UTC(year, month - 1, day));
  const dayNum = dt.getUTCDay() || 7;
  dt.setUTCDate(dt.getUTCDate() + 4 - dayNum);
  const yearStart = new Date(Date.UTC(dt.getUTCFullYear(), 0, 1));
  const weekNo = Math.ceil((((dt - yearStart) / 86400000) + 1) / 7);
  return `${dt.getUTCFullYear()}-W${String(weekNo).padStart(2, '0')}`;
}

function isSafeBackupSegment(value = '') {
  return /^[A-Za-z0-9._-]+$/.test(String(value || ''));
}

async function ensureConnectionReady(conn) {
  if (!conn) throw new Error('Database connection is unavailable');
  if (conn.readyState === 1) return conn;
  if (conn.readyState === 2) {
    await new Promise((resolve, reject) => {
      const onOpen = () => {
        conn.off('error', onError);
        resolve();
      };
      const onError = (error) => {
        conn.off('open', onOpen);
        reject(error);
      };
      conn.once('open', onOpen);
      conn.once('error', onError);
    });
    return conn;
  }
  throw new Error('Database connection is not ready');
}

async function walkSnapshotFiles(absPath, relPath = '', files = []) {
  const stat = await fsp.stat(absPath);
  if (stat.isDirectory()) {
    const entries = await fsp.readdir(absPath, { withFileTypes: true });
    for (const entry of entries) {
      if (entry.name === '.git' || entry.name === 'node_modules' || entry.name === 'backups') continue;
      await walkSnapshotFiles(path.join(absPath, entry.name), path.join(relPath, entry.name), files);
    }
    return files;
  }
  const raw = await fsp.readFile(absPath);
  files.push({
    path: relPath.replace(/\\/g, '/'),
    size: raw.length,
    encoding: 'base64',
    data: raw.toString('base64'),
  });
  return files;
}

async function buildSiteSnapshotFile(destFile) {
  const files = [];
  for (const target of SITE_BACKUP_TARGETS) {
    const absTarget = path.join(__dirname, target);
    try {
      await fsp.access(absTarget);
    } catch (_err) {
      continue;
    }
    await walkSnapshotFiles(absTarget, target, files);
  }
  const totalBytes = files.reduce((sum, entry) => sum + Number(entry.size || 0), 0);
  const payload = {
    createdAt: new Date().toISOString(),
    root: __dirname,
    targets: SITE_BACKUP_TARGETS.slice(),
    fileCount: files.length,
    totalBytes,
    files,
  };
  await fsp.writeFile(destFile, zlib.gzipSync(Buffer.from(JSON.stringify(payload))), 'binary');
  return {
    fileCount: files.length,
    totalBytes,
  };
}

async function buildDatabaseSnapshotFile(conn, label, destFile) {
  const readyConn = await ensureConnectionReady(conn);
  const db = readyConn.db;
  const collections = await db.listCollections().toArray();
  const collectionSummaries = [];
  const exportPayload = {
    label,
    createdAt: new Date().toISOString(),
    databaseName: db.databaseName,
    collections: {},
  };

  for (const collection of collections) {
    const name = String(collection && collection.name || '').trim();
    if (!name || name.startsWith('system.')) continue;
    const nativeCollection = db.collection(name);
    const [documents, indexes] = await Promise.all([
      nativeCollection.find({}).toArray(),
      nativeCollection.indexes().catch(() => ([])),
    ]);
    exportPayload.collections[name] = {
      indexes,
      documents,
    };
    collectionSummaries.push({
      name,
      count: documents.length,
    });
  }

  const serialized = EJSON.stringify(exportPayload, null, 2, { relaxed: false });
  await fsp.writeFile(destFile, zlib.gzipSync(Buffer.from(serialized, 'utf8')), 'binary');
  return {
    databaseName: db.databaseName,
    collectionCount: collectionSummaries.length,
    documentCount: collectionSummaries.reduce((sum, row) => sum + Number(row.count || 0), 0),
    collections: collectionSummaries,
  };
}

async function statFileSafe(filePath) {
  try {
    return await fsp.stat(filePath);
  } catch (_err) {
    return null;
  }
}

async function loadBackupManifest(backupId) {
  if (!isSafeBackupSegment(backupId)) throw new Error('Invalid backup id');
  const manifestPath = path.join(BACKUP_ROOT, backupId, 'manifest.json');
  return JSON.parse(await fsp.readFile(manifestPath, 'utf8'));
}

async function pruneBackupRetention(retainCount = BACKUP_SETTINGS_DEFAULTS.retainCount) {
  const keep = Math.max(1, Number(retainCount) || BACKUP_SETTINGS_DEFAULTS.retainCount);
  const backups = await listAdminBackups();
  if (backups.length <= keep) return { removed: [] };
  const removable = backups.slice(keep);
  const removed = [];
  for (const backup of removable) {
    const backupId = String(backup && backup.id || '').trim();
    if (!isSafeBackupSegment(backupId)) continue;
    const backupDir = path.join(BACKUP_ROOT, backupId);
    await fsp.rm(backupDir, { recursive: true, force: true });
    removed.push(backupId);
  }
  return { removed };
}

async function loadDatabaseSnapshotFile(filePath) {
  const raw = await fsp.readFile(filePath);
  return EJSON.parse(zlib.gunzipSync(raw).toString('utf8'), { relaxed: false });
}

function buildRestoreIndexes(indexes = []) {
  return indexes
    .filter((index) => index && index.name && index.name !== '_id_' && index.key)
    .map((index) => {
      const options = {
        name: index.name,
      };
      if (index.unique) options.unique = true;
      if (index.sparse) options.sparse = true;
      if (index.expireAfterSeconds !== undefined) options.expireAfterSeconds = index.expireAfterSeconds;
      return { key: index.key, ...options };
    });
}

async function restoreDatabaseFromSnapshot(conn, snapshot = {}, label = 'database') {
  const readyConn = await ensureConnectionReady(conn);
  const db = readyConn.db;
  const collectionEntries = Object.entries(snapshot && snapshot.collections && typeof snapshot.collections === 'object'
    ? snapshot.collections
    : {});
  const targetNames = new Set(collectionEntries.map(([name]) => name));
  const existing = await db.listCollections().toArray();
  for (const collection of existing) {
    const name = String(collection && collection.name || '').trim();
    if (!name || name.startsWith('system.')) continue;
    await db.collection(name).drop().catch(() => {});
  }

  for (const [name, payload] of collectionEntries) {
    const documents = Array.isArray(payload && payload.documents) ? payload.documents : [];
    const indexes = buildRestoreIndexes(Array.isArray(payload && payload.indexes) ? payload.indexes : []);
    await db.createCollection(name).catch((error) => {
      if (!/already exists/i.test(String(error && error.message || ''))) throw error;
    });
    const collection = db.collection(name);
    if (documents.length) {
      await collection.insertMany(documents, { ordered: true });
    }
    if (indexes.length) {
      await collection.createIndexes(indexes);
    }
  }

  return {
    label,
    databaseName: db.databaseName,
    collectionCount: targetNames.size,
    documentCount: collectionEntries.reduce((sum, [, payload]) => sum + (Array.isArray(payload && payload.documents) ? payload.documents.length : 0), 0),
  };
}

async function createAdminBackupBundle() {
  const backupSettings = await getBackupSettings();
  const id = backupIdFromDate(new Date());
  const backupDir = path.join(BACKUP_ROOT, id);
  await fsp.mkdir(backupDir, { recursive: true });

  const primaryFile = path.join(backupDir, 'primary-db.ejson.gz');
  const secondaryFile = path.join(backupDir, 'secondary-db.ejson.gz');
  const siteFile = path.join(backupDir, 'site-snapshot.json.gz');
  const manifestFile = path.join(backupDir, 'manifest.json');
  const secondaryConn = getSecondaryConn();

  const [primarySummary, secondarySummary, siteSummary] = await Promise.all([
    buildDatabaseSnapshotFile(mongoose.connection, 'primary', primaryFile),
    secondaryConn
      ? buildDatabaseSnapshotFile(secondaryConn, 'secondary', secondaryFile)
      : Promise.resolve({
        databaseName: null,
        collectionCount: 0,
        documentCount: 0,
        collections: [],
        available: false,
      }),
    buildSiteSnapshotFile(siteFile),
  ]);

  const primaryStat = await statFileSafe(primaryFile);
  const secondaryStat = await statFileSafe(secondaryFile);
  const siteStat = await statFileSafe(siteFile);
  const files = [
    { name: 'primary-db.ejson.gz', size: primaryStat ? primaryStat.size : 0 },
    { name: 'site-snapshot.json.gz', size: siteStat ? siteStat.size : 0 },
  ];
  if (secondaryStat) files.splice(1, 0, { name: 'secondary-db.ejson.gz', size: secondaryStat.size });
  const manifest = {
    id,
    createdAt: new Date().toISOString(),
    app: {
      siteUrl: SITE_URL,
      nodeVersion: process.version,
    },
    files,
    databases: {
      primary: primarySummary,
      secondary: secondarySummary,
    },
    site: siteSummary,
    retention: {
      retainCount: backupSettings.retainCount,
    },
    notes: [
      'Database files are EJSON gzip exports.',
      'Site snapshot file is a gzip JSON package of application files.',
      'Store a copy of this backup outside the server machine for disaster recovery.',
    ],
  };
  await fsp.writeFile(manifestFile, JSON.stringify(manifest, null, 2), 'utf8');
  const retention = await pruneBackupRetention(backupSettings.retainCount);
  if (retention.removed.length) manifest.retention.removed = retention.removed;
  await updateBackupStatus({
    lastSuccessfulBackupAt: manifest.createdAt,
    lastSuccessfulBackupId: manifest.id,
    lastSuccessfulBackupBytes: files.reduce((sum, file) => sum + Number(file && file.size || 0), 0),
    lastFailureAt: null,
    lastFailureMessage: '',
  });
  return manifest;
}

async function listAdminBackups() {
  await fsp.mkdir(BACKUP_ROOT, { recursive: true });
  const entries = await fsp.readdir(BACKUP_ROOT, { withFileTypes: true });
  const backups = [];
  for (const entry of entries) {
    if (!entry.isDirectory() || !isSafeBackupSegment(entry.name)) continue;
    const dirPath = path.join(BACKUP_ROOT, entry.name);
    const manifestPath = path.join(dirPath, 'manifest.json');
    try {
      const manifest = JSON.parse(await fsp.readFile(manifestPath, 'utf8'));
      backups.push(manifest);
    } catch (_err) {
      const stat = await fsp.stat(dirPath).catch(() => null);
      backups.push({
        id: entry.name,
        createdAt: stat ? stat.mtime.toISOString() : null,
        files: [],
        note: 'Manifest missing',
      });
    }
  }
  backups.sort((a, b) => String(b.createdAt || '').localeCompare(String(a.createdAt || '')));
  return backups;
}

function monthKeyInTZ(date = new Date(), timeZone = LOCAL_TZ) {
  const parts = new Intl.DateTimeFormat('en-CA', {
    timeZone,
    year: 'numeric',
    month: '2-digit',
  }).formatToParts(date);
  const year = parts.find((part) => part.type === 'year')?.value || '0000';
  const month = parts.find((part) => part.type === 'month')?.value || '00';
  return `${year}-${month}`;
}

function monthInActiveSeason(settings, date = new Date(), timeZone = LOCAL_TZ) {
  if (!settings.activeSeasonOnly) return true;
  const parts = new Intl.DateTimeFormat('en-US', {
    timeZone,
    month: 'numeric',
  }).formatToParts(date);
  const month = Number(parts.find((part) => part.type === 'month')?.value || 1);
  const start = Number(settings.activeSeasonStartMonth || 1);
  const end = Number(settings.activeSeasonEndMonth || 12);
  if (start <= end) return month >= start && month <= end;
  return month >= start || month <= end;
}

function nextScheduledBackupAt(settings, now = new Date(), timeZone = LOCAL_TZ) {
  const candidates = [];
  const start = new Date(now.getTime() + 60000);
  for (let dayOffset = 0; dayOffset < 400; dayOffset += 1) {
    const cursor = new Date(start.getTime() + (dayOffset * 86400000));
    const parts = new Intl.DateTimeFormat('en-US', {
      timeZone,
      year: 'numeric',
      month: 'numeric',
      day: 'numeric',
    }).formatToParts(cursor);
    const year = Number(parts.find((part) => part.type === 'year')?.value || 0);
    const month = Number(parts.find((part) => part.type === 'month')?.value || 1);
    const day = Number(parts.find((part) => part.type === 'day')?.value || 1);
    const weekday = new Date(Date.UTC(year, month - 1, day)).getUTCDay();
    if (settings.monthlyEnabled && day === Number(settings.monthlyDay || 1)) {
      candidates.push(new Date(year, month - 1, day, Number(settings.monthlyHour || 0), Number(settings.monthlyMinute || 0), 0, 0));
    }
    if (settings.weeklyEnabled && weekday === Number(settings.weeklyDay || 0)) {
      candidates.push(new Date(year, month - 1, day, Number(settings.weeklyHour || 0), Number(settings.weeklyMinute || 0), 0, 0));
    }
    if (settings.dailyEnabled && monthInActiveSeason(settings, cursor, timeZone)) {
      candidates.push(new Date(year, month - 1, day, Number(settings.dailyHour || 0), Number(settings.dailyMinute || 0), 0, 0));
    }
    const valid = candidates.filter((candidate) => candidate.getTime() > now.getTime());
    if (valid.length) {
      valid.sort((a, b) => a.getTime() - b.getTime());
      return valid[0].toISOString();
    }
  }
  return null;
}

function buildBackupOverview(backups = [], settings = {}, status = {}) {
  const latest = Array.isArray(backups) && backups.length ? backups[0] : null;
  const lastSuccessfulBackupAt = status.lastSuccessfulBackupAt || (latest && latest.createdAt) || null;
  const lastSuccessfulBackupId = status.lastSuccessfulBackupId || (latest && latest.id) || '';
  const lastSuccessfulBackupBytes = Number(status.lastSuccessfulBackupBytes || 0) || (
    latest ? (Array.isArray(latest.files) ? latest.files.reduce((sum, file) => sum + Number(file && file.size || 0), 0) : 0) : 0
  );
  const warnings = [];
  if (!lastSuccessfulBackupAt) warnings.push('No successful backups have been recorded yet.');
  if (!settings.offsiteCopyEnabled) warnings.push('Off-machine copy is not configured.');
  if (status.lastFailureAt && (!lastSuccessfulBackupAt || new Date(status.lastFailureAt).getTime() > new Date(lastSuccessfulBackupAt).getTime())) {
    warnings.push(`Most recent backup failure: ${status.lastFailureMessage || status.lastFailureAt}`);
  }
  return {
    lastSuccessfulBackupAt,
    lastSuccessfulBackupId,
    lastSuccessfulBackupBytes,
    lastFailureAt: status.lastFailureAt || null,
    lastFailureMessage: status.lastFailureMessage || '',
    nextScheduledBackupAt: nextScheduledBackupAt(settings),
    warnings,
  };
}
function buildDedupeKey(dateVal, teeTimes = [], isTeam = false, courseName = '') {
  if (isTeam) return null;
  if (!dateVal || !Array.isArray(teeTimes) || !teeTimes.length) return null;
  const d = asUTCDate(dateVal);
  if (isNaN(d)) return null;
  const dateISO = d.toISOString().slice(0, 10);
  const times = teeTimes.map((t) => t && t.time).filter(Boolean).sort();
  if (!times.length) return null;
  const normalizedCourse = String(courseName || '').trim().toLowerCase().replace(/\s+/g, ' ');
  return `${dateISO}|${times.join(',')}|${normalizedCourse || 'no-course'}`;
}

function buildEventStorageDedupeKey(dateVal, teeTimes = [], isTeam = false, groupSlug = DEFAULT_SITE_GROUP_SLUG, seniorsRegistrationMode = '', stableId = '', courseName = '') {
  const normalizedGroupSlug = normalizeGroupSlug(groupSlug);
  const normalizedSeniorsRegistrationMode = normalizeSeniorsRegistrationMode(seniorsRegistrationMode, normalizedGroupSlug);
  if (normalizedGroupSlug === 'seniors' && normalizedSeniorsRegistrationMode === 'event-only') {
    const d = asUTCDate(dateVal);
    const dateISO = isNaN(d) ? 'undated' : d.toISOString().slice(0, 10);
    const uniquePart = String(stableId || new mongoose.Types.ObjectId()).trim();
    return `seniors-event-only|${dateISO}|${uniquePart}`;
  }
  return buildDedupeKey(dateVal, teeTimes, isTeam, courseName);
}
function btn(label='Go to Sign-up Page', href = SITE_URL){
  return `<p style="margin:24px 0"><a href="${esc(href || SITE_URL)}" style="background:#2563eb;color:#fff;text-decoration:none;padding:10px 16px;border-radius:6px;display:inline-block">${esc(label)}</a></p>`;
}
function buildSiteEventUrl(groupSlug = DEFAULT_SITE_GROUP_SLUG, eventId = '', extraParams = {}) {
  const base = new URL(SITE_URL);
  if (eventId) base.searchParams.set('event', String(eventId));
  const slug = normalizeGroupSlug(groupSlug);
  if (slug !== DEFAULT_SITE_GROUP_SLUG) base.searchParams.set('group', slug);
  Object.entries(extraParams || {}).forEach(([key, value]) => {
    if (value === undefined || value === null || value === '') return;
    base.searchParams.set(key, String(value));
  });
  return base.toString();
}
function frame(title, body){
  return `<table role="presentation" cellpadding="0" cellspacing="0" width="100%" style="font-family:system-ui,-apple-system,Segoe UI,Roboto,Arial,sans-serif;background:#f6f7f9;padding:24px"><tr><td align="center"><table role="presentation" cellpadding="0" cellspacing="0" width="600" style="max-width:600px;background:#ffffff;border-radius:10px;padding:24px;border:1px solid #e5e7eb"><tr><td><h2 style="margin:0 0 12px 0;color:#111827;font-size:20px">${esc(title)}</h2>${body}<p style="color:#6b7280;font-size:12px;margin-top:24px">You received this because you subscribed to tee time updates.</p></td></tr></table></td></tr></table>`;
}
function reminderEmail(blocks, opts = {}){
  // blocks: [{course, dateISO, dateLong, empties: ['08:18 AM','08:28 AM']}]
  if (!blocks.length) return '';
  const { daysAhead = 1 } = opts;
  const when = daysAhead === 2 ? 'in 2 days' : 'Tomorrow';
  const expl = daysAhead === 2
    ? '<p><strong>This is a 48-hour advance notice.</strong> These tee times are still empty for events happening in 2 days. Grab a spot if you want to play!</p>'
    : '<p>These tee times are still empty. Grab a spot:</p>';
  const rows = blocks.map(b=>{
    const list = b.empties.map(t=>`<li>${esc(t)}</li>`).join('');
    return `<div style="margin:12px 0;padding:12px;border:1px solid #e5e7eb;border-radius:8px">
      <p style="margin:0 0 6px 0"><strong>${esc(b.course)}</strong> — ${esc(b.dateLong)} (${esc(b.dateISO)})</p>
      <p style="margin:0 0 6px 0">Empty tee times:</p>
      <ul style="margin:0 0 0 18px">${list}</ul>
    </div>`;
  }).join('');
  return frame(`Reminder: Empty Tee Times ${when}`, `${expl}${rows}${btn('Go to Sign-up Page')}`);
}

function brianJonesEmptyTeeAlertEmail(blocks){
  if (!blocks.length) return '';
  const rows = blocks.map((b) => {
    const list = b.empties.map((t) => `<li>${esc(t)}</li>`).join('');
    return `<div style="margin:12px 0;padding:12px;border:1px solid #e5e7eb;border-radius:8px">
      <p style="margin:0 0 6px 0"><strong>${esc(b.course)}</strong> — ${esc(b.dateLong)} (${esc(b.dateISO)})</p>
      <p style="margin:0 0 6px 0">Empty tee times:</p>
      <ul style="margin:0 0 0 18px">${list}</ul>
    </div>`;
  }).join('');
  return frame('Alert: Empty Tee Times Tomorrow', `<p>The following tee times are still empty for tomorrow.</p>${rows}${btn('Go to Sign-up Page')}`);
}

async function runBrianJonesTomorrowEmptyTeeAlert(label = 'manual', groupSlug = DEFAULT_SITE_GROUP_SLUG){
  const scopedGroupSlug = normalizeGroupSlug(groupSlug);
  const blocks = await findEmptyTeeTimesForDay(1, scopedGroupSlug);
  const { clubEmail } = await getGroupContactTargets(scopedGroupSlug);
  if (!blocks.length) {
    console.log(JSON.stringify({ t:new Date().toISOString(), level:'info', msg:'brian-empty-alert-skip', reason:'no empty tees', label, groupSlug: scopedGroupSlug }));
    return { ok: true, sent: 0, to: clubEmail, message: 'No empty tee times for tomorrow', groupSlug: scopedGroupSlug };
  }
  const subject = 'Alert: Empty Tee Times for Tomorrow';
  const html = brianJonesEmptyTeeAlertEmail(blocks);
  const httpRes = await sendEmailViaResendApi(clubEmail, subject, html);
  if (httpRes && httpRes.ok) {
    console.log(JSON.stringify({ t:new Date().toISOString(), level:'info', msg:'brian-empty-alert-sent', method:'http', label, to:clubEmail, events: blocks.length, groupSlug: scopedGroupSlug }));
    return { ok: true, sent: 1, method: 'http', to: clubEmail, events: blocks.length, groupSlug: scopedGroupSlug };
  }
  const smtpRes = await sendEmail(clubEmail, subject, html);
  if (smtpRes && smtpRes.ok) {
    console.log(JSON.stringify({ t:new Date().toISOString(), level:'info', msg:'brian-empty-alert-sent', method:'smtp', label, to:clubEmail, events: blocks.length, groupSlug: scopedGroupSlug }));
    return { ok: true, sent: 1, method: 'smtp', to: clubEmail, events: blocks.length, groupSlug: scopedGroupSlug };
  }
  const error = (httpRes && httpRes.error && httpRes.error.message) || (smtpRes && smtpRes.error && smtpRes.error.message) || 'Unknown email error';
  console.error(JSON.stringify({ t:new Date().toISOString(), level:'error', msg:'brian-empty-alert-failed', label, to:clubEmail, error, groupSlug: scopedGroupSlug }));
  return { ok: false, sent: 0, to: clubEmail, error, groupSlug: scopedGroupSlug };
}

async function checkEmptyTeeTimesForAdminAlert(groupSlug = DEFAULT_SITE_GROUP_SLUG) {
  const scopedGroupSlug = normalizeGroupSlug(groupSlug);
  const blocks24 = await findEmptyTeeTimesForDay(1, scopedGroupSlug);
  const blocks48 = await findEmptyTeeTimesForDay(2, scopedGroupSlug);

  if (!blocks24.length && !blocks48.length) {
    return { ok: true, sent: 0, message: 'No empty tee times', groupSlug: scopedGroupSlug };
  }

  const renderSection = (blocks, title) => {
    if (!blocks.length) return '';
    const rows = blocks.map(b => {
      const list = b.empties.map(t => `<li>${esc(t)}</li>`).join('');
      return `<div style="margin:12px 0;padding:12px;border:1px solid #e5e7eb;border-radius:8px">
        <p style="margin:0 0 6px 0"><strong>${esc(b.course)}</strong> — ${esc(b.dateLong)} (${esc(b.dateISO)})</p>
        <p style="margin:0 0 6px 0">Empty tee times:</p>
        <ul style="margin:0 0 0 18px">${list}</ul>
      </div>`;
    }).join('');
    return `<h3 style="margin:8px 0 4px 0;">${title}</h3>${rows}`;
  };

  const body = `${renderSection(blocks24, 'Empty tee times in next 24 hours')}${renderSection(blocks48, 'Empty tee times in next 48 hours')}${btn('Go to Sign-up Page', buildSiteEventUrl(scopedGroupSlug))}`;
  const res = await sendAdminAlert('Admin Alert: Empty Tee Times', body, scopedGroupSlug);
  return { ok: true, sent: res.sent, counts: { within24: blocks24.length, within48: blocks48.length }, groupSlug: scopedGroupSlug };
}

/* local YMD in a TZ */
function ymdInTZ(d=new Date(), tz='America/New_York'){
  const fmt = new Intl.DateTimeFormat('en-CA',{ timeZone: tz, year:'numeric', month:'2-digit', day:'2-digit' });
  return fmt.format(d); // YYYY-MM-DD
}
function addDaysUTC(d, days){ const x = new Date(d.getTime()); x.setUTCDate(x.getUTCDate()+days); return x; }

/* ---------------- Anti-chaos helpers ---------------- */
// Check if a player name already exists in any tee time (case-insensitive)
function isDuplicatePlayerName(ev, playerName, excludeTeeId = null) {
  const normalizedName = String(playerName).trim().toLowerCase();
  for (const tt of (ev.teeTimes || [])) {
    if (excludeTeeId && String(tt._id) === String(excludeTeeId)) continue;
    for (const p of (tt.players || [])) {
      if (String(p.name).trim().toLowerCase() === normalizedName) {
        return true;
      }
    }
  }
  return false;
}

// Check if a player is already on another tee time (case-insensitive)
function isPlayerOnAnotherTee(ev, playerName, currentTeeId) {
  const normalizedName = String(playerName).trim().toLowerCase();
  for (const tt of (ev.teeTimes || [])) {
    if (String(tt._id) === String(currentTeeId)) continue;
    for (const p of (tt.players || [])) {
      if (String(p.name).trim().toLowerCase() === normalizedName) {
        return { found: true, teeId: tt._id, teeName: tt.name || tt.time };
      }
    }
  }
  return { found: false };
}

// Get human-readable label for a tee/team
function getTeeLabel(ev, teeId) {
  const tt = ev.teeTimes.id(teeId);
  if (!tt) return 'Unknown';
  if (ev.isTeamEvent) {
    if (tt.name) return tt.name;
    const idx = ev.teeTimes.findIndex(t => String(t._id) === String(teeId));
    return `${groupedSlotNamePrefix(ev)} ${idx + 1}`;
  }
  return tt.time ? fmt.tee(tt.time) : 'Unknown';
}

function slotCapacityForEvent(ev) {
  return ev && ev.isTeamEvent ? (ev.teamSizeMax || 4) : 4;
}

function slotMaxCapacityForEvent(ev) {
  return ev && ev.isTeamEvent ? slotCapacityForEvent(ev) : 5;
}

function slotFifthCount(tt) {
  return ((tt && tt.players) || []).filter((p) => !!(p && p.isFifth)).length;
}

function eventFifthCount(ev, opts = {}) {
  if (!ev || ev.isTeamEvent) return 0;
  const ignoredPlayerId = String(opts.ignorePlayerId || '').trim();
  return ((ev && ev.teeTimes) || []).reduce((sum, tt) => {
    if (!tt) return sum;
    normalizeSlotFifthState(ev, tt);
    return sum + ((tt.players || []).filter((player) => {
      if (!(player && player.isFifth)) return false;
      return !ignoredPlayerId || String(player._id) !== ignoredPlayerId;
    }).length);
  }, 0);
}

function slotPlayerCountIgnoring(tt, opts = {}) {
  if (!tt || !Array.isArray(tt.players)) return 0;
  const ignoredPlayerId = String(opts.ignorePlayerId || '').trim();
  if (!ignoredPlayerId) return tt.players.length;
  return tt.players.filter((player) => !player || String(player._id) !== ignoredPlayerId).length;
}

function eventHasOtherOpenBaseSlot(ev, targetTeeId = null, opts = {}) {
  if (!ev || ev.isTeamEvent) return false;
  const baseSize = slotCapacityForEvent(ev);
  const targetId = String(targetTeeId || '').trim();
  return ((ev && ev.teeTimes) || []).some((tt) => {
    if (!tt) return false;
    if (targetId && String(tt._id) === targetId) return false;
    return slotPlayerCountIgnoring(tt, opts) < baseSize;
  });
}

function evaluateFifthAvailability(ev, tt, opts = {}) {
  if (!ev || ev.isTeamEvent) {
    return { ok: false, error: 'team full' };
  }
  if (eventFifthCount(ev, opts) > 0) {
    return { ok: false, error: 'only one 5-some is allowed per event' };
  }
  if (eventHasOtherOpenBaseSlot(ev, tt && tt._id, opts)) {
    return { ok: false, error: 'fill every other tee time to four players before adding a 5th' };
  }
  return { ok: true, error: '' };
}

function normalizeSlotFifthState(ev, tt) {
  if (!tt) return;
  if (!Array.isArray(tt.players)) tt.players = [];

  if (ev && ev.isTeamEvent) {
    for (const player of tt.players) {
      if (player && player.isFifth) player.isFifth = false;
    }
    return;
  }

  if (tt.players.length <= 4) {
    for (const player of tt.players) {
      if (player && player.isFifth) player.isFifth = false;
    }
    return;
  }

  let keptMarkedPlayer = false;
  for (let i = tt.players.length - 1; i >= 0; i -= 1) {
    const player = tt.players[i];
    if (!player) continue;
    if (player.isFifth && !keptMarkedPlayer) {
      keptMarkedPlayer = true;
      continue;
    }
    if (player.isFifth) player.isFifth = false;
  }
  if (!keptMarkedPlayer && tt.players.length) {
    tt.players[tt.players.length - 1].isFifth = true;
  }
}

function evaluateSlotAddition(ev, tt, opts = {}) {
  if (!tt) return { ok: false, error: 'tee time not found', canAddFifth: false, asFifth: false };
  if (!Array.isArray(tt.players)) tt.players = [];
  normalizeSlotFifthState(ev, tt);

  const baseSize = slotCapacityForEvent(ev);
  const maxSize = slotMaxCapacityForEvent(ev);
  const allowFifth = !!opts.allowFifth;
  const fifthAvailability = evaluateFifthAvailability(ev, tt, { ignorePlayerId: opts.ignorePlayerId });
  const fifthAvailable = fifthAvailability.ok;
  const canAddFifth = !ev.isTeamEvent && tt.players.length === baseSize && slotFifthCount(tt) === 0 && fifthAvailable;

  if (tt.players.length < baseSize) {
    return { ok: true, asFifth: false, canAddFifth };
  }
  if (!ev.isTeamEvent && allowFifth && tt.players.length < maxSize && slotFifthCount(tt) === 0) {
    if (!fifthAvailable) {
      return { ok: false, error: fifthAvailability.error, canAddFifth: false, asFifth: false };
    }
    return { ok: true, asFifth: true, canAddFifth: false };
  }
  return {
    ok: false,
    error: ev.isTeamEvent ? 'team full' : 'tee time full',
    canAddFifth
  };
}

function buildPlayerEntry(name, opts = {}) {
  return {
    name: String(name || '').trim(),
    checkedIn: !!opts.checkedIn,
    isFifth: !!opts.isFifth
  };
}

function findNextOpenSlot(ev, preferredTeeId = null) {
  const maxSize = slotCapacityForEvent(ev);
  if (preferredTeeId) {
    const preferred = ev.teeTimes.id(preferredTeeId);
    if (preferred) {
      if (!Array.isArray(preferred.players)) preferred.players = [];
      if (preferred.players.length < maxSize) return preferred;
      return null;
    }
  }
  for (const tt of (ev.teeTimes || [])) {
    if (!Array.isArray(tt.players)) tt.players = [];
    if (tt.players.length < maxSize) return tt;
  }
  return null;
}

function auditContextFromEvent(ev = {}) {
  return {
    groupSlug: normalizeGroupSlug(ev.groupSlug),
    eventCourse: String(ev.course || '').trim(),
    eventDateISO: fmt.dateISO(ev.date),
    isTeamEvent: !!ev.isTeamEvent,
  };
}

function captureEventAuditSnapshot(ev = {}) {
  return {
    course: String(ev.course || '').trim(),
    dateISO: fmt.dateISO(ev.date),
    notes: String(ev.notes || ''),
    isTeamEvent: !!ev.isTeamEvent,
    teamSizeMax: Number(ev.teamSizeMax || 4),
    seniorsEventType: String(ev.seniorsEventType || '').trim().toLowerCase(),
    seniorsRegistrationMode: String(ev.seniorsRegistrationMode || '').trim().toLowerCase(),
    teeCount: Array.isArray(ev.teeTimes) ? ev.teeTimes.length : 0,
    courseInfoKey: JSON.stringify(normalizeCourseInfo(ev.courseInfo || {})),
  };
}

function buildEventUpdateAuditMessage(before = {}, after = {}) {
  const changed = [];
  if (before.course !== after.course) changed.push('course');
  if (before.dateISO !== after.dateISO) changed.push('date');
  if (before.notes !== after.notes) changed.push('notes');
  if (before.isTeamEvent !== after.isTeamEvent) changed.push('format');
  if (before.teamSizeMax !== after.teamSizeMax) changed.push('team size');
  if (before.seniorsEventType !== after.seniorsEventType) changed.push('Seniors event type');
  if (before.seniorsRegistrationMode !== after.seniorsRegistrationMode) changed.push('registration mode');
  if (before.teeCount !== after.teeCount) changed.push('slot count');
  if (before.courseInfoKey !== after.courseInfoKey) changed.push('course details');
  return changed.length
    ? `Updated event details: ${changed.join(', ')}.`
    : 'Updated event details.';
}

function formatAuditSlotLabel(label = '', isTeamEvent = false) {
  const trimmed = String(label || '').trim();
  if (!trimmed) return isTeamEvent ? 'team' : 'tee time';
  if (isTeamEvent) return trimmed;
  return /^\d{1,2}:\d{2}$/.test(trimmed) ? fmt.tee(trimmed) : trimmed;
}

function buildAuditMessage(action = '', playerName = '', data = {}) {
  const normalizedAction = String(action || '').trim().toLowerCase();
  const subject = String(playerName || '').trim();
  const teeLabel = formatAuditSlotLabel(data.teeLabel, data.isTeamEvent);
  const fromTeeLabel = formatAuditSlotLabel(data.fromTeeLabel, data.isTeamEvent);
  const toTeeLabel = formatAuditSlotLabel(data.toTeeLabel, data.isTeamEvent);
  switch (normalizedAction) {
    case 'create_event':
      return `Created event ${data.eventCourse || 'event'} on ${data.eventDateISO || 'the selected date'}.`;
    case 'update_event':
      return 'Updated event details.';
    case 'delete_event':
      return `Deleted event ${data.eventCourse || 'event'} on ${data.eventDateISO || 'the selected date'}.`;
    case 'restore_event':
      return `Restored event ${data.eventCourse || 'event'} on ${data.eventDateISO || 'the selected date'}.`;
    case 'request_extra_tee_time':
      return 'Requested an additional tee time from the club.';
    case 'restore_tee_time':
      return `Restored ${teeLabel}.`;
    case 'add_player':
      return `Added ${subject || 'player'} to ${teeLabel}.`;
    case 'remove_player':
      return `Removed ${subject || 'player'} from ${teeLabel}.`;
    case 'move_player':
      return `Moved ${subject || 'player'} from ${fromTeeLabel} to ${toTeeLabel}.`;
    case 'check_in_player':
      return `Checked in ${subject || 'player'} at ${teeLabel}.`;
    case 'undo_check_in_player':
      return `Cleared check-in for ${subject || 'player'} at ${teeLabel}.`;
    case 'bulk_check_in':
      return `Checked in all players at ${teeLabel}.`;
    case 'bulk_clear_check_in':
      return `Cleared check-in for all players at ${teeLabel}.`;
    case 'add_maybe':
      return `Added ${subject || 'player'} to the maybe list.`;
    case 'remove_maybe':
      return `Removed ${subject || 'player'} from the maybe list.`;
    case 'fill_maybe':
      return `Moved ${subject || 'player'} from the maybe list to ${teeLabel}.`;
    case 'seniors_register':
      return `Registered ${subject || 'player'} for this Seniors event.`;
    case 'remove_seniors_registration':
      return `Removed ${subject || 'player'} from this Seniors event.`;
    case 'apply_pairings': {
      const groupCount = Number(data.details && data.details.groupCount || 0);
      const playerCount = Number(data.details && data.details.playerCount || 0);
      if (groupCount || playerCount) {
        return `Applied pairings across ${groupCount || 0} group${groupCount === 1 ? '' : 's'} for ${playerCount || 0} player${playerCount === 1 ? '' : 's'}.`;
      }
      return 'Applied pairings.';
    }
    case 'randomize_skins_pops': {
      const shared = Array.isArray(data.details && data.details.sharedHoles) ? data.details.sharedHoles.join(', ') : '';
      const bonus = Array.isArray(data.details && data.details.bonusHoles) ? data.details.bonusHoles.join(', ') : '';
      if (shared && bonus) return `Randomized skins pop holes. 12-17: ${shared}. 18+: ${bonus}.`;
      return 'Randomized skins pop holes.';
    }
    case 'reset_skins_pops':
      return 'Cleared skins pop holes.';
    case 'refresh_weather':
      return 'Refreshed weather for this event.';
    default:
      return normalizedAction ? normalizedAction.replace(/_/g, ' ') : 'audit entry';
  }
}

function buildLegacyTeeAuditEntry(log = {}) {
  if (!log || !log._id) return null;
  const isTeamEvent = !!log.isTeamEvent;
  const noun = isTeamEvent ? 'team' : 'tee time';
  const beforeLabel = formatAuditSlotLabel(log.labelBefore, isTeamEvent);
  const afterLabel = formatAuditSlotLabel(log.labelAfter, isTeamEvent);
  let message = '';
  if (log.action === 'add') {
    message = `Added ${noun} ${afterLabel}.`;
  } else if (log.action === 'update') {
    message = `Updated ${noun} from ${beforeLabel} to ${afterLabel}.`;
  } else if (log.action === 'delete') {
    message = `Deleted ${noun} ${beforeLabel}.`;
    if (log.notifyClub) message += ' Club notification requested.';
    if (log.mailError) message += ` Email error: ${String(log.mailError).trim()}.`;
  } else {
    message = `${noun} ${String(log.action || 'change').trim()}.`;
  }
  return {
    _id: `tee-log-${String(log._id)}`,
    groupSlug: normalizeGroupSlug(log.groupSlug),
    eventId: log.eventId,
    action: `tee_time_${String(log.action || '').trim().toLowerCase()}`,
    playerName: '',
    teeId: log.teeId || null,
    fromTeeId: null,
    toTeeId: null,
    teeLabel: String(log.labelAfter || log.labelBefore || '').trim(),
    fromTeeLabel: '',
    toTeeLabel: '',
    eventCourse: String(log.course || '').trim(),
    eventDateISO: String(log.dateISO || '').trim(),
    isTeamEvent,
    message,
    details: {
      notifyClub: !!log.notifyClub,
      mailMethod: log.mailMethod || null,
      mailError: log.mailError || null,
      source: 'legacy_tee_time_log',
    },
    timestamp: log.createdAt || new Date(),
  };
}

// Log audit entry
async function logAudit(eventId, action, playerName, data = {}) {
  if (!AuditLog) return;
  try {
    const payload = {
      groupSlug: normalizeGroupSlug(data.groupSlug),
      eventId,
      action: String(action || '').trim().toLowerCase(),
      playerName: String(playerName || '').trim(),
      teeId: data.teeId || null,
      fromTeeId: data.fromTeeId || null,
      toTeeId: data.toTeeId || null,
      teeLabel: String(data.teeLabel || '').trim(),
      fromTeeLabel: String(data.fromTeeLabel || '').trim(),
      toTeeLabel: String(data.toTeeLabel || '').trim(),
      eventCourse: String(data.eventCourse || '').trim(),
      eventDateISO: String(data.eventDateISO || '').trim(),
      isTeamEvent: !!data.isTeamEvent,
      details: data.details && typeof data.details === 'object' ? data.details : {},
      timestamp: new Date(),
    };
    payload.message = String(data.message || buildAuditMessage(payload.action, payload.playerName, payload)).trim();
    await AuditLog.create(payload);
  } catch (e) {
    console.error('Audit log failed:', e.message);
  }
}

function buildEventCreatedAuditEntry(ev = {}, options = {}) {
  if (!ev || !ev._id || !ev.createdAt) return null;
  const cutoff = options && options.cutoff ? new Date(options.cutoff) : null;
  if (cutoff && !Number.isNaN(cutoff.getTime()) && new Date(ev.createdAt).getTime() < cutoff.getTime()) return null;
  const context = auditContextFromEvent(ev);
  return {
    _id: `created-${String(ev._id)}`,
    groupSlug: context.groupSlug,
    eventId: ev._id,
    action: 'create_event',
    playerName: 'SYSTEM',
    teeId: null,
    fromTeeId: null,
    toTeeId: null,
    teeLabel: '',
    fromTeeLabel: '',
    toTeeLabel: '',
    eventCourse: context.eventCourse,
    eventDateISO: context.eventDateISO,
    isTeamEvent: context.isTeamEvent,
    message: buildAuditMessage('create_event', 'SYSTEM', context),
    details: {},
    timestamp: ev.createdAt,
  };
}

function buildAdminTeeTimeAuditEntries({ auditLogs = [], teeTimeLogs = [], activeEventIds = new Set() } = {}) {
  const activeIds = activeEventIds instanceof Set
    ? activeEventIds
    : new Set(Array.from(activeEventIds || []).map((value) => String(value || '')));
  return (auditLogs || [])
    .concat((teeTimeLogs || []).map((entry) => buildLegacyTeeAuditEntry(entry)).filter(Boolean))
    .map((entry) => {
      const eventId = entry && entry.eventId ? String(entry.eventId) : '';
      const timestamp = entry && entry.timestamp ? new Date(entry.timestamp) : null;
      const source = entry && entry.details && entry.details.source === 'legacy_tee_time_log'
        ? 'legacy_tee_time_log'
        : 'audit_log';
      return {
        _id: String(entry && entry._id || ''),
        source,
        groupSlug: normalizeGroupSlug(entry && entry.groupSlug),
        eventId,
        eventCourse: String(entry && entry.eventCourse || '').trim(),
        eventDateISO: String(entry && entry.eventDateISO || '').trim(),
        isTeamEvent: !!(entry && entry.isTeamEvent),
        action: String(entry && entry.action || '').trim().toLowerCase(),
        message: String(entry && entry.message || buildAuditMessage(entry && entry.action, entry && entry.playerName, entry || {})).trim(),
        timestamp,
        eventExists: !!eventId && activeIds.has(eventId),
      };
    })
    .filter((entry) => entry.eventId && entry.timestamp instanceof Date && !Number.isNaN(entry.timestamp.getTime()))
    .sort((left, right) => right.timestamp.getTime() - left.timestamp.getTime());
}

/* ---------------- Core API (unchanged parts trimmed for brevity) ---------------- */
function genTeeTimes(startHHMM, count=3, mins=10) {
  if (!startHHMM) startHHMM = '08:00'; // Default to 08:00 if no time provided
  const m = /^(\d{1,2}):(\d{2})$/.exec(startHHMM);
  if (!m) return [{ time: startHHMM, players: [] }];
  let h = parseInt(m[1], 10), mm = parseInt(m[2], 10);
  const out = [];
  for (let i=0;i<count;i++) {
    const tMin = h*60 + mm + i*mins;
    const H = Math.floor(tMin/60)%24;
    const M = tMin%60;
    out.push({ time: String(H).padStart(2,'0') + ':' + String(M).padStart(2,'0'), players: [] });
  }
  return out;
}

/* Helper: generate next automatic team name for an event (smallest unused Team N) */
function nextTeamNameForEvent(ev) {
  const prefix = groupedSlotNamePrefix(ev);
  const used = new Set();
  (ev.teeTimes || []).forEach((tt, idx) => {
    if (tt && tt.name) used.add(String(tt.name).trim());
    else used.add(`${prefix} ${idx+1}`);
  });
  let n = 1;
  while (used.has(`${prefix} ${n}`)) n++;
  return `${prefix} ${n}`;
}

function buildInitialGroupedSlots({
  count = 3,
  startType = 'shotgun',
  startTime = '',
  prefix = 'Team',
} = {}) {
  const slotCount = Math.max(1, Math.min(24, Number(count || 3)));
  if (!String(startTime || '').trim()) {
    throw new Error('teamStartTime required for team events');
  }
  if (String(startType || 'shotgun') === 'shotgun') {
    return Array.from({ length: slotCount }, (_unused, index) => ({
      name: `${prefix} ${index + 1}`,
      time: startTime,
      players: [],
    }));
  }
  const times = genTeeTimes(startTime, slotCount, 9);
  return times.map((entry, index) => ({
    name: `${prefix} ${index + 1}`,
    time: entry.time,
    players: [],
  }));
}

/* Helper: compute next tee time by searching last valid time and adding mins (default 9), wrap at 24h */
function nextTeeTimeForEvent(ev, mins = 9, defaultTime = '07:00') {
  if (ev.teeTimes && ev.teeTimes.length) {
    for (let i = ev.teeTimes.length - 1; i >= 0; i--) {
      const lt = ev.teeTimes[i] && ev.teeTimes[i].time;
      if (typeof lt === 'string') {
        const m = /^(\d{1,2}):(\d{2})$/.exec(lt.trim());
        if (m) {
          const hours = parseInt(m[1], 10);
          const minutes = parseInt(m[2], 10);
          if (!Number.isNaN(hours) && !Number.isNaN(minutes)) {
            const total = hours * 60 + minutes + mins;
            const newHours = Math.floor(total / 60) % 24;
            const newMinutes = total % 60;
            return `${String(newHours).padStart(2, '0')}:${String(newMinutes).padStart(2, '0')}`;
          }
        }
      }
    }
  }
  return defaultTime;
}

function buildCalendarSummaryForEvents(events = []) {
  const summaryByDate = new Map();
  const now = new Date();
  const todayUtc = Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate());
  const DAY_MS = 24 * 60 * 60 * 1000;

  for (const ev of (events || [])) {
    const dateISO = fmt.dateISO(ev && ev.date);
    if (!dateISO) continue;
    const [year, month, day] = dateISO.split('-').map(Number);
    if (!Number.isInteger(year) || !Number.isInteger(month) || !Number.isInteger(day)) continue;
    const entry = summaryByDate.get(dateISO) || {
      date: dateISO,
      eventCount: 0,
      teamEventCount: 0,
      urgentTeeEventCount: 0,
      nonBlueRidgeTeeEventCount: 0,
    };
    entry.eventCount += 1;
    if (ev && ev.isTeamEvent) {
      entry.teamEventCount += 1;
    } else {
      const courseName = String((ev && ev.course) || '').trim().toLowerCase();
      const isBlueRidgeShadows = /blue\s*ridge\s*shadows/.test(courseName);
      if (courseName && !isBlueRidgeShadows) entry.nonBlueRidgeTeeEventCount += 1;
      const eventDayUtc = Date.UTC(year, month - 1, day);
      const daysUntil = Math.round((eventDayUtc - todayUtc) / DAY_MS);
      if (daysUntil >= 0 && daysUntil <= 3) entry.urgentTeeEventCount += 1;
    }
    summaryByDate.set(dateISO, entry);
  }

  return Array.from(summaryByDate.values()).sort((a, b) => String(a.date || '').localeCompare(String(b.date || '')));
}

function normalizePlayerName(name = '') {
  return String(name).trim().toLowerCase().replace(/\s+/g, ' ');
}

async function getHandicapIndexByNameMap(playerNames = []) {
  const byName = new Map();
  if (!Handicap || !playerNames.length) return byName;
  const normalizedWanted = new Set(playerNames.map((n) => normalizePlayerName(n)).filter(Boolean));
  if (!normalizedWanted.size) return byName;
  const handicaps = await Handicap.find({}, { name: 1, handicapIndex: 1 }).lean();
  for (const h of handicaps) {
    const key = normalizePlayerName(h.name);
    if (!key || !normalizedWanted.has(key) || byName.has(key)) continue;
    const idx = Number(h.handicapIndex);
    byName.set(key, Number.isFinite(idx) ? idx : null);
  }
  return byName;
}

async function buildPairingSuggestion(ev) {
  const maxSize = ev.isTeamEvent ? (ev.teamSizeMax || 4) : 4;
  const sourcePlayers = [];
  for (const tt of (ev.teeTimes || [])) {
    for (const p of (tt.players || [])) {
      const playerId = String(p._id);
      const name = String(p.name || '').trim();
      if (!name) continue;
      const item = {
        playerId,
        name,
        sourceTeeId: String(tt._id),
        sourceLabel: ev.isTeamEvent ? (tt.name || 'Team') : fmt.tee(tt.time)
      };
      sourcePlayers.push(item);
    }
  }
  if (!sourcePlayers.length) {
    return { groupSize: maxSize, groups: [], totalPlayers: 0, unassignedPlayers: [] };
  }

  const handicapMap = await getHandicapIndexByNameMap(sourcePlayers.map((p) => p.name));
  const playersWithHcp = sourcePlayers.map((p) => {
    const normalized = normalizePlayerName(p.name);
    const handicapIndex = handicapMap.has(normalized) ? handicapMap.get(normalized) : null;
    return { ...p, handicapIndex };
  });

  playersWithHcp.sort((a, b) => {
    const aIdx = Number.isFinite(a.handicapIndex) ? a.handicapIndex : 999;
    const bIdx = Number.isFinite(b.handicapIndex) ? b.handicapIndex : 999;
    if (aIdx !== bIdx) return bIdx - aIdx;
    return a.name.localeCompare(b.name);
  });

  const groupCount = Math.max(1, Math.ceil(playersWithHcp.length / maxSize));
  const groups = Array.from({ length: groupCount }, (_, index) => ({
    index,
    teeId: ev.teeTimes[index] ? String(ev.teeTimes[index]._id) : null,
    players: [],
    totalHandicap: 0,
    knownHandicapCount: 0
  }));

  for (const player of playersWithHcp) {
    const candidates = groups.filter((g) => g.players.length < maxSize);
    candidates.sort((a, b) => {
      if (a.totalHandicap !== b.totalHandicap) return a.totalHandicap - b.totalHandicap;
      return a.players.length - b.players.length;
    });
    const target = candidates[0];
    target.players.push(player);
    if (Number.isFinite(player.handicapIndex)) {
      target.totalHandicap += player.handicapIndex;
      target.knownHandicapCount += 1;
    }
  }

  const outputGroups = groups.map((g, index) => {
    const existingSlot = ev.teeTimes[index];
    const label = ev.isTeamEvent
      ? (existingSlot?.name || `Team ${index + 1}`)
      : (existingSlot?.time ? fmt.tee(existingSlot.time) : `Group ${index + 1}`);
    return {
      teeId: g.teeId,
      label,
      playerCount: g.players.length,
      avgHandicap: g.knownHandicapCount ? Number((g.totalHandicap / g.knownHandicapCount).toFixed(1)) : null,
      players: g.players.map((p) => ({
        playerId: p.playerId,
        name: p.name,
        handicapIndex: p.handicapIndex,
        sourceTeeId: p.sourceTeeId,
        sourceLabel: p.sourceLabel
      }))
    };
  });

  return {
    groupSize: maxSize,
    groups: outputGroups,
    totalPlayers: playersWithHcp.length,
    unassignedPlayers: []
  };
}

app.get('/api/events', cacheJson(10 * 1000), async (req, res) => {
  const items = await Event.find(scopeQuery(req)).sort({ date: 1 }).lean();
  res.json(items);
});

app.get('/api/events/calendar/summary', cacheJson(10 * 1000), async (req, res) => {
  try {
    const localYmd = ymdInTZ(new Date(), LOCAL_TZ);
    const [defaultYear, defaultMonth] = localYmd.split('-').map((v) => Number(v));
    const year = Number(req.query.year || defaultYear);
    const month = Number(req.query.month || defaultMonth);
    if (!Number.isInteger(year) || year < 2000 || year > 2100) {
      return res.status(400).json({ error: 'Invalid year; use YYYY' });
    }
    if (!Number.isInteger(month) || month < 1 || month > 12) {
      return res.status(400).json({ error: 'Invalid month; use 1-12' });
    }
    const start = new Date(Date.UTC(year, month - 1, 1, 0, 0, 0));
    const end = new Date(Date.UTC(year, month, 1, 0, 0, 0));
    const events = await Event.find(
      scopeQuery(req, { date: { $gte: start, $lt: end } }),
      { date: 1, isTeamEvent: 1, course: 1 }
    ).lean();
    res.json({
      year,
      month,
      days: buildCalendarSummaryForEvents(events),
    });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

app.get('/api/events/calendar/month.ics', async (req, res) => {
  try {
    const localYmd = ymdInTZ(new Date(), LOCAL_TZ);
    const [defaultYear, defaultMonth] = localYmd.split('-').map((v) => Number(v));
    const year = Number(req.query.year || defaultYear);
    const month = Number(req.query.month || defaultMonth);
    if (!Number.isInteger(year) || year < 2000 || year > 2100) {
      return res.status(400).json({ error: 'Invalid year; use YYYY' });
    }
    if (!Number.isInteger(month) || month < 1 || month > 12) {
      return res.status(400).json({ error: 'Invalid month; use 1-12' });
    }

    const includeTeamEvents = ['1', 'true', 'yes'].includes(String(req.query.includeTeams || '').trim().toLowerCase());
    const start = new Date(Date.UTC(year, month - 1, 1, 0, 0, 0));
    const end = new Date(Date.UTC(year, month, 1, 0, 0, 0));
    const query = scopeQuery(req, {
      date: { $gte: start, $lt: end },
      ...(includeTeamEvents ? {} : { isTeamEvent: false }),
    });
    const events = await Event.find(query).lean();

    const monthLabel = `${year}-${twoDigits(month)}`;
    const calName = includeTeamEvents ? `Golf Events ${monthLabel}` : `Tee Times ${monthLabel}`;
    const calDesc = includeTeamEvents
      ? `Monthly golf events export for ${monthLabel}`
      : `Monthly tee-time export for ${monthLabel}`;
    const icsBody = buildEventsIcs(events, { calName, calDesc });
    res.setHeader('Content-Type', 'text/calendar; charset=utf-8');
    res.setHeader('Content-Disposition', `attachment; filename="tee-times-${monthLabel}.ics"`);
    res.send(icsBody);
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

app.get('/groups/:groupSlug/calendar.ics', async (req, res) => {
  try {
    const groupSlug = normalizeGroupSlug(req.params.groupSlug);
    const includeTeamEvents = ['1', 'true', 'yes'].includes(String(req.query.includeTeams || '').trim().toLowerCase());
    const fromRaw = String(req.query.from || '').trim();
    let start = new Date();
    if (/^\d{4}-\d{2}-\d{2}$/.test(fromRaw)) {
      start = new Date(`${fromRaw}T00:00:00.000Z`);
    } else {
      start = new Date(Date.UTC(start.getUTCFullYear(), start.getUTCMonth(), start.getUTCDate(), 0, 0, 0));
    }
    const events = await Event.find({
      ...groupScopeFilter(groupSlug),
      date: { $gte: start },
      ...(includeTeamEvents ? {} : { isTeamEvent: false }),
    }).sort({ date: 1, createdAt: 1 }).lean();
    const profile = await getSiteProfile(groupSlug);
    const calendarLabel = profile.shortTitle || profile.siteTitle || 'Tee Times';
    const calName = includeTeamEvents ? `${calendarLabel} Events` : `${calendarLabel} Tee Times`;
    const calDesc = includeTeamEvents
      ? `Upcoming golf events for ${profile.groupName || profile.siteTitle || 'Tee Times'}`
      : `Upcoming tee times for ${profile.groupName || profile.siteTitle || 'Tee Times'}`;
    const icsBody = buildEventsIcs(events, { calName, calDesc });
    res.setHeader('Content-Type', 'text/calendar; charset=utf-8');
    res.setHeader('Content-Disposition', `attachment; filename="${groupSlug}-tee-times.ics"`);
    return res.send(icsBody);
  } catch (error) {
    return res.status(500).json({ error: error && error.message ? error.message : 'Failed to build group calendar feed' });
  }
});

app.get('/api/events/by-date', cacheJson(10 * 1000), async (req, res) => {
  try {
    const dateISO = String(req.query.date || '').trim();
    if (!/^\d{4}-\d{2}-\d{2}$/.test(dateISO)) {
      return res.status(400).json({ error: 'Invalid date; use YYYY-MM-DD' });
    }
    const start = new Date(`${dateISO}T00:00:00.000Z`);
    const end = new Date(start.getTime() + (24 * 60 * 60 * 1000));
    const events = await Event.find(scopeQuery(req, { date: { $gte: start, $lt: end } })).sort({ date: 1, createdAt: 1 }).lean();
    res.json({ date: dateISO, events });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// Fetch a single event by id for targeted refreshes
app.get('/api/events/:id', cacheJson(10 * 1000), async (req, res) => {
  try {
    const ev = await findScopedEventById(req, req.params.id, { lean: true });
    if (!ev) return res.status(404).json({ error: 'Event not found' });
    res.json(ev);
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

app.get('/api/seniors-roster/public', cacheJson(30 * 1000), async (req, res) => {
  try {
    if (normalizeGroupSlug(getGroupSlug(req)) !== 'seniors') return res.json([]);
    if (!SeniorsGolfer) return res.status(500).json({ error: 'Seniors golfer model not available' });
    const golfers = await SeniorsGolfer.find({ groupSlug: 'seniors', active: true }).sort({ nameKey: 1 }).lean();
    return res.json(golfers.map((golfer) => ({
      _id: golfer._id,
      name: golfer.name,
      ghinNumber: golfer.ghinNumber || '',
      handicapIndex: Number.isFinite(golfer.handicapIndex) ? golfer.handicapIndex : null,
    })));
  } catch (error) {
    return res.status(500).json({ error: error && error.message ? error.message : 'Failed to load Seniors roster' });
  }
});

app.post('/api/events/:id/seniors-register', async (req, res) => {
  try {
    if (normalizeGroupSlug(getGroupSlug(req)) !== 'seniors') return res.status(404).json({ error: 'Seniors registration is not available for this group' });
    const ev = await findScopedEventById(req, req.params.id);
    if (!ev) return res.status(404).json({ error: 'Event not found' });
    if (!isUpcomingOrCurrentEventDate(ev.date)) return res.status(400).json({ error: 'Registration is only available for current or future events' });

    const golferName = String(req.body && req.body.name || '').trim();
    if (!golferName) return res.status(400).json({ error: 'name required' });
    const golfer = await resolveSeniorsRosterGolferByName(golferName) || buildManualSeniorsGolferRecord(golferName);
    if (!golfer) return res.status(400).json({ error: 'name required' });

    const normalizedName = normalizeSeniorsGolferName(golfer.name).toLowerCase();
    const alreadyRegisteredEventOnly = (ev.seniorsRegistrations || []).some((entry) => normalizeSeniorsGolferName(entry && entry.name).toLowerCase() === normalizedName);
    const alreadyRegisteredTeeTime = (ev.teeTimes || []).some((teeTime) => ((teeTime && teeTime.players) || []).some((player) => normalizeSeniorsGolferName(player && player.name).toLowerCase() === normalizedName));
    if (alreadyRegisteredEventOnly || alreadyRegisteredTeeTime) {
      return res.status(409).json({ error: 'Golfer is already registered for this event' });
    }

    if (!isSeniorsEventOnlyEvent(ev)) {
      return res.status(400).json({ error: 'This event uses tee times. Sign up on an open tee time instead.' });
    }

    ev.seniorsRegistrations.push({
      golferId: golfer._id || null,
      name: golfer.name,
      email: golfer.email || '',
      phone: golfer.phone || '',
      ghinNumber: golfer.ghinNumber || '',
      handicapIndex: Number.isFinite(golfer.handicapIndex) ? golfer.handicapIndex : null,
    });
    await ev.save();
    await logAudit(ev._id, 'seniors_register', golfer.name, {
      ...auditContextFromEvent(ev),
      details: { registrationId: ev.seniorsRegistrations[ev.seniorsRegistrations.length - 1]?._id || null },
    });

    if (golfer.email) {
      await sendSeniorsRegistrationConfirmationEmail(ev, golfer);
    }
    return res.json({ ok: true, event: ev });
  } catch (error) {
    return res.status(500).json({ error: error && error.message ? error.message : 'Failed to register golfer' });
  }
});

app.delete('/api/events/:id/seniors-registrations/:registrationId', async (req, res) => {
  try {
    if (normalizeGroupSlug(getGroupSlug(req)) !== 'seniors') return res.status(404).json({ error: 'Seniors registration is not available for this group' });
    const ev = await findScopedEventById(req, req.params.id);
    if (!ev) return res.status(404).json({ error: 'Event not found' });
    const registration = ev.seniorsRegistrations.id(req.params.registrationId);
    if (!registration) return res.status(404).json({ error: 'Registration not found' });
    const golferName = String(registration.name || '').trim();
    registration.deleteOne();
    await ev.save();
    await logAudit(ev._id, 'remove_seniors_registration', golferName, {
      ...auditContextFromEvent(ev),
      details: { registrationId: req.params.registrationId },
    });
    return res.json({ ok: true, event: ev });
  } catch (error) {
    return res.status(500).json({ error: error && error.message ? error.message : 'Failed to remove registration' });
  }
});

app.get('/api/admin/events/:id/seniors-registrations/export.csv', async (req, res) => {
  if (requireSeniorsGroupAdmin(req, res)) return;
  try {
    const ev = await Event.findOne({ ...groupScopeFilter('seniors'), _id: req.params.id }).lean();
    if (!ev) return res.status(404).json({ error: 'Event not found' });
    const rows = await buildSeniorsParticipantRows(ev, { includeContact: true });
    const exportRows = [
      ['Name', 'Email', 'Phone', 'GHIN', 'Handicap', 'Slot', 'Event Type', 'Date', 'Course'],
      ...rows.map((row) => [
        row.name,
        row.email || '',
        row.phone || '',
        row.ghinNumber || '',
        Number.isFinite(row.handicapIndex) ? row.handicapIndex : '',
        row.slotLabel || '',
        seniorsEventTypeLabel(ev.seniorsEventType || (isSeniorsEventOnlyEvent(ev) ? 'outing' : 'tee-times')),
        fmt.dateISO(ev.date),
        ev.course || '',
      ]),
    ];
    const csv = buildCsv(exportRows);
    res.setHeader('Content-Type', 'text/csv; charset=utf-8');
    res.setHeader('Content-Disposition', `attachment; filename="seniors-event-${req.params.id}-registrations.csv"`);
    return res.send(`\ufeff${csv}`);
  } catch (error) {
    return res.status(500).json({ error: error && error.message ? error.message : 'Failed to export event registrations' });
  }
});

app.get('/api/admin/events/:id/seniors-registrations/export.xlsx', async (req, res) => {
  if (requireSeniorsGroupAdmin(req, res)) return;
  try {
    const ev = await Event.findOne({ ...groupScopeFilter('seniors'), _id: req.params.id }).lean();
    if (!ev) return res.status(404).json({ error: 'Event not found' });
    const rows = await buildSeniorsParticipantRows(ev, { includeContact: true });
    const workbook = buildWorkbookBuffer('Registrations', [
      ['Name', 'Email', 'Phone', 'GHIN', 'Handicap', 'Slot', 'Event Type', 'Date', 'Course'],
      ...rows.map((row) => [
        row.name,
        row.email || '',
        row.phone || '',
        row.ghinNumber || '',
        Number.isFinite(row.handicapIndex) ? row.handicapIndex : '',
        row.slotLabel || '',
        seniorsEventTypeLabel(ev.seniorsEventType || (isSeniorsEventOnlyEvent(ev) ? 'outing' : 'tee-times')),
        fmt.dateISO(ev.date),
        ev.course || '',
      ]),
    ]);
    res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
    res.setHeader('Content-Disposition', `attachment; filename="seniors-event-${req.params.id}-registrations.xlsx"`);
    return res.send(workbook);
  } catch (error) {
    return res.status(500).json({ error: error && error.message ? error.message : 'Failed to export event registrations' });
  }
});

app.get('/api/events/:id/calendar.ics', async (req, res) => {
  try {
    const ev = await findScopedEventById(req, req.params.id, { lean: true });
    if (!ev) return res.status(404).json({ error: 'Event not found' });
    const icsBody = buildEventIcs(ev);
    if (!icsBody) return res.status(400).json({ error: 'Unable to build calendar event' });
    const filename = eventCalendarFileName(ev);
    res.setHeader('Content-Type', 'text/calendar; charset=utf-8');
    res.setHeader('Content-Disposition', `attachment; filename="${filename}"`);
    res.send(icsBody);
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

app.post('/api/events', validateBody(validateCreateEvent), async (req, res) => {
  if (requireSeniorsSiteAdminForWrite(req, res)) return;
  try {
    const groupSlug = getGroupSlug(req);
    const { course, courseInfo, date, teeTime, teeTimes, notes, isTeamEvent, teamSizeMax, teamStartType, teamStartTime, teamCount, seniorsEventType, seniorsRegistrationMode } = req.body || {};
    const requestedSeniorsRegistrationMode = String(seniorsRegistrationMode || '').trim().toLowerCase();
    const seniorsEventOnly = normalizeGroupSlug(groupSlug) === 'seniors' && requestedSeniorsRegistrationMode === 'event-only';
    const normalizedSeniorsRegistrationMode = normalizeGroupSlug(groupSlug) === 'seniors'
      ? (seniorsEventOnly ? 'event-only' : 'tee-times')
      : normalizeSeniorsRegistrationMode(seniorsRegistrationMode, groupSlug);
    const normalizedSeniorsEventType = normalizeSeniorsEventType(seniorsEventType || (seniorsEventOnly ? 'outing' : 'tee-times'));
    const seniorsSignupShotgun = seniorsEventOnly
      && normalizedSeniorsEventType === 'regular-shotgun'
      && String(teamStartTime || '').trim();
    const groupedShotgunPrefix = normalizeGroupSlug(groupSlug) === 'seniors' && normalizedSeniorsEventType === 'regular-shotgun'
      ? 'Group'
      : 'Team';
    let tt;
    if (seniorsEventOnly) {
      tt = seniorsSignupShotgun
        ? [{ time: String(teamStartTime || '').trim(), players: [] }]
        : [];
    } else if (isTeamEvent) {
      tt = buildInitialGroupedSlots({
        count: teamCount || 3,
        startType: teamStartType || 'shotgun',
        startTime: teamStartTime,
        prefix: groupedShotgunPrefix,
      });
    } else {
      // Generate 3 default tee times for tee-time events
      if (!teeTime) return res.status(400).json({ error: 'teeTime required for tee-time events' });
      tt = Array.isArray(teeTimes) && teeTimes.length ? teeTimes : genTeeTimes(teeTime, 3, 9);
    }
    const eventDate = /^\d{4}-\d{2}-\d{2}$/.test(String(date||'')) ? new Date(String(date)+'T12:00:00Z') : asUTCDate(date);
    const dedupeKey = buildEventStorageDedupeKey(
      eventDate,
      tt,
      !!isTeamEvent,
      groupSlug,
      normalizedSeniorsRegistrationMode,
      '',
      course
    );
    const normalizedCourseInfo = enrichCourseInfo(course, courseInfo || {});
    const weatherData = await fetchWeatherForEvent({
      course,
      courseInfo: normalizedCourseInfo,
      date: eventDate,
    });
    
    if (dedupeKey && !seniorsEventOnly) {
      const existing = await Event.findOne(scopeQuery(req, { dedupeKey }));
      if (existing) {
        return res.status(200).json(existing);
      }
    }

    let created;
    try {
      created = await Event.create({
        groupSlug,
        course,
        courseInfo: normalizedCourseInfo,
        date: eventDate,
        notes,
        isTeamEvent: seniorsEventOnly ? false : !!isTeamEvent,
        seniorsEventType: normalizeGroupSlug(groupSlug) === 'seniors' ? normalizedSeniorsEventType : '',
        seniorsRegistrationMode: normalizeGroupSlug(groupSlug) === 'seniors' ? normalizedSeniorsRegistrationMode : '',
        teamSizeMax: Math.max(2, Math.min(4, Number(teamSizeMax || 4))),
        teeTimes: tt,
        dedupeKey: dedupeKey || undefined,
        weather: {
          condition: weatherData.condition,
          icon: weatherData.icon,
          temp: weatherData.temp,
          tempLow: weatherData.tempLow,
          tempHigh: weatherData.tempHigh,
          rainChance: weatherData.rainChance,
          description: weatherData.description,
          lastFetched: weatherData.lastFetched
        }
      });
    } catch (err) {
      // If another request created the event at the same time, return the existing one
      if (err && err.code === 11000 && dedupeKey && !seniorsEventOnly) {
        const existing = await Event.findOne(scopeQuery(req, { dedupeKey }));
        if (existing) return res.status(200).json(existing);
      }
      throw err;
    }
    await logAudit(created._id, 'create_event', 'SYSTEM', {
      ...auditContextFromEvent(created),
      message: `Created event ${created.course || 'event'} on ${fmt.dateISO(created.date) || 'the selected date'} with ${(created.teeTimes || []).length} ${created.isTeamEvent ? 'group' : 'tee time'}${(created.teeTimes || []).length === 1 ? '' : 's'}.`,
      details: {
        teeCount: Array.isArray(created.teeTimes) ? created.teeTimes.length : 0,
        seniorsEventType: created.seniorsEventType || '',
        seniorsRegistrationMode: created.seniorsRegistrationMode || '',
      },
    });
    res.status(201).json(created);
    const eventUrl = buildSiteEventUrl(created.groupSlug, created._id);
    if (normalizeGroupSlug(created.groupSlug) === 'seniors') {
      const typeLabel = seniorsEventTypeLabel(created.seniorsEventType || (isSeniorsEventOnlyEvent(created) ? 'outing' : 'tee-times'));
      await sendTeeTimeEventLifecycleEmail(`New Event: ${created.course} (${fmt.dateISO(created.date)})`,
        frame('A New Golf Event Has Been Scheduled!',
              `<p>The following event is now open for sign-up:</p>
               <p><strong>Event Type:</strong> ${esc(typeLabel)}</p>
               <p><strong>Event:</strong> ${esc(fmt.dateShortTitle(created.date))}</p>
               <p><strong>Course:</strong> ${esc(created.course||'')}</p>
               <p><strong>Date:</strong> ${esc(fmt.dateLong(created.date))}</p>
               ${created.teeTimes?.[0]?.time ? `<p><strong>${isSeniorsGroupedSlotEvent(created) ? 'Shotgun Start' : 'First Tee Time'}:</strong> ${esc(fmt.tee(created.teeTimes[0].time))}</p>`:''}
               <p>Please <a href="${eventUrl}" style="color:#166534;text-decoration:underline">click here to view this event directly</a> or visit the sign-up page to secure your spot!</p>${btn('Go to Sign-up Page', eventUrl)}`),
        { groupSlug: created.groupSlug });
    }
  } catch (e) { res.status(400).json({ error: e.message }); }
});

app.put('/api/events/:id', async (req, res) => {
  if (requireSeniorsSiteAdminForWrite(req, res)) return;
  try {
    const ev = await findScopedEventById(req, req.params.id);
    if (!ev) return res.status(404).json({ error: 'Not found' });
    const beforeAudit = captureEventAuditSnapshot(ev);
    const body = req.body || {};
    const { course, courseInfo, date, notes, isTeamEvent, teamSizeMax, seniorsEventType, seniorsRegistrationMode } = body;
    const hasCourseInfo = Object.prototype.hasOwnProperty.call(body, 'courseInfo');
    let weatherNeedsRefresh = false;

    if (course !== undefined) {
      const prevCourse = String(ev.course || '').trim().toLowerCase();
      const nextCourse = String(course || '').trim();
      const courseChanged = nextCourse.toLowerCase() !== prevCourse;
      ev.course = nextCourse;
      if (courseChanged) {
        weatherNeedsRefresh = true;
        // If the course changed and no courseInfo was provided, clear stale location metadata.
        if (!hasCourseInfo) ev.courseInfo = enrichCourseInfo(course, {});
      }
    }

    if (hasCourseInfo) {
      ev.courseInfo = enrichCourseInfo(ev.course, courseInfo || {});
      weatherNeedsRefresh = true;
    }

    if (date !== undefined) {
      ev.date = /^\d{4}-\d{2}-\d{2}$/.test(String(date)) ? new Date(String(date)+'T12:00:00Z') : asUTCDate(date);
      weatherNeedsRefresh = true;
    }
    if (notes !== undefined) ev.notes = String(notes);
    const normalizedGroupSlug = normalizeGroupSlug(ev.groupSlug);
    let nextSeniorsRegistrationMode = normalizeSeniorsRegistrationMode(ev.seniorsRegistrationMode, ev.groupSlug);
    let nextSeniorsEventType = normalizeSeniorsEventType(ev.seniorsEventType || (isSeniorsEventOnlyEvent(ev) ? 'outing' : 'tee-times'));
    if (normalizedGroupSlug === 'seniors') {
      if (seniorsEventType !== undefined) {
        nextSeniorsEventType = normalizeSeniorsEventType(seniorsEventType);
        ev.seniorsEventType = nextSeniorsEventType;
      }
      if (seniorsRegistrationMode !== undefined) {
        nextSeniorsRegistrationMode = normalizeSeniorsRegistrationMode(seniorsRegistrationMode, ev.groupSlug);
        ev.seniorsRegistrationMode = nextSeniorsRegistrationMode;
        if (nextSeniorsRegistrationMode === 'event-only') {
          ev.isTeamEvent = false;
          ev.teeTimes = [];
        }
      }
      if (nextSeniorsRegistrationMode !== 'event-only') {
        ev.isTeamEvent = nextSeniorsEventType === 'regular-shotgun';
        if (ev.isTeamEvent && (!Number.isFinite(Number(ev.teamSizeMax)) || Number(ev.teamSizeMax) !== 4)) {
          ev.teamSizeMax = 4;
        }
      }
    }
    if (normalizedGroupSlug !== 'seniors' && isTeamEvent !== undefined && !isSeniorsEventOnlyEvent(ev)) ev.isTeamEvent = !!isTeamEvent;
    if (teamSizeMax !== undefined && !(normalizedGroupSlug === 'seniors' && ev.isTeamEvent)) {
      ev.teamSizeMax = Math.max(2, Math.min(4, Number(teamSizeMax || 4)));
    }
    if (weatherNeedsRefresh) {
      const weatherData = await fetchWeatherForEvent(ev);
      assignWeatherToEvent(ev, weatherData);
    }
    ev.dedupeKey = buildEventStorageDedupeKey(
      ev.date,
      ev.teeTimes,
      ev.isTeamEvent,
      ev.groupSlug,
      ev.seniorsRegistrationMode,
      ev._id,
      ev.course
    ) || undefined;
    await ev.save();
    const afterAudit = captureEventAuditSnapshot(ev);
    await logAudit(ev._id, 'update_event', 'SYSTEM', {
      ...auditContextFromEvent(ev),
      message: buildEventUpdateAuditMessage(beforeAudit, afterAudit),
      details: {
        before: beforeAudit,
        after: afterAudit,
      },
    });
    res.json(ev);
  } catch (e) { res.status(400).json({ error: e.message }); }
});

app.delete('/api/events/:id', async (req, res) => {
  if (normalizeGroupSlug(getGroupSlug(req)) === 'seniors') {
    if (requireSeniorsSiteAdminForWrite(req, res)) return;
  } else if (!isAdminDelete(req)) {
    return res.status(403).json({ error: 'Delete code required' });
  }
  const del = await Event.findOne(scopeQuery(req, { _id: req.params.id }));
  if (!del) return res.status(404).json({ error: 'Not found' });
  await archiveDeletedEvent(del, {
    deletedBy: 'SYSTEM',
    deleteSource: 'admin_delete',
    notes: 'Deleted from the event admin endpoint.',
  });
  await del.deleteOne();
  await logAudit(del._id, 'delete_event', 'SYSTEM', {
    ...auditContextFromEvent(del),
    message: `Deleted event ${del.course || 'event'} on ${fmt.dateISO(del.date) || 'the selected date'}.`,
    details: {
      teeCount: Array.isArray(del.teeTimes) ? del.teeTimes.length : 0,
      seniorsRegistrationCount: Array.isArray(del.seniorsRegistrations) ? del.seniorsRegistrations.length : 0,
    },
  });
  
  // Send response immediately
  res.json({ ok: true });
  
  // Notify subscribers about the cancellation (non-blocking)
  if (normalizeGroupSlug(del.groupSlug) === 'seniors' && !del.isTeamEvent) {
    sendTeeTimeEventLifecycleEmail(`Event Cancelled: ${del.course} (${fmt.dateISO(del.date)})`,
      frame('Golf Event Cancelled',
            `<p>The following event has been cancelled:</p>
             <p><strong>Event:</strong> ${esc(fmt.dateShortTitle(del.date))}</p>
             <p><strong>Course:</strong> ${esc(del.course||'')}</p>
             <p><strong>Date:</strong> ${esc(fmt.dateLong(del.date))}</p>
             <p>We apologize for any inconvenience.</p>${btn('View Other Events', buildGroupAwarePath(SITE_URL, del.groupSlug))}`, del.groupSlug),
      { groupSlug: del.groupSlug })
      .catch(err => console.error('Failed to send deletion emails:', err));
  }
});

app.post('/api/events/:id/request-extra-tee-time', async (req, res) => {
  try {
    const ev = await findScopedEventById(req, req.params.id);
    if (!ev) return res.status(404).json({ error: 'Not found' });

    const note = String(req.body?.note || '').trim();
    const teeCount = Array.isArray(ev.teeTimes) ? ev.teeTimes.length : 0;
    const teeLabels = (ev.teeTimes || [])
      .map((tt, idx) => {
        if (ev.isTeamEvent) return tt?.name || `Team ${idx + 1}`;
        if (!tt?.time) return `Tee ${idx + 1}`;
        return fmt.tee(tt.time);
      })
      .filter(Boolean)
      .join(', ');

    const { clubEmail, clubCcEmails, groupLabel } = await getGroupContactTargets(ev.groupSlug);
    const ccList = clubCcEmails;
    const smtpRecipients = Array.from(new Set([clubEmail, ...ccList]));
    const subj = `Request additional tee time: ${ev.course || 'Course'} ${fmt.dateISO(ev.date)} - ${groupLabel}`;
    const html = `<p>Please add an additional tee time for the event below:</p>
      <ul>
        <li><strong>Course:</strong> ${esc(ev.course || '')}</li>
        <li><strong>Date:</strong> ${esc(fmt.dateLong(ev.date))}</li>
        <li><strong>Current ${ev.isTeamEvent ? 'teams' : 'tee times'}:</strong> ${teeCount}</li>
        <li><strong>Current list:</strong> ${esc(teeLabels || 'None')}</li>
        <li><strong>Group:</strong> ${esc(groupLabel)}</li>
        <li><strong>Source:</strong> Tee Time booking app</li>
      </ul>
      ${note ? `<p><strong>Request note:</strong> ${esc(note)}</p>` : ''}
      <p>Thank you.</p>`;

    const httpRes = await sendEmailViaResendApi(clubEmail, subj, html, ccList.length ? { cc: ccList } : undefined);
    if (httpRes.ok) {
      await logAudit(ev._id, 'request_extra_tee_time', 'SYSTEM', {
        ...auditContextFromEvent(ev),
        details: { mailMethod: 'http', teeCount, noteProvided: !!note },
      });
      return res.json({ ok: true, mailMethod: 'http', to: clubEmail, cc: ccList });
    }

    const smtpRes = await sendEmail(smtpRecipients, subj, html);
    if (smtpRes && smtpRes.ok) {
      await logAudit(ev._id, 'request_extra_tee_time', 'SYSTEM', {
        ...auditContextFromEvent(ev),
        details: { mailMethod: 'smtp', teeCount, noteProvided: !!note },
      });
      return res.json({ ok: true, mailMethod: 'smtp', to: smtpRecipients });
    }

    return res.status(500).json({
      error: 'Failed to send additional tee time request',
      details: (smtpRes && smtpRes.error && smtpRes.error.message) || (httpRes.error && httpRes.error.message) || 'Unknown email error',
    });
  } catch (e) {
    console.error('[extra-tee-request] Error', { eventId: req.params.id, error: e.message });
    res.status(500).json({ error: e.message });
  }
});

app.post('/api/request-club-time', async (req, res) => {
  try {
    const requestDateRaw = String(req.body?.date || '').trim();
    const preferredTimeRaw = String(req.body?.preferredTime || '').trim();
    const requesterName = String(req.body?.requesterName || '').trim();
    const note = String(req.body?.note || '').trim();
    if (!requestDateRaw || !/^\d{4}-\d{2}-\d{2}$/.test(requestDateRaw)) {
      return res.status(400).json({ error: 'date required (YYYY-MM-DD)' });
    }
    if (!preferredTimeRaw || !/^\d{1,2}:\d{2}$/.test(preferredTimeRaw)) {
      return res.status(400).json({ error: 'preferredTime required (HH:MM)' });
    }
    const preferredMinutes = parseHHMMToMinutes(preferredTimeRaw);
    if (preferredMinutes === null) return res.status(400).json({ error: 'invalid preferredTime' });
    if (!requesterName) return res.status(400).json({ error: 'requesterName required' });

    const requestDate = asUTCDate(requestDateRaw);
    if (Number.isNaN(requestDate.getTime())) return res.status(400).json({ error: 'invalid date' });

    const { clubEmail, clubCcEmails, groupLabel } = await getGroupContactTargets(getGroupSlug(req));
    const ccList = clubCcEmails;
    const smtpRecipients = Array.from(new Set([clubEmail, ...ccList]));
    const preferredTimeText = fmt.tee(preferredTimeRaw);
    const subj = `Request additional tee time: ${fmt.dateISO(requestDate)} ${preferredTimeText} - ${groupLabel}`;
    const html = `<p>Please add an additional tee time for the date below:</p>
      <ul>
        <li><strong>Date requested:</strong> ${esc(fmt.dateLong(requestDate))}</li>
        <li><strong>Preferred time:</strong> ${esc(preferredTimeText)}</li>
        <li><strong>Requested by:</strong> ${esc(requesterName)}</li>
        <li><strong>Group:</strong> ${esc(groupLabel)}</li>
        <li><strong>Source:</strong> Monthly calendar request</li>
      </ul>
      ${note ? `<p><strong>Request note:</strong> ${esc(note)}</p>` : ''}
      <p>Thank you.</p>`;

    const httpRes = await sendEmailViaResendApi(clubEmail, subj, html, ccList.length ? { cc: ccList } : undefined);
    if (httpRes.ok) {
      return res.json({ ok: true, mailMethod: 'http', to: clubEmail, cc: ccList });
    }

    const smtpRes = await sendEmail(smtpRecipients, subj, html);
    if (smtpRes && smtpRes.ok) {
      return res.json({ ok: true, mailMethod: 'smtp', to: smtpRecipients });
    }

    return res.status(500).json({
      error: 'Failed to send club time request',
      details: (smtpRes && smtpRes.error && smtpRes.error.message) || (httpRes.error && httpRes.error.message) || 'Unknown email error',
    });
  } catch (e) {
    console.error('[calendar-club-request] Error', { error: e.message });
    return res.status(500).json({ error: e.message });
  }
});

// Remove duplicate tee-time events for the same date/time/tee-count, keeping the requested event
app.post('/api/events/:id/dedupe', async (req, res) => {
  try {
    const ev = await findScopedEventById(req, req.params.id);
    if (!ev) return res.status(404).json({ error: 'Not found' });
    if (ev.isTeamEvent) return res.status(400).json({ error: 'Dedupe only supported for tee-time events' });
    if (!Array.isArray(ev.teeTimes) || !ev.teeTimes.length || !ev.teeTimes[0].time) {
      return res.status(400).json({ error: 'Event missing tee time data' });
    }

    const baseDate = asUTCDate(ev.date);
    if (isNaN(baseDate)) return res.status(400).json({ error: 'Invalid event date' });

    const start = new Date(Date.UTC(baseDate.getUTCFullYear(), baseDate.getUTCMonth(), baseDate.getUTCDate(), 0, 0, 0));
    const end = new Date(Date.UTC(baseDate.getUTCFullYear(), baseDate.getUTCMonth(), baseDate.getUTCDate(), 23, 59, 59, 999));

    const baseTimes = (ev.teeTimes || []).map((t) => t && t.time).filter(Boolean).sort();
    const baseCount = baseTimes.length;
    if (!baseCount) return res.status(400).json({ error: 'No tee times to match' });
    const baseKey = baseTimes.join('|');

    const candidates = await Event.find(scopeQuery(req, {
      isTeamEvent: false,
      date: { $gte: start, $lte: end }
    })).sort({ createdAt: 1 });

    const matches = candidates.filter((e) => {
      const times = (e.teeTimes || []).map((t) => t && t.time).filter(Boolean).sort();
      if (times.length !== baseCount) return false;
      return times.join('|') === baseKey;
    });

    if (matches.length <= 1) {
      return res.json({ ok: true, removed: 0, keptId: ev._id, matched: matches.length });
    }

    // Prefer to keep the requested event; if not in matches, keep the earliest
    const keepId = matches.some((m) => String(m._id) === String(ev._id))
      ? ev._id
      : matches[0]._id;

    const toRemove = matches.filter((m) => String(m._id) !== String(keepId)).map((m) => m._id);
    const removedEvents = matches.filter((m) => String(m._id) !== String(keepId));
    for (const removedEvent of removedEvents) {
      await archiveDeletedEvent(removedEvent, {
        deletedBy: 'SYSTEM',
        deleteSource: 'dedupe',
        notes: `Duplicate cleanup kept event ${String(keepId)}.`,
      });
    }
    const delResult = await Event.deleteMany({ ...groupScopeFilter(ev.groupSlug), _id: { $in: toRemove } });
    await logAudit(keepId, 'dedupe_event', 'SYSTEM', {
      ...auditContextFromEvent(ev),
      message: `Removed ${removedEvents.length} duplicate event${removedEvents.length === 1 ? '' : 's'} and kept this event as the canonical record.`,
      details: {
        removedEventIds: removedEvents.map((entry) => String(entry._id)),
      },
    });
    for (const removedEvent of removedEvents) {
      await logAudit(removedEvent._id, 'delete_event', 'SYSTEM', {
        ...auditContextFromEvent(removedEvent),
        message: `Deleted duplicate event during dedupe. Kept event ${String(keepId)} as the canonical record.`,
        details: {
          keptEventId: String(keepId),
        },
      });
    }
    console.log('[dedupe] Removed duplicate events', { keepId: String(keepId), removed: delResult.deletedCount, ids: toRemove.map(String) });

    return res.json({ ok: true, keptId: keepId, removed: delResult.deletedCount, removedIds: toRemove, matched: matches.length });
  } catch (e) {
    console.error('[dedupe] Error removing duplicates', e);
    return res.status(500).json({ error: 'Failed to remove duplicates', details: e.message });
  }
});

/* tee/team, players, move endpoints remain as in your current server.js */
app.post('/api/events/:id/tee-times', async (req, res) => {
  if (requireSeniorsSiteAdminForWrite(req, res)) return;
  // Clean logging: only log errors or important info
  const ev = await findScopedEventById(req, req.params.id);
  if (!ev) {
    console.error('[tee-time] Add failed: event not found', { eventId: req.params.id });
    return res.status(404).json({ error: 'Not found' });
  }
  if (ev.isTeamEvent) {
    const slotLabel = groupedSlotNamePrefix(ev);
    // Accept optional name. If missing/blank, auto-assign the next available "Team N".
    let name = (req.body && typeof req.body.name === 'string') ? String(req.body.name).trim() : '';
    if (!name) {
      name = nextTeamNameForEvent(ev);
    } else {
      // Defensive: prevent duplicate team names (case-insensitive)
      const dup = (ev.teeTimes || []).some(t => t && t.name && String(t.name).trim().toLowerCase() === name.toLowerCase());
      if (dup) return res.status(409).json({ error: 'duplicate team name' });
    }
    // Check if all existing teams have the same time (shotgun) or different times (staggered)
    let time = null;
    if (ev.teeTimes && ev.teeTimes.length > 0) {
      const firstTime = ev.teeTimes[0].time;
      const allSameTime = ev.teeTimes.every(t => t.time === firstTime);
      if (allSameTime) {
        // Shotgun start: use same time as existing teams
        time = firstTime;
      } else {
        // Staggered start: compute next time (9 minutes after last)
        time = nextTeeTimeForEvent(ev, 9, '07:00');
      }
    } else {
      // First team being added, default to 07:00
      time = '07:00';
    }
    // Use $push to add the new team atomically
    const pushResult = await Event.findByIdAndUpdate(
      req.params.id,
      { $push: { teeTimes: { name, time, players: [] } } },
      { new: true }
    );
    console.log('[tee-time] Team added', { eventId: ev._id, teamName: name, time });
    const eventUrl = buildSiteEventUrl(ev.groupSlug, ev._id);
    sendSubscriberChangeEmail(
      `New ${slotLabel} Added: ${ev.course} (${fmt.dateISO(ev.date)})`,
      frame(`New ${slotLabel} Added!`,
        `<p>A new ${slotLabel.toLowerCase()} has been added:</p>
         <p><strong>Event:</strong> ${esc(ev.course)}</p>
         <p><strong>Date:</strong> ${esc(fmt.dateLong(ev.date))}</p>
         <p><strong>${slotLabel}:</strong> ${esc(name)}</p>
         <p>Please <a href="${eventUrl}" style="color:#166534;text-decoration:underline">click here to view this event directly</a>.</p>${btn('View Event', eventUrl)}`),
      { groupSlug: ev.groupSlug }
    ).catch(err => console.error('[tee-time] Failed to send team add email:', err));
    const added = pushResult && pushResult.teeTimes ? pushResult.teeTimes[pushResult.teeTimes.length - 1] : null;
    await logTeeTimeChange({
      groupSlug: pushResult?.groupSlug || ev.groupSlug,
      eventId: pushResult?._id,
      teeId: added?._id,
      action: 'add',
      labelAfter: added ? (added.name || '') : name,
      isTeamEvent: true,
      course: pushResult?.course,
      dateISO: fmt.dateISO(pushResult?.date),
    });
    return res.json(pushResult);
  }
  // For tee times: accept optional time. If missing, compute next time using event data.
  const { time } = req.body || {};
  let newTime = typeof time === 'string' && time.trim() ? time.trim() : null;
  if (!newTime) {
    newTime = nextTeeTimeForEvent(ev, 9, '07:00');
  }
  // Validate HH:MM and ranges
  const mTime = /^(\d{1,2}):(\d{2})$/.exec(newTime);
  if (!mTime) {
    console.error('[tee-time] Add failed: invalid time format', { eventId: ev._id, time: newTime });
    return res.status(400).json({ error: 'time required HH:MM' });
  }
  const hh = parseInt(mTime[1], 10); const mm = parseInt(mTime[2], 10);
  if (Number.isNaN(hh) || Number.isNaN(mm) || hh < 0 || hh > 23 || mm < 0 || mm > 59) {
    console.error('[tee-time] Add failed: invalid time value', { eventId: ev._id, time: newTime });
    return res.status(400).json({ error: 'invalid time' });
  }
  if (ev.teeTimes.some(t => t.time === newTime)) {
    console.error('[tee-time] Add failed: duplicate time', { eventId: ev._id, time: newTime });
    return res.status(409).json({ error: 'duplicate time' });
  }
  ev.teeTimes.push({ time: newTime, players: [] });
  ev.teeTimes.sort((a, b) => {
    const [ah, am] = a.time.split(":").map(Number);
    const [bh, bm] = b.time.split(":").map(Number);
    return ah !== bh ? ah - bh : am - bm;
  });
  await ev.save();
  console.log('[tee-time] Tee time added', { eventId: ev._id, time: newTime });
  const eventUrl = buildSiteEventUrl(ev.groupSlug, ev._id, { time: newTime });
  sendSubscriberChangeEmail(
    `New Tee Time Added: ${ev.course} (${fmt.dateISO(ev.date)})`,
    frame('New Tee Time Added!',
      `<p>A new tee time has been added:</p>
       <p><strong>Event:</strong> ${esc(ev.course)}</p>
       <p><strong>Date:</strong> ${esc(fmt.dateLong(ev.date))}</p>
       <p><strong>Tee Time:</strong> ${esc(fmt.tee(newTime))}</p>
       <p>Please <a href="${eventUrl}" style="color:#166534;text-decoration:underline">click here to view this tee time directly</a>.</p>${btn('View Event', eventUrl)}`),
    { groupSlug: ev.groupSlug }
  ).catch(err => console.error('[tee-time] Failed to send tee add email:', err));
  sendBrsTeeTimeChangeAlert('added', ev, newTime)
    .catch(err => console.error('[tee-time] Failed to send BRS tee add alert:', err));
  const added = ev.teeTimes[ev.teeTimes.length - 1];
  await logTeeTimeChange({
    groupSlug: ev.groupSlug,
    eventId: ev._id,
    teeId: added?._id,
    action: 'add',
    labelAfter: added ? (added.time || '') : newTime,
    isTeamEvent: false,
    course: ev.course,
    dateISO: fmt.dateISO(ev.date),
  });
  res.json(ev);
});


// Edit tee time or team name
app.put('/api/events/:id/tee-times/:teeId', async (req, res) => {
  if (requireSeniorsSiteAdminForWrite(req, res)) return;
  try {
    const ev = await findScopedEventById(req, req.params.id);
    if (!ev) return res.status(404).json({ error: 'Not found' });
    const tt = ev.teeTimes.id(req.params.teeId);
    if (!tt) return res.status(404).json({ error: 'tee/team not found' });
    const beforeLabel = ev.isTeamEvent ? (tt.name || '') : (tt.time || '');

    if (ev.isTeamEvent) {
      const { name } = req.body || {};
      if (!name || !name.trim()) return res.status(400).json({ error: 'name required' });
      tt.name = name.trim();
    } else {
      const { time } = req.body || {};
      if (!time || !time.trim()) return res.status(400).json({ error: 'time required' });
      const timeStr = time.trim();
      if (!/^\d{1,2}:\d{2}$/.test(timeStr)) {
        return res.status(400).json({ error: 'time must be HH:MM format' });
      }
      tt.time = timeStr;
    }
    await ev.save();
    const afterLabel = ev.isTeamEvent ? (tt.name || '') : (tt.time || '');
    await logTeeTimeChange({
      groupSlug: ev.groupSlug,
      eventId: ev._id,
      teeId: tt._id,
      action: 'update',
      labelBefore: beforeLabel,
      labelAfter: afterLabel,
      isTeamEvent: ev.isTeamEvent,
      course: ev.course,
      dateISO: fmt.dateISO(ev.date),
    });
    res.json(ev);
  } catch (e) {
    console.error('Edit tee time error:', e);
    res.status(500).json({ error: e.message });
  }
});


app.delete('/api/events/:id/tee-times/:teeId', async (req, res) => {
  if (requireSeniorsSiteAdminForWrite(req, res)) return;
  try {
    const seniorsDelete = normalizeGroupSlug(getGroupSlug(req)) === 'seniors';
    const seniorsAdminDelete = isSeniorsAdminViewRequest(req);
    const confirmedDelete = hasDeleteActionConfirmed(req);
    const hasDeleteCode = isAdminDelete(req);
    if (!seniorsDelete && !seniorsAdminDelete && !hasDeleteCode && !confirmedDelete) return res.status(403).json({ error: 'Removal confirmation required' });
    const notifyClub = String(req.query.notifyClub || '0') === '1';
    console.log('[tee-time] Remove request', {
      eventId: req.params.id,
      teeId: req.params.teeId,
      notifyClub,
      hasDeleteCode,
      confirmedDelete,
    });

    const ev = await findScopedEventById(req, req.params.id);
    if (!ev) {
      console.error('[tee-time] Remove failed: event not found', { eventId: req.params.id });
      return res.status(404).json({ error: 'Not found' });
    }

    const tt = ev.teeTimes.id(req.params.teeId);
    if (!tt) {
      console.error('[tee-time] Remove failed: tee/team not found', { eventId: req.params.id, teeId: req.params.teeId });
      return res.status(404).json({ error: 'Tee/team not found' });
    }

    const rawTime = tt.time || '';
    const teeLabel = ev.isTeamEvent ? (tt.name || 'Team') : (rawTime ? fmt.tee(rawTime) : 'Tee time');

    await archiveDeletedTeeTime(ev, tt, {
      deletedBy: 'SYSTEM',
      deleteSource: notifyClub ? 'tee_delete_notify_club' : 'tee_delete',
      notes: notifyClub ? 'Club cancellation notification requested.' : 'Deleted from tee-time admin endpoint.',
    });
    tt.deleteOne();
    await ev.save();

    // Notify subscribers (existing behavior) - fire and forget
    sendSubscriberChangeEmail(
      `${ev.isTeamEvent ? 'Team' : 'Tee Time'} Removed: ${ev.course} (${fmt.dateISO(ev.date)})`,
      frame(`${ev.isTeamEvent ? 'Team' : 'Tee Time'} Removed`,
        `<p>A ${ev.isTeamEvent ? 'team' : 'tee time'} has been removed:</p>
         <p><strong>Event:</strong> ${esc(ev.course)}</p>
         <p><strong>Date:</strong> ${esc(fmt.dateLong(ev.date))}</p>
         ${btn('View Event', buildSiteEventUrl(ev.groupSlug, ev._id))}`),
      { groupSlug: ev.groupSlug }
    ).catch(err => console.error('Failed to send tee/team removal email:', err));
    sendBrsTeeTimeChangeAlert('removed', ev, rawTime || teeLabel)
      .catch(err => console.error('[tee-time] Failed to send BRS tee removal alert:', err));

    let mailMethod = null;
    let mailError = null;
    if (notifyClub && !isBlueRidgeShadowsCourseName(ev.course)) {
      return res.status(400).json({ error: 'Club notification is only available for Blue Ridge Shadows tee times.' });
    }
    if (notifyClub) {
      const { clubEmail, clubCcEmails, groupLabel } = await getGroupContactTargets(ev.groupSlug);
      const ccList = clubCancelCcRecipientsForEvent(ev, clubCcEmails);
      const subj = `Cancel tee time: ${ev.course || 'Course'} ${fmt.dateISO(ev.date)} ${teeLabel} - ${groupLabel}`;
      const html = `<p>Please cancel the tee time below:</p>
        <ul>
          <li><strong>Course:</strong> ${esc(ev.course || '')}</li>
          <li><strong>Date:</strong> ${esc(fmt.dateLong(ev.date))}</li>
          <li><strong>Tee time:</strong> ${esc(teeLabel)}</li>
          <li><strong>Group:</strong> ${esc(groupLabel)}</li>
          <li><strong>Source:</strong> Tee Time booking app</li>
        </ul>
        <p>Please remove this tee time from your system to release it back to inventory. If already cancelled, no further action needed.</p>`;
      const cc = ccList.length ? ccList.join(',') : '';
      const httpRes = await sendEmailViaResendApi(clubEmail, subj, html, cc ? { cc } : undefined);
      if (httpRes.ok) {
        mailMethod = 'http';
        console.log('[tee-time] Club cancel email sent (HTTP)', { clubEmail, cc, subject: subj, result: httpRes });
      } else {
        console.warn('[tee-time] HTTP send failed, falling back to SMTP', httpRes.error);
        try {
          const mailRes = await sendEmail(clubEmail, subj, html, cc ? { cc } : undefined);
          mailMethod = 'smtp';
          console.log('[tee-time] Club cancel email sent (SMTP fallback)', { clubEmail, cc, subject: subj, result: mailRes });
        } catch (err) {
          mailError = err.message || 'SMTP send failed';
          console.error('Failed to send club cancel email (SMTP)', err);
        }
      }
    }

    await logTeeTimeChange({
      groupSlug: ev.groupSlug,
      eventId: ev._id,
      teeId: req.params.teeId,
      action: 'delete',
      labelBefore: teeLabel,
      labelAfter: '',
      isTeamEvent: ev.isTeamEvent,
      course: ev.course,
      dateISO: fmt.dateISO(ev.date),
      notifyClub,
      mailMethod,
      mailError,
    });

    if (mailError) {
      return res.status(500).json({ error: 'Failed to send club cancel email', details: mailError, notifyClub: true, eventId: ev._id, teeLabel });
    }

    res.json({ ok: true, notifyClub, eventId: ev._id, teeLabel, mailMethod });
  } catch (e) {
    console.error('[tee-time] Remove error', { eventId: req.params.id, teeId: req.params.teeId, error: e.message });
    res.status(500).json({ error: e.message });
  }
});

app.post('/api/events/:id/tee-times/:teeId/players', validateBody(validateAddPlayer), async (req, res) => {
  const { name, asFifth } = req.body || {};
  if (!name) return res.status(400).json({ error: 'name required' });
  const trimmedName = String(name).trim();
  if (!trimmedName) return res.status(400).json({ error: 'name cannot be empty' });

  const ev = await findScopedEventById(req, req.params.id);
  if (!ev) return res.status(404).json({ error: 'Not found' });
  const tt = ev.teeTimes.id(req.params.teeId);
  if (!tt) return res.status(404).json({ error: 'tee time not found' });
  if (!Array.isArray(tt.players)) tt.players = [];

  // Extra logging for debugging
  console.log('[add_player] Request', {
    eventId: req.params.id,
    teeId: req.params.teeId,
    playerName: trimmedName,
    eventDate: ev.date,
    now: new Date().toISOString(),
    players: tt.players.map(p => p.name)
  });
  // Special test message for event on 11/16
  const eventDateStr = ev.date instanceof Date ? ev.date.toISOString().slice(0,10) : String(ev.date).slice(0,10);
  if (eventDateStr === '2025-11-16') {
    console.log('[add_player][TEST] Adding player to 11/16 event:', trimmedName);
  }

  const capacity = evaluateSlotAddition(ev, tt, { allowFifth: !!asFifth });
  if (!capacity.ok) {
    return res.status(400).json({ error: capacity.error, canAddFifth: capacity.canAddFifth });
  }
  const maxSize = slotCapacityForEvent(ev);

  let seniorsGolfer = null;
  if (normalizeGroupSlug(ev.groupSlug) === 'seniors') {
    seniorsGolfer = await resolveSeniorsRosterGolferByName(trimmedName) || buildManualSeniorsGolferRecord(trimmedName);
  }

  // Anti-chaos check: duplicate name prevention
  if (isDuplicatePlayerName(ev, trimmedName)) {
    return res.status(409).json({ error: 'duplicate player name', message: 'A player with this name already exists. Use a nickname (e.g., "John S" or "John 2").' });
  }

  tt.players.push(buildPlayerEntry(trimmedName, { isFifth: capacity.asFifth }));
  normalizeSlotFifthState(ev, tt);
  await ev.save();

  // Audit log
  const addPlayerTeeLabel = getTeeLabel(ev, tt._id);
  await logAudit(ev._id, 'add_player', trimmedName, {
    ...auditContextFromEvent(ev),
    teeId: tt._id,
    teeLabel: addPlayerTeeLabel,
    details: { asFifth: !!capacity.asFifth }
  });

  if (seniorsGolfer && seniorsGolfer.email) {
    sendSeniorsRegistrationConfirmationEmail(ev, seniorsGolfer, addPlayerTeeLabel)
      .catch((err) => console.error('[add_player] Failed to send Seniors confirmation email:', err));
  }

  // Send notification email only if notifications are enabled
  if (ev.notificationsEnabled !== false) {
    const teeLabel = addPlayerTeeLabel;
    sendSubscriberChangeEmail(
      `Player Added: ${ev.course} (${fmt.dateISO(ev.date)})`,
      frame('Player Signed Up!',
        `<p><strong>${esc(trimmedName)}</strong> has signed up for:</p>
         <p><strong>Event:</strong> ${esc(ev.course)}</p>
         <p><strong>Date:</strong> ${esc(fmt.dateLong(ev.date))}</p>
         <p><strong>${ev.isTeamEvent ? 'Team' : 'Tee Time'}:</strong> ${esc(teeLabel)}</p>
         ${btn('View Event', buildSiteEventUrl(ev.groupSlug, ev._id))}`),
      { groupSlug: ev.groupSlug }
    ).catch(err => console.error('Failed to send player add email:', err));

    const oneSpotLeft = tt.players.length === Math.max(1, maxSize - 1);
    if (oneSpotLeft) {
      sendSubscriberChangeEmail(
        `Need 1 More: ${ev.course} (${fmt.dateISO(ev.date)})`,
        frame('One Spot Left',
          `<p>${esc(ev.isTeamEvent ? 'Team' : 'Tee time')} <strong>${esc(teeLabel)}</strong> has just one spot left.</p>
           <p><strong>Event:</strong> ${esc(ev.course)}</p>
           <p><strong>Date:</strong> ${esc(fmt.dateLong(ev.date))}</p>
           <p>Last call if you want in.</p>
           ${btn('Join This Event', buildSiteEventUrl(ev.groupSlug, ev._id))}`),
        { groupSlug: ev.groupSlug }
      ).catch(err => console.error('Failed to send one-spot-left email:', err));
    }
  }

  res.json(ev);
});
app.delete('/api/events/:id/tee-times/:teeId/players/:playerId', async (req, res) => {
  try {
    const seniorsDelete = normalizeGroupSlug(getGroupSlug(req)) === 'seniors';
    const seniorsAdminDelete = isSeniorsAdminViewRequest(req);
    const confirmedDelete = hasDeleteActionConfirmed(req);
    if (!seniorsDelete && !seniorsAdminDelete && !isAdminDelete(req) && !confirmedDelete) return res.status(403).json({ error: 'Removal confirmation required' });
    const ev = await findScopedEventById(req, req.params.id);
    if (!ev) return res.status(404).json({ error: 'Not found' });
    const tt = ev.teeTimes.id(req.params.teeId);
    if (!tt) return res.status(404).json({ error: 'tee/team not found' });
    if (!Array.isArray(tt.players)) tt.players = [];
    const idx = tt.players.findIndex(p => String(p._id) === String(req.params.playerId));
    if (idx === -1) return res.status(404).json({ error: 'player not found' });

    // Extra logging for debugging
    const playerName = tt.players[idx].name;
    const teeLabel = getTeeLabel(ev, tt._id);
    console.log('[remove_player] Request', {
      eventId: req.params.id,
      teeId: req.params.teeId,
      playerId: req.params.playerId,
      playerName,
      eventDate: ev.date,
      now: new Date().toISOString(),
      players: tt.players.map(p => p.name)
    });
    // Special test message for event on 11/16
    const eventDateStr = ev.date instanceof Date ? ev.date.toISOString().slice(0,10) : String(ev.date).slice(0,10);
    if (eventDateStr === '2025-11-16') {
      console.log('[remove_player][TEST] Removing player from 11/16 event:', playerName);
    }

    tt.players.splice(idx, 1);
    normalizeSlotFifthState(ev, tt);
    await ev.save();

    // Audit log
    await logAudit(ev._id, 'remove_player', playerName, {
      ...auditContextFromEvent(ev),
      teeId: tt._id,
      teeLabel,
    });

    if (ev.notificationsEnabled !== false) {
      sendSubscriberChangeEmail(
        `Player Removed: ${ev.course} (${fmt.dateISO(ev.date)})`,
        frame('Player Removed',
          `<p><strong>${esc(playerName)}</strong> has been removed from:</p>
           <p><strong>Event:</strong> ${esc(ev.course)}</p>
           <p><strong>Date:</strong> ${esc(fmt.dateLong(ev.date))}</p>
           <p><strong>${ev.isTeamEvent ? 'Team' : 'Tee Time'}:</strong> ${esc(teeLabel)}</p>
           ${btn('View Event', buildSiteEventUrl(ev.groupSlug, ev._id))}`),
        { groupSlug: ev.groupSlug }
      ).catch(err => console.error('Failed to send player removal email:', err));
    }

    return res.json(ev);
  } catch (e) {
    return res.status(500).json({ error: e.message });
  }
});
app.post('/api/events/:id/move-player', async (req, res) => {
  const { fromTeeId, toTeeId, playerId, asFifth } = req.body || {};
  if (!fromTeeId || !toTeeId || !playerId) return res.status(400).json({ error: 'fromTeeId, toTeeId, playerId required' });
  const ev = await findScopedEventById(req, req.params.id);
  if (!ev) return res.status(404).json({ error: 'Not found' });
  const fromTT = ev.teeTimes.id(fromTeeId);
  const toTT = ev.teeTimes.id(toTeeId);
  if (!fromTT || !toTT) return res.status(404).json({ error: 'tee time not found' });
  if (!Array.isArray(fromTT.players)) fromTT.players = [];
  if (!Array.isArray(toTT.players)) toTT.players = [];
  const idx = fromTT.players.findIndex(p => String(p._id) === String(playerId));
  if (idx === -1) return res.status(404).json({ error: 'player not found' });
  normalizeSlotFifthState(ev, fromTT);
  normalizeSlotFifthState(ev, toTT);
  const movingPlayer = fromTT.players[idx];
  const capacity = evaluateSlotAddition(ev, toTT, {
    allowFifth: !!asFifth,
    ignorePlayerId: movingPlayer && movingPlayer.isFifth ? movingPlayer._id : null
  });
  if (!capacity.ok) return res.status(400).json({ error: capacity.error, canAddFifth: capacity.canAddFifth });
  
  const [player] = fromTT.players.splice(idx, 1);
  const playerName = player.name;
  
  // Anti-chaos check: ensure player isn't already on another tee (shouldn't happen, but defensive)
  const conflict = isPlayerOnAnotherTee(ev, playerName, toTeeId);
  if (conflict.found) {
    // Roll back the splice
    fromTT.players.splice(idx, 0, player);
    return res.status(409).json({ error: 'player conflict', message: `${playerName} is already on ${conflict.teeName}` });
  }
  
  normalizeSlotFifthState(ev, fromTT);
  toTT.players.push(buildPlayerEntry(playerName, { checkedIn: !!player.checkedIn, isFifth: capacity.asFifth }));
  normalizeSlotFifthState(ev, toTT);
  await ev.save();
  
  // Audit log
  await logAudit(ev._id, 'move_player', playerName, {
    ...auditContextFromEvent(ev),
    fromTeeId: fromTT._id,
    toTeeId: toTT._id,
    fromTeeLabel: getTeeLabel(ev, fromTT._id),
    toTeeLabel: getTeeLabel(ev, toTT._id),
    details: { asFifth: !!capacity.asFifth }
  });
  
  res.json(ev);
});

app.post('/api/events/:id/tee-times/:teeId/players/:playerId/check-in', async (req, res) => {
  try {
    const checkedIn = req.body && req.body.checkedIn;
    if (typeof checkedIn !== 'boolean') return res.status(400).json({ error: 'checkedIn boolean required' });

    const ev = await findScopedEventById(req, req.params.id);
    if (!ev) return res.status(404).json({ error: 'Not found' });
    const tt = ev.teeTimes.id(req.params.teeId);
    if (!tt) return res.status(404).json({ error: 'tee/team not found' });
    if (!Array.isArray(tt.players)) tt.players = [];
    const p = tt.players.id(req.params.playerId);
    if (!p) return res.status(404).json({ error: 'player not found' });

    p.checkedIn = checkedIn;
    await ev.save();

    await logAudit(ev._id, checkedIn ? 'check_in_player' : 'undo_check_in_player', p.name, {
      ...auditContextFromEvent(ev),
      teeId: tt._id,
      teeLabel: getTeeLabel(ev, tt._id)
    });

    res.json(ev);
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

app.post('/api/events/:id/tee-times/:teeId/check-in-all', async (req, res) => {
  try {
    const checkedIn = req.body && req.body.checkedIn;
    if (typeof checkedIn !== 'boolean') return res.status(400).json({ error: 'checkedIn boolean required' });

    const ev = await findScopedEventById(req, req.params.id);
    if (!ev) return res.status(404).json({ error: 'Not found' });
    const tt = ev.teeTimes.id(req.params.teeId);
    if (!tt) return res.status(404).json({ error: 'tee/team not found' });
    if (!Array.isArray(tt.players) || !tt.players.length) return res.status(400).json({ error: 'no players in slot' });

    for (const p of tt.players) p.checkedIn = checkedIn;
    await ev.save();

    await logAudit(ev._id, checkedIn ? 'bulk_check_in' : 'bulk_clear_check_in', 'ALL_PLAYERS', {
      ...auditContextFromEvent(ev),
      teeId: tt._id,
      teeLabel: getTeeLabel(ev, tt._id),
      details: { playerCount: tt.players.length }
    });

    res.json(ev);
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

app.post('/api/events/:id/pairings/suggest', async (req, res) => {
  try {
    const ev = await findScopedEventById(req, req.params.id, { lean: true });
    if (!ev) return res.status(404).json({ error: 'Not found' });
    const suggestion = await buildPairingSuggestion(ev);
    res.json({
      eventId: String(ev._id),
      course: ev.course,
      date: ev.date,
      groupSize: suggestion.groupSize,
      totalPlayers: suggestion.totalPlayers,
      groups: suggestion.groups,
      unassignedPlayers: suggestion.unassignedPlayers
    });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

app.post('/api/events/:id/pairings/apply', async (req, res) => {
  try {
    const ev = await findScopedEventById(req, req.params.id);
    if (!ev) return res.status(404).json({ error: 'Not found' });

    const suggestion = await buildPairingSuggestion(ev);
    const requestedGroups = Array.isArray(req.body?.groups) ? req.body.groups : suggestion.groups;
    if (!requestedGroups.length) return res.status(400).json({ error: 'No suggested groups to apply' });

    const maxSize = ev.isTeamEvent ? (ev.teamSizeMax || 4) : 4;
    const playerIndex = new Map();
    for (const tt of (ev.teeTimes || [])) {
      for (const p of (tt.players || [])) {
        playerIndex.set(String(p._id), String(p.name || '').trim());
      }
    }

    const seenPlayerIds = new Set();
    const materialized = [];
    for (let i = 0; i < requestedGroups.length; i++) {
      const group = requestedGroups[i] || {};
      const ids = Array.isArray(group.playerIds)
        ? group.playerIds
        : Array.isArray(group.players)
          ? group.players.map((p) => p && (p.playerId || p._id)).filter(Boolean)
          : [];
      if (!ids.length) continue;
      if (ids.length > maxSize) return res.status(400).json({ error: `Group ${i + 1} exceeds max size (${maxSize})` });
      const names = [];
      for (const rawId of ids) {
        const id = String(rawId);
        if (!playerIndex.has(id)) return res.status(400).json({ error: `Unknown player in group ${i + 1}` });
        if (seenPlayerIds.has(id)) return res.status(400).json({ error: `Duplicate player assignment in group ${i + 1}` });
        seenPlayerIds.add(id);
        names.push(playerIndex.get(id));
      }
      materialized.push({ teeId: group.teeId ? String(group.teeId) : null, names });
    }

    for (const tt of (ev.teeTimes || [])) tt.players = [];

    let nextGeneratedTime = nextTeeTimeForEvent(ev, 9, '07:00');
    for (let i = 0; i < materialized.length; i++) {
      const group = materialized[i];
      let target = null;
      if (group.teeId) target = ev.teeTimes.id(group.teeId);
      if (!target) target = ev.teeTimes[i];

      if (!target) {
        const slotPayload = ev.isTeamEvent
          ? { name: `Team ${i + 1}`, players: [] }
          : { time: nextGeneratedTime, players: [] };
        ev.teeTimes.push(slotPayload);
        target = ev.teeTimes[ev.teeTimes.length - 1];
        if (!ev.isTeamEvent) {
          const tempEvent = { teeTimes: [{ time: nextGeneratedTime }] };
          nextGeneratedTime = nextTeeTimeForEvent(tempEvent, 9, nextGeneratedTime);
        }
      }

      target.players = group.names.map((name) => ({ name, checkedIn: false }));
      if (ev.isTeamEvent && !target.name) target.name = `Team ${i + 1}`;
      if (!ev.isTeamEvent && !target.time) target.time = nextTeeTimeForEvent(ev, 9, '07:00');
    }

    await ev.save();
    await logAudit(ev._id, 'apply_pairings', 'SYSTEM', {
      ...auditContextFromEvent(ev),
      details: {
        groupCount: materialized.length,
        playerCount: materialized.reduce((sum, group) => sum + group.names.length, 0),
      },
    });
    res.json(ev);
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

app.post('/api/events/:id/player-layout', async (req, res) => {
  try {
    const ev = await findScopedEventById(req, req.params.id);
    if (!ev) return res.status(404).json({ error: 'Not found' });
    if (ev.isTeamEvent) return res.status(400).json({ error: 'Player randomization is only available for tee-time events.' });

    const requestedGroups = Array.isArray(req.body?.groups) ? req.body.groups : [];
    const maybeList = Array.isArray(req.body?.maybeList)
      ? req.body.maybeList.map((name) => String(name || '').trim()).filter(Boolean)
      : Array.isArray(ev.maybeList) ? ev.maybeList : [];
    const action = String(req.body?.action || '').trim().toLowerCase();
    const includeInterested = req.body?.includeInterested === true;

    if (!requestedGroups.length) {
      return res.status(400).json({ error: 'No player groups supplied.' });
    }

    const teeMap = new Map((ev.teeTimes || []).map((tt) => [String(tt._id), tt]));
    const originalPlayerIds = new Set();
    for (const tt of (ev.teeTimes || [])) {
      for (const player of (tt.players || [])) {
        originalPlayerIds.add(String(player && player._id));
      }
    }

    for (const tt of (ev.teeTimes || [])) tt.players = [];

    for (const group of requestedGroups) {
      const teeId = String(group && group.teeId || '').trim();
      const target = teeMap.get(teeId);
      if (!target) return res.status(400).json({ error: 'Unknown tee time in player layout.' });
      const requestedPlayers = Array.isArray(group && group.players) ? group.players : [];
      if (requestedPlayers.length > 5) return res.status(400).json({ error: 'A tee time cannot have more than 5 players.' });
      target.players = requestedPlayers.map((player, index) => {
        const name = String(player && player.name || '').trim();
        if (!name) throw new Error('Player name is required for randomized layout.');
        const playerId = String(player && (player.playerId || player._id) || '').trim();
        if (playerId && !originalPlayerIds.has(playerId)) {
          throw new Error(`Unknown player id in randomized layout: ${playerId}`);
        }
        const payload = {
          name,
          checkedIn: !!(player && player.checkedIn),
          isFifth: !!(player && player.isFifth) || index >= 4,
        };
        if (playerId) payload._id = playerId;
        return payload;
      });
    }

    ev.maybeList = maybeList;
    await ev.save();

    const totalPlayers = (ev.teeTimes || []).reduce((sum, tt) => sum + ((tt.players || []).length), 0);
    const auditAction = action === 'revert' ? 'revert_randomized_players' : 'apply_randomized_players';
    const auditMessage = action === 'revert'
      ? 'Restored the previous tee-time player order.'
      : `Randomized tee-time players${includeInterested ? ' including interested golfers' : ''}.`;
    await logAudit(ev._id, auditAction, 'SYSTEM', {
      ...auditContextFromEvent(ev),
      message: auditMessage,
      details: {
        teeCount: Array.isArray(ev.teeTimes) ? ev.teeTimes.length : 0,
        playerCount: totalPlayers,
        maybeCount: Array.isArray(ev.maybeList) ? ev.maybeList.length : 0,
        includeInterested,
      },
    });

    res.json(ev);
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

app.post('/api/events/:id/skins-pops/randomize', async (req, res) => {
  try {
    if (!isMainSiteAdminRequest(req)) return res.status(403).json({ error: 'Admin code required' });
    const ev = await findScopedEventById(req, req.params.id);
    if (!ev) return res.status(404).json({ error: 'Not found' });
    if (!weekendGameEligibleEvent(ev)) {
      return res.status(400).json({ error: 'Skins Pops are available only for main-group tee-time events.' });
    }
    const unlockAt = skinsPopsUnlockAt(ev);
    if (!unlockAt) {
      return res.status(400).json({ error: 'Event is missing valid tee-time data.' });
    }
    const now = new Date();
    if (!SKINS_POPS_FORCE_READY && now.getTime() < unlockAt.getTime()) {
      return res.status(400).json({
        error: `Skins Pops become available ${unlockAt.toLocaleString('en-US', { timeZone: LOCAL_TZ, month: 'short', day: 'numeric', hour: 'numeric', minute: '2-digit' })}${LOCAL_TZ === 'America/New_York' ? ' ET' : ''}`,
        availableAt: unlockAt.toISOString(),
      });
    }

    ev.skinsPops = buildWeekendSkinsPopsDraw();
    await ev.save();
    await logAudit(ev._id, 'randomize_skins_pops', 'SKINS_POPS', {
      ...auditContextFromEvent(ev),
      details: {
        sharedHoles: ev.skinsPops && ev.skinsPops.sharedHoles || [],
        bonusHoles: ev.skinsPops && ev.skinsPops.bonusHoles || [],
      },
    });
    res.json({ ok: true, event: ev });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

app.delete('/api/events/:id/skins-pops', async (req, res) => {
  try {
    if (!isMainSiteAdminRequest(req)) return res.status(403).json({ error: 'Admin code required' });
    const ev = await findScopedEventById(req, req.params.id);
    if (!ev) return res.status(404).json({ error: 'Not found' });
    const hasDraw = Array.isArray(ev.skinsPops && ev.skinsPops.sharedHoles) && ev.skinsPops.sharedHoles.length
      || Array.isArray(ev.skinsPops && ev.skinsPops.bonusHoles) && ev.skinsPops.bonusHoles.length
      || !!(ev.skinsPops && ev.skinsPops.generatedAt);
    if (!hasDraw) {
      return res.status(400).json({ error: 'No Skins Pops draw is saved for this event.' });
    }
    ev.skinsPops = { sharedHoles: [], bonusHoles: [], generatedAt: null };
    await ev.save();
    await logAudit(ev._id, 'reset_skins_pops', 'SKINS_POPS', {
      ...auditContextFromEvent(ev),
    });
    res.json({ ok: true, event: ev });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

/* ---------------- Golf Course API ---------------- */
const GOLF_API_KEY = process.env.GOLF_API_KEY || '';
const GOLF_API_KEY_BACKUP = process.env.GOLF_API_KEY_BACKUP || '';
const GOLF_API_BASE = 'https://api.golfcourseapi.com/v1';
const LOCAL_GOLF_COURSES = Object.freeze([
  {
    id: 'custom-1',
    name: 'Blue Ridge Shadows Golf Club',
    city: 'Front Royal',
    state: 'VA',
    latitude: 38.9492339,
    longitude: -78.1649919,
    phone: '(540) 631-9661',
    website: 'https://blueridgeshadows.com',
    holes: 18,
    par: 72
  },
  {
    id: 'custom-2',
    name: 'Caverns Country Club Resort',
    city: 'Luray',
    state: 'VA',
    phone: '(540) 743-7111',
    website: 'https://cavernscc.com',
    holes: 18,
    par: 72
  },
  {
    id: 'custom-3',
    name: 'Rock Harbor Golf Club',
    city: 'Winchester',
    state: 'VA',
    phone: '(540) 722-7111',
    website: 'https://www.rockharborgolf.com',
    holes: 18,
    par: 72
  },
  {
    id: 'custom-4',
    name: 'Shenandoah Valley Golf Club',
    city: 'Front Royal',
    state: 'VA',
    phone: '(540) 636-4653',
    website: 'https://svgcgolf.com',
    holes: 27,
    par: 72
  },
  {
    id: 'custom-5',
    name: 'Shenvalee Golf Resort',
    city: 'New Market',
    state: 'VA',
    phone: '(540) 740-3181',
    website: 'https://shenvalee.com',
    holes: 27,
    par: 72
  },
  {
    id: 'custom-6',
    name: 'The Club at Ironwood',
    city: 'Greenville',
    state: 'VA',
    phone: '(540) 337-1234',
    website: null,
    holes: 18,
    par: 72
  },
  {
    id: 'custom-7',
    name: 'World Tour Golf Links',
    city: 'Myrtle Beach',
    state: 'SC',
    phone: null,
    website: null,
    holes: 18,
    par: 72
  },
  {
    id: 'custom-8',
    name: 'Wild Wing Avocet',
    city: 'Conway',
    state: 'SC',
    phone: null,
    website: null,
    holes: 18,
    par: 72
  },
  {
    id: 'custom-9',
    name: 'MB National Kings North',
    city: 'Myrtle Beach',
    state: 'SC',
    phone: null,
    website: null,
    holes: 18,
    par: 72
  },
  {
    id: 'custom-10',
    name: 'River Hills',
    city: 'Little River',
    state: 'SC',
    phone: null,
    website: null,
    holes: 18,
    par: 72
  },
  {
    id: 'custom-11',
    name: 'Long Bay',
    city: 'Longs',
    state: 'SC',
    phone: null,
    website: null,
    holes: 18,
    par: 72
  },
  {
    id: 'custom-12',
    name: 'Cacapon Resort Golf Course',
    city: 'Berkeley Springs',
    state: 'WV',
    phone: null,
    website: null,
    holes: 18,
    par: 72
  },
  {
    id: 'custom-13',
    name: 'Bryce Resort',
    city: 'Basye',
    state: 'VA',
    phone: null,
    website: null,
    holes: 18,
    par: 71
  },
  {
    id: 'custom-14',
    name: 'Lakeview Golf Club',
    city: 'Harrisonburg',
    state: 'VA',
    phone: null,
    website: null,
    holes: 18,
    par: 72
  },
  {
    id: 'custom-15',
    name: 'Spotswood Country Club',
    city: 'Harrisonburg',
    state: 'VA',
    phone: null,
    website: null,
    holes: 18,
    par: 72
  },
  {
    id: 'custom-16',
    name: 'Orchard Creek',
    city: 'Waynesboro',
    state: 'VA',
    phone: null,
    website: null,
    holes: 18,
    par: 72
  }
]);

function buildLocalGolfCourses() {
  return LOCAL_GOLF_COURSES.map((course) => ({ ...course }));
}

function normalizeCourseLookupName(value = '') {
  return String(value || '')
    .trim()
    .toLowerCase()
    .replace(/&/g, 'and')
    .replace(/[^a-z0-9]+/g, ' ')
    .replace(/\b(golf club|golf course|country club|club|resort)\b/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function findLocalCourseByName(courseName = '') {
  const lookup = normalizeCourseLookupName(courseName);
  if (!lookup) return null;
  const courses = buildLocalGolfCourses();
  return courses.find((course) => {
    const candidate = normalizeCourseLookupName(course && course.name);
    return candidate === lookup || candidate.includes(lookup) || lookup.includes(candidate);
  }) || null;
}

function enrichCourseInfo(courseName = '', existingCourseInfo = {}) {
  const normalizedExisting = normalizeCourseInfo(existingCourseInfo || {});
  const localCourse = findLocalCourseByName(courseName);
  if (!localCourse) return normalizedExisting;
  return normalizeCourseInfo({
    ...localCourse,
    ...normalizedExisting,
  });
}

// Helper: Try API request with fallback to backup key
async function fetchGolfAPI(url, primaryKey = GOLF_API_KEY, backupKey = GOLF_API_KEY_BACKUP) {
  // Try primary key first
  if (primaryKey) {
    try {
      const response = await fetch(url, {
        headers: { 'Authorization': `Key ${primaryKey}` }
      });
      
      // If successful or non-auth error, return it
      if (response.ok || (response.status !== 401 && response.status !== 403 && response.status !== 429)) {
        return { response, keyUsed: 'primary' };
      }
      
      console.warn(`Golf API primary key failed with ${response.status}, trying backup...`);
    } catch (err) {
      console.warn('Golf API primary key request failed:', err.message);
    }
  }
  
  // Try backup key if primary failed with auth/rate limit error
  if (backupKey) {
    try {
      const response = await fetch(url, {
        headers: { 'Authorization': `Key ${backupKey}` }
      });
      return { response, keyUsed: 'backup' };
    } catch (err) {
      console.error('Golf API backup key also failed:', err.message);
      throw err;
    }
  }
  
  // No backup key available
  throw new Error('Golf API request failed and no backup key available');
}

// Validate course data consistency
function validateCourseData(course) {
  const issues = [];
  
  // Check if city/state matches common patterns for the course name
  if (course.name && course.city) {
    const nameLower = course.name.toLowerCase();
    const cityLower = (course.city || '').toLowerCase();
    
    // Flag if course name mentions a location that doesn't match city/state
    const locationKeywords = ['richmond', 'virginia beach', 'norfolk', 'roanoke', 'front royal', 'luray', 'new market'];
    for (const keyword of locationKeywords) {
      if (nameLower.includes(keyword) && !cityLower.includes(keyword)) {
        issues.push(`Course name mentions "${keyword}" but city is "${course.city}"`);
      }
    }
  }
  
  // Check if phone format is valid
  if (course.phone && !/^\(?\d{3}\)?[\s.-]?\d{3}[\s.-]?\d{4}$/.test(course.phone)) {
    issues.push(`Invalid phone format: ${course.phone}`);
  }
  
  // Check for missing critical data
  if (!course.name) issues.push('Missing course name');
  if (!course.city && !course.state) issues.push('Missing location data');
  
  return {
    isValid: issues.length === 0,
    issues
  };
}

app.get('/api/golf-courses/list', async (req, res) => {
  const localCourses = buildLocalGolfCourses();

  // If no API keys, return only local courses
  if (!GOLF_API_KEY && !GOLF_API_KEY_BACKUP) {
    return res.json(localCourses);
  }
  
  try {
    // Allow state and limit to be passed as query params for testing
    const limit = Math.min(parseInt(req.query.limit) || 100, 200);
    
    // Use search endpoint - the API searches by course name, so cast a wide net
    // Search for just "golf" to get many results, then filter by VA state
    const url = `${GOLF_API_BASE}/search?search_query=golf`;
    const { response, keyUsed } = await fetchGolfAPI(url);
    
    if (!response.ok) {
      throw new Error(`Golf API error: ${response.status} (used ${keyUsed} key)`);
    }
    
    console.log(`Golf API courses loaded using ${keyUsed} key`);
    
    const data = await response.json();
    const courses = (data.courses || [])
      .filter(c => {
        // Filter for Virginia state courses only
        const courseState = c.location?.state;
        return courseState && courseState.toUpperCase() === 'VA';
      })
      .filter(c => c.club_name || c.course_name) // Only courses with names
      .sort((a, b) => {
        const nameA = a.club_name || a.course_name || '';
        const nameB = b.club_name || b.course_name || '';
        return nameA.localeCompare(nameB);
      })
      .slice(0, limit) // Limit after sorting
      .map(c => {
        const latitude = toLatitude(c.location?.latitude ?? c.location?.lat);
        const longitude = toLongitude(c.location?.longitude ?? c.location?.lon ?? c.location?.lng);
        const course = {
          id: c.id,
          name: c.club_name || c.course_name || 'Unknown',
          city: c.location?.city || null,
          state: c.location?.state || null,
          address: c.location?.address || c.location?.address_1 || c.location?.street || null,
          phone: null, // API doesn't provide phone
          website: null, // API doesn't provide website
          holes: 18, // Default, API doesn't provide this in search
          par: null, // API doesn't provide this in search
          latitude,
          longitude
        };
        
        // Validate course data and log issues
        const validation = validateCourseData(course);
        if (!validation.isValid) {
          console.warn(`[Golf API] Data quality issue for "${course.name}":`, validation.issues);
        }
        
        return course;
      });
    
    // Combine local courses (first) with API courses
    // Filter out duplicates by name (case-insensitive)
    const localNames = new Set(localCourses.map(c => c.name.toLowerCase()));
    const apiCoursesFiltered = courses.filter(c => !localNames.has(c.name.toLowerCase()));
    
    const combinedCourses = [...localCourses, ...apiCoursesFiltered];
    res.json(combinedCourses);
  } catch (e) {
    console.error('Golf course list error:', e);
    // Return only local courses on error
    res.json(localCourses);
  }
});

// Search golf courses by query string
app.get('/api/golf-courses/search', async (req, res) => {
  const query = req.query.q || '';
  
  const localCourses = buildLocalGolfCourses();
  
  if (!query || query.length < 2) {
    // Return local courses for short/empty queries
    return res.json(localCourses);
  }
  
  // Filter local courses by query
  const queryLower = query.toLowerCase();
  const matchingLocal = localCourses.filter(c => 
    c.name.toLowerCase().includes(queryLower) ||
    (c.city && c.city.toLowerCase().includes(queryLower))
  );
  
  // If no API keys, return only local matches
  if (!GOLF_API_KEY && !GOLF_API_KEY_BACKUP) {
    return res.json(matchingLocal);
  }
  
  try {
    // Search API with the query
    const url = `${GOLF_API_BASE}/search?search_query=${encodeURIComponent(query)}`;
    const { response, keyUsed } = await fetchGolfAPI(url);
    
    if (!response.ok) {
      throw new Error(`Golf API error: ${response.status}`);
    }
    
    console.log(`Golf API search for "${query}" using ${keyUsed} key`);
    
    const data = await response.json();
    console.log(`  API returned ${data.courses ? data.courses.length : 0} total courses`);
    
    const apiCourses = (data.courses || [])
      .filter(c => {
        const courseState = c.location?.state;
        return courseState && courseState.toUpperCase() === 'VA';
      })
      .filter(c => c.club_name || c.course_name)
      .slice(0, 50) // Limit API results
      .map(c => ({
        id: c.id,
        name: c.club_name || c.course_name || 'Unknown',
        city: c.location?.city || null,
        state: c.location?.state || null,
        address: c.location?.address || c.location?.address_1 || c.location?.street || null,
        phone: null,
        website: null,
        holes: 18,
        par: null,
        latitude: toLatitude(c.location?.latitude ?? c.location?.lat),
        longitude: toLongitude(c.location?.longitude ?? c.location?.lon ?? c.location?.lng)
      }));
    
    console.log(`  Filtered to ${apiCourses.length} VA courses`);
    console.log(`  Local matches: ${matchingLocal.length}`);
    
    // Combine local matches first, then API results
    const localNames = new Set(matchingLocal.map(c => c.name.toLowerCase()));
    const apiFiltered = apiCourses.filter(c => !localNames.has(c.name.toLowerCase()));
    
    const results = [...matchingLocal, ...apiFiltered];
    console.log(`  Returning ${results.length} total courses (${matchingLocal.length} local + ${apiFiltered.length} API)`);
    res.json(results);
  } catch (e) {
    console.error('Golf course search error:', e);
    console.log(`  Returning ${matchingLocal.length} local matches only (error fallback)`);
    res.json(matchingLocal); // Return local matches on error
  }
});

/* ---------------- Weather ---------------- */
// Refresh weather for an event
app.post('/api/events/:id/weather', async (req, res) => {
  if (requireSeniorsSiteAdminForWrite(req, res)) return;
  try {
    const ev = await findScopedEventById(req, req.params.id);
    if (!ev) return res.status(404).json({ error: 'Not found' });
    ev.courseInfo = enrichCourseInfo(ev.course, ev.courseInfo || {});
    const weatherData = await fetchWeatherForEvent(ev);
    assignWeatherToEvent(ev, weatherData);
    
    await ev.save();
    await logAudit(ev._id, 'refresh_weather', 'SYSTEM', {
      ...auditContextFromEvent(ev),
      details: {
        condition: ev.weather && ev.weather.condition || null,
        temp: ev.weather && ev.weather.temp || null,
      },
    });
    res.json(ev);
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// Refresh weather for all events
app.post('/api/events/weather/refresh-all', async (req, res) => {
  if (requireSeniorsSiteAdminForWrite(req, res)) return;
  try {
    const events = await Event.find(scopeQuery(req));
    let updated = 0;
    let failed = 0;
    let errors = [];
    for (const ev of events) {
      try {
        if (!ev.date || !(ev.date instanceof Date) || isNaN(ev.date.getTime())) {
          failed++;
          errors.push({ eventId: ev._id, date: ev.date, reason: 'Missing or invalid event date' });
          console.error('Weather refresh skipped for event', ev._id, 'due to missing/invalid date:', ev.date);
          continue;
        }
        ev.courseInfo = enrichCourseInfo(ev.course, ev.courseInfo || {});
        const weatherData = await fetchWeatherForEvent(ev);
        assignWeatherToEvent(ev, weatherData);
        await ev.save();
        if (weatherData.success) updated++;
        else {
          failed++;
          errors.push({ eventId: ev._id, date: ev.date, reason: weatherData.description || 'Unknown error' });
        }
      } catch (err) {
        failed++;
        errors.push({ eventId: ev._id, date: ev.date, reason: err.message });
        console.error('Weather refresh failed for event', ev._id, err);
      }
    }
    res.json({ ok: true, updated, failed, total: events.length, errors });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});
// Global error handler to prevent server crash on unhandled promise rejections
process.on('unhandledRejection', (reason, promise) => {
  console.error('Unhandled Rejection at:', promise, 'reason:', reason);
});
process.on('uncaughtException', (err) => {
  console.error('Uncaught Exception thrown:', err);
});

/* ---------------- Maybe List ---------------- */
// Add player to maybe list
app.post('/api/events/:id/maybe', async (req, res) => {
  if (requireSeniorsSiteAdminForWrite(req, res)) return;
  try {
    const { name } = req.body || {};
    if (!name) return res.status(400).json({ error: 'name required' });
    
    const ev = await findScopedEventById(req, req.params.id);
    if (!ev) return res.status(404).json({ error: 'Not found' });
    
    if (!Array.isArray(ev.maybeList)) ev.maybeList = [];
    
    const trimmedName = String(name).trim();
    // Check for duplicates (case-insensitive)
    const exists = ev.maybeList.some(n => String(n).toLowerCase() === trimmedName.toLowerCase());
    if (exists) return res.status(409).json({ error: 'Name already on maybe list' });
    
    ev.maybeList.push(trimmedName);
    await ev.save();
    await logAudit(ev._id, 'add_maybe', trimmedName, {
      ...auditContextFromEvent(ev),
      details: { maybeCount: ev.maybeList.length },
    });
    res.json(ev);
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// Promote a maybe-list player into an open tee/team slot
app.post('/api/events/:id/maybe/fill', async (req, res) => {
  if (requireSeniorsSiteAdminForWrite(req, res)) return;
  try {
    const { name, teeId, asFifth } = req.body || {};
    const ev = await findScopedEventById(req, req.params.id);
    if (!ev) return res.status(404).json({ error: 'Not found' });
    if (!Array.isArray(ev.maybeList)) ev.maybeList = [];
    if (!Array.isArray(ev.teeTimes) || !ev.teeTimes.length) {
      return res.status(400).json({ error: 'No tee/team slots available' });
    }
    if (!ev.maybeList.length) return res.status(400).json({ error: 'Maybe list is empty' });

    let maybeIndex = 0;
    if (name) {
      const normalized = String(name).trim().toLowerCase();
      maybeIndex = ev.maybeList.findIndex((n) => String(n).trim().toLowerCase() === normalized);
      if (maybeIndex === -1) return res.status(404).json({ error: 'Name not found on maybe list' });
    }

    const pickedName = String(ev.maybeList[maybeIndex] || '').trim();
    if (!pickedName) return res.status(400).json({ error: 'Invalid maybe list name' });
    if (isDuplicatePlayerName(ev, pickedName)) {
      return res.status(409).json({ error: 'duplicate player name', message: 'Player already registered on this event.' });
    }

    let slot = null;
    let addAsFifth = false;
    if (teeId) {
      slot = ev.teeTimes.id(teeId);
      const capacity = evaluateSlotAddition(ev, slot, { allowFifth: !!asFifth });
      if (!capacity.ok) {
        return res.status(409).json({ error: capacity.error, canAddFifth: capacity.canAddFifth });
      }
      addAsFifth = capacity.asFifth;
    } else {
      slot = findNextOpenSlot(ev, teeId || null);
    }
    if (!slot) {
      return res.status(409).json({ error: teeId ? 'selected slot full' : 'all slots full' });
    }

    slot.players.push(buildPlayerEntry(pickedName, { isFifth: addAsFifth }));
    normalizeSlotFifthState(ev, slot);
    ev.maybeList.splice(maybeIndex, 1);
    await ev.save();

    const teeLabel = getTeeLabel(ev, slot._id);
    await logAudit(ev._id, 'fill_maybe', pickedName, {
      ...auditContextFromEvent(ev),
      teeId: slot._id,
      teeLabel,
      details: {
        asFifth: !!addAsFifth,
        maybeCount: ev.maybeList.length,
      },
    });

    if (ev.notificationsEnabled !== false) {
      sendSubscriberChangeEmail(
        `Player Confirmed: ${ev.course} (${fmt.dateISO(ev.date)})`,
        frame('Maybe List Player Confirmed',
          `<p><strong>${esc(pickedName)}</strong> moved from maybe list to active ${esc(ev.isTeamEvent ? 'team' : 'tee time')}.</p>
           <p><strong>Event:</strong> ${esc(ev.course)}</p>
           <p><strong>Date:</strong> ${esc(fmt.dateLong(ev.date))}</p>
           <p><strong>${ev.isTeamEvent ? 'Team' : 'Tee Time'}:</strong> ${esc(teeLabel)}</p>
           ${btn('View Event', buildSiteEventUrl(ev.groupSlug, ev._id))}`),
        { groupSlug: ev.groupSlug }
      ).catch((err) => console.error('Failed maybe fill email:', err));
    }

    return res.json({ ok: true, event: ev, addedName: pickedName, teeLabel });
  } catch (e) {
    return res.status(500).json({ error: e.message });
  }
});



// Remove player from maybe list
app.delete('/api/events/:id/maybe/:index', async (req, res) => {
  if (requireSeniorsSiteAdminForWrite(req, res)) return;
  try {
    const ev = await findScopedEventById(req, req.params.id);
    if (!ev) return res.status(404).json({ error: 'Not found' });
    
    if (!Array.isArray(ev.maybeList)) ev.maybeList = [];
    const index = parseInt(req.params.index, 10);
    
    if (index < 0 || index >= ev.maybeList.length) {
      return res.status(404).json({ error: 'Invalid index' });
    }
    
    const removedName = String(ev.maybeList[index] || '').trim();
    ev.maybeList.splice(index, 1);
    await ev.save();
    await logAudit(ev._id, 'remove_maybe', removedName, {
      ...auditContextFromEvent(ev),
      details: { maybeCount: ev.maybeList.length },
    });
    res.json(ev);
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

/* ---------------- Audit Log ---------------- */
app.get('/api/events/:id/audit-log', async (req, res) => {
  try {
    if (!AuditLog) return res.status(501).json({ error: 'Audit log not available' });
    const normalizedGroupSlug = normalizeGroupSlug(getGroupSlug(req));
    const cutoff = getTeeTimeAuditWindowStart();
    const ev = await findScopedEventById(req, req.params.id, { lean: true });
    const auditQuery = { eventId: req.params.id, groupSlug: normalizedGroupSlug, timestamp: { $gte: cutoff } };
    const [auditLogs, legacyTeeLogs] = await Promise.all([
      AuditLog.find(auditQuery).sort({ timestamp: 1 }).limit(200).lean(),
      TeeTimeLog
        ? TeeTimeLog.find({ eventId: req.params.id, groupSlug: normalizedGroupSlug, createdAt: { $gte: cutoff } }).sort({ createdAt: 1 }).limit(200).lean()
        : Promise.resolve([]),
    ]);
    const logs = auditLogs
      .concat((legacyTeeLogs || []).map((entry) => buildLegacyTeeAuditEntry(entry)).filter(Boolean))
      .sort((left, right) => new Date(left.timestamp).getTime() - new Date(right.timestamp).getTime())
      .slice(0, 200);
    if (!ev && !logs.length) return res.status(404).json({ error: 'Event not found' });
    const hasCreateEntry = logs.some((log) => String(log && log.action || '').trim() === 'create_event');
    const createdEntry = hasCreateEntry ? null : buildEventCreatedAuditEntry(ev, { cutoff });
    res.json(createdEntry ? [createdEntry, ...logs] : logs);
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

/* ---------------- Subscribers ---------------- */
async function ensureSubscriberRecord(groupSlug = DEFAULT_SITE_GROUP_SLUG, email = '', details = {}) {
  if (!Subscriber) throw new Error('subscriber model missing');
  const normalizedGroupSlug = normalizeGroupSlug(groupSlug);
  const normalizedEmail = String(email || '').trim().toLowerCase();
  if (!normalizedEmail) throw new Error('email required');
  const subscriberFields = {};
  if (Object.prototype.hasOwnProperty.call(details, 'ghinNumber')) {
    const normalizedGhin = String(details.ghinNumber || '').trim();
    if (normalizedGhin) subscriberFields.ghinNumber = normalizedGhin;
  }
  if (Object.prototype.hasOwnProperty.call(details, 'handicapIndex')) {
    const rawHandicap = details.handicapIndex;
    if (rawHandicap !== '' && rawHandicap !== null && rawHandicap !== undefined) {
      const handicapIndex = Number(rawHandicap);
      if (!Number.isFinite(handicapIndex)) throw new Error('handicapIndex must be a number');
      subscriberFields.handicapIndex = handicapIndex;
    }
  }

  let existing = await Subscriber.findOne({ ...groupScopeFilter(normalizedGroupSlug), email: normalizedEmail });
  if (existing) {
    let changed = false;
    if (Object.prototype.hasOwnProperty.call(subscriberFields, 'ghinNumber') && subscriberFields.ghinNumber !== String(existing.ghinNumber || '')) {
      existing.ghinNumber = subscriberFields.ghinNumber;
      changed = true;
    }
    if (Object.prototype.hasOwnProperty.call(subscriberFields, 'handicapIndex') && subscriberFields.handicapIndex !== existing.handicapIndex) {
      existing.handicapIndex = subscriberFields.handicapIndex;
      changed = true;
    }
    if (!existing.unsubscribeToken) {
      existing.unsubscribeToken = require('crypto').randomBytes(32).toString('hex');
      changed = true;
    }
    if (changed) {
      await existing.save();
    }
    return { subscriber: existing, isNew: false };
  }

  const created = new Subscriber({ groupSlug: normalizedGroupSlug, email: normalizedEmail, ...subscriberFields });
  await created.save();
  return { subscriber: created, isNew: true };
}

app.post('/api/subscribe', async (req, res) => {
  const { email, ghinNumber, handicapIndex } = req.body || {};
  const groupSlug = getGroupSlug(req);
  
  console.log(JSON.stringify({ t:new Date().toISOString(), level:'info', msg:'subscribe request received', email: email ? '***' : null }));
  
  if (!email) return res.status(400).json({ error: 'email required' });
  
  try {
    const subscriberDetails = {};
    if (ghinNumber !== undefined) subscriberDetails.ghinNumber = ghinNumber;
    if (handicapIndex !== undefined) subscriberDetails.handicapIndex = handicapIndex;
    const { subscriber: s, isNew } = await ensureSubscriberRecord(groupSlug, email, subscriberDetails);
    const subscriberEmail = String(s.email || '').trim().toLowerCase();
    const groupContext = await getSubscriptionGroupContext(groupSlug);
    console.log(JSON.stringify({ t:new Date().toISOString(), level:'info', msg:'subscriber added', email: subscriberEmail, isNew }));
    
    // Send response immediately
    res.json({
      ok: true,
      id: s._id.toString(),
      isNew,
      groupSlug: groupContext.groupSlug,
      groupReference: groupContext.groupReference,
      subscriber: {
        _id: s._id.toString(),
        email: s.email,
        ghinNumber: s.ghinNumber || '',
        handicapIndex: Number.isFinite(s.handicapIndex) ? s.handicapIndex : null,
      },
    });
    
    // Send confirmation email asynchronously (don't block the response)
    console.log(JSON.stringify({ t:new Date().toISOString(), level:'info', msg:'sending confirmation', to: subscriberEmail }));
    const unsubLink = `${SITE_URL}api/unsubscribe/${s.unsubscribeToken}`;
    const subject = `${groupContext.groupReference} Notifications - Subscription Confirmed`;
    const message = `
      <p>Thanks for subscribing to <strong>${esc(groupContext.groupReference)}</strong> notifications.</p>
      <p>This subscription only applies to that golf group. If you also subscribe to another group, that list stays separate.</p>
      <p><a href="${unsubLink}">Click here to unsubscribe from ${esc(groupContext.groupReference)}</a></p>
    `;
    
    sendEmail(subscriberEmail, subject, message)
      .then(result => {
        console.log(JSON.stringify({ t:new Date().toISOString(), level:'info', msg:'confirmation sent', result }));
      })
      .catch(emailErr => {
        console.error(JSON.stringify({ t:new Date().toISOString(), level:'error', msg:'confirmation failed', error:emailErr.message, stack:emailErr.stack }));
      });
  } catch (e) { 
    console.error(JSON.stringify({ t:new Date().toISOString(), level:'error', msg:'subscribe error', error:e.message, stack:e.stack }));
    res.status(500).json({ error:e.message }); 
  }
});

app.post('/api/admin/subscribers', async (req, res) => {
  if (!isSiteAdmin(req)) {
    return res.status(403).json({ error: 'Forbidden' });
  }

  const { email, ghinNumber, handicapIndex } = req.body || {};
  if (!email) return res.status(400).json({ error: 'email required' });

  try {
    const groupSlug = getGroupSlug(req);
    const subscriberDetails = {};
    if (ghinNumber !== undefined) subscriberDetails.ghinNumber = ghinNumber;
    if (handicapIndex !== undefined) subscriberDetails.handicapIndex = handicapIndex;
    const { subscriber, isNew } = await ensureSubscriberRecord(groupSlug, email, subscriberDetails);
    const groupContext = await getSubscriptionGroupContext(groupSlug);
    return res.status(isNew ? 201 : 200).json({
      ok: true,
      isNew,
      groupSlug: groupContext.groupSlug,
      groupReference: groupContext.groupReference,
      subscriber: {
        _id: subscriber._id,
        email: subscriber.email,
        ghinNumber: subscriber.ghinNumber || '',
        handicapIndex: Number.isFinite(subscriber.handicapIndex) ? subscriber.handicapIndex : null,
        unsubscribeToken: subscriber.unsubscribeToken,
        createdAt: subscriber.createdAt,
        updatedAt: subscriber.updatedAt,
      },
    });
  } catch (error) {
    console.error('Admin add subscriber error:', error);
    return res.status(500).json({ error: error && error.message ? error.message : 'Failed to add subscriber' });
  }
});

/* Unsubscribe */
app.get('/api/unsubscribe/:token', async (req, res) => {
  try {
    if (!Subscriber) return res.status(500).send('Subscriber model not available');
    
    const subscriber = await Subscriber.findOne({ unsubscribeToken: req.params.token });
    if (!subscriber) {
      return res.status(404).send(`
        <!DOCTYPE html>
        <html><head><title>Unsubscribe</title><style>body{font-family:system-ui;max-width:600px;margin:50px auto;padding:20px;text-align:center}</style></head>
        <body><h1>⚠️ Invalid Link</h1><p>This unsubscribe link is invalid or has expired.</p></body></html>
      `);
    }
    
    const groupContext = await getSubscriptionGroupContext(subscriber.groupSlug);
    await Subscriber.findByIdAndDelete(subscriber._id);
    
    res.send(`
      <!DOCTYPE html>
      <html><head><title>Unsubscribed</title><style>body{font-family:system-ui;max-width:600px;margin:50px auto;padding:20px;text-align:center}h1{color:#10b981}</style></head>
      <body><h1>✅ Unsubscribed Successfully</h1><p>You've been removed from the <strong>${esc(groupContext.groupReference)}</strong> notification list.</p><p>If you were subscribed to any other golf groups, those subscriptions are still active.</p></body></html>
    `);
  } catch (e) {
    console.error('Unsubscribe error:', e);
    res.status(500).send('Error processing unsubscribe request');
  }
});

/* Admin - Get/Set Global Notification Setting */
app.get('/api/admin/settings/notifications', async (req, res) => {
  if (!isSiteAdmin(req)) {
    return res.status(403).json({ error: 'Forbidden' });
  }
  
  try {
    const groupSlug = getGroupSlug(req);
    const enabled = await areNotificationsEnabled(groupSlug);
    res.json({ notificationsEnabled: enabled, groupSlug });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

app.put('/api/admin/settings/notifications', async (req, res) => {
  if (!isSiteAdmin(req)) {
    return res.status(403).json({ error: 'Forbidden' });
  }
  
  try {
    if (!Settings) return res.status(500).json({ error: 'Settings model not available' });
    
    const { notificationsEnabled } = req.body || {};
    if (typeof notificationsEnabled !== 'boolean') {
      return res.status(400).json({ error: 'notificationsEnabled must be a boolean' });
    }
    const groupSlug = getGroupSlug(req);
    await Settings.findOneAndUpdate(
      scopedSettingQuery(groupSlug, 'notificationsEnabled'),
      { groupSlug, key: 'notificationsEnabled', value: notificationsEnabled },
      { upsert: true, new: true }
    );
    
    res.json({ ok: true, notificationsEnabled, groupSlug });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

app.get('/api/admin/settings/subscriber-change-notifications', async (req, res) => {
  if (!isSiteAdmin(req)) {
    return res.status(403).json({ error: 'Forbidden' });
  }

  try {
    const groupSlug = getGroupSlug(req);
    const enabled = await areSubscriberChangeNotificationsEnabled(groupSlug);
    res.json({ subscriberChangeNotificationsEnabled: enabled, groupSlug });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

app.put('/api/admin/settings/subscriber-change-notifications', async (req, res) => {
  if (!isSiteAdmin(req)) {
    return res.status(403).json({ error: 'Forbidden' });
  }

  try {
    if (!Settings) return res.status(500).json({ error: 'Settings model not available' });

    const { subscriberChangeNotificationsEnabled } = req.body || {};
    if (typeof subscriberChangeNotificationsEnabled !== 'boolean') {
      return res.status(400).json({ error: 'subscriberChangeNotificationsEnabled must be a boolean' });
    }
    const groupSlug = getGroupSlug(req);
    await Settings.findOneAndUpdate(
      scopedSettingQuery(groupSlug, 'subscriberChangeNotificationsEnabled'),
      { groupSlug, key: 'subscriberChangeNotificationsEnabled', value: subscriberChangeNotificationsEnabled },
      { upsert: true, new: true }
    );

    res.json({ ok: true, subscriberChangeNotificationsEnabled, groupSlug });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

app.get('/api/admin/settings/tee-time-event-lifecycle-notifications', async (req, res) => {
  if (!isSiteAdmin(req)) {
    return res.status(403).json({ error: 'Forbidden' });
  }

  try {
    const groupSlug = getGroupSlug(req);
    const enabled = await areTeeTimeEventLifecycleNotificationsEnabled(groupSlug);
    res.json({ teeTimeEventLifecycleNotificationsEnabled: enabled, groupSlug });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

app.put('/api/admin/settings/tee-time-event-lifecycle-notifications', async (req, res) => {
  if (!isSiteAdmin(req)) {
    return res.status(403).json({ error: 'Forbidden' });
  }

  try {
    if (!Settings) return res.status(500).json({ error: 'Settings model not available' });

    const { teeTimeEventLifecycleNotificationsEnabled } = req.body || {};
    if (typeof teeTimeEventLifecycleNotificationsEnabled !== 'boolean') {
      return res.status(400).json({ error: 'teeTimeEventLifecycleNotificationsEnabled must be a boolean' });
    }
    const groupSlug = getGroupSlug(req);
    await Settings.findOneAndUpdate(
      scopedSettingQuery(groupSlug, 'teeTimeEventLifecycleNotificationsEnabled'),
      { groupSlug, key: 'teeTimeEventLifecycleNotificationsEnabled', value: teeTimeEventLifecycleNotificationsEnabled },
      { upsert: true, new: true }
    );

    res.json({ ok: true, teeTimeEventLifecycleNotificationsEnabled, groupSlug });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

/* Admin - Get/Set Scheduler Enable Setting */
app.get('/api/admin/settings/scheduler', async (req, res) => {
  if (!isSiteAdmin(req)) {
    return res.status(403).json({ error: 'Forbidden' });
  }
  try {
    const groupSlug = getGroupSlug(req);
    const schedulerEnabled = await areSchedulerJobsEnabled(groupSlug);
    res.json({ schedulerEnabled, lockedByEnv: SCHEDULER_ENV_DISABLED, groupSlug });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

app.put('/api/admin/settings/scheduler', async (req, res) => {
  if (!isSiteAdmin(req)) {
    return res.status(403).json({ error: 'Forbidden' });
  }

  if (SCHEDULER_ENV_DISABLED) {
    return res.status(409).json({
      error: 'Scheduler is locked off by environment setting ENABLE_SCHEDULER=0',
      lockedByEnv: true,
      schedulerEnabled: false,
    });
  }

  try {
    if (!Settings) return res.status(500).json({ error: 'Settings model not available' });
    const groupSlug = getGroupSlug(req);
    const rawEnabled = req.body && req.body.schedulerEnabled;
    if (typeof rawEnabled !== 'boolean') {
      return res.status(400).json({ error: 'schedulerEnabled must be a boolean' });
    }
    const enabled = rawEnabled;
    await Settings.findOneAndUpdate(
      scopedSettingQuery(groupSlug, 'schedulerEnabled'),
      { groupSlug, key: 'schedulerEnabled', value: enabled },
      { upsert: true, new: true }
    );
    schedulerEnabledCache.set(normalizeGroupSlug(groupSlug), { value: enabled, ts: Date.now() });
    res.json({ ok: true, schedulerEnabled: enabled, lockedByEnv: false, groupSlug });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

/* Admin - Get/Set Scheduled Email Rule Settings */
app.get('/api/admin/settings/scheduled-email-rules', async (req, res) => {
  if (!isSiteAdmin(req)) {
    return res.status(403).json({ error: 'Forbidden' });
  }
  try {
    const groupSlug = getGroupSlug(req);
    const rules = await getScheduledEmailRules(groupSlug);
    res.json({ rules, availableRules: SCHEDULED_EMAIL_RULE_KEYS, groupSlug });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

app.put('/api/admin/settings/scheduled-email-rules', async (req, res) => {
  if (!isSiteAdmin(req)) {
    return res.status(403).json({ error: 'Forbidden' });
  }
  try {
    if (!Settings) return res.status(500).json({ error: 'Settings model not available' });
    const groupSlug = getGroupSlug(req);
    const ruleKey = String((req.body && req.body.ruleKey) || '').trim();
    if (!SCHEDULED_EMAIL_RULE_KEYS.includes(ruleKey)) {
      return res.status(400).json({ error: 'Invalid ruleKey', availableRules: SCHEDULED_EMAIL_RULE_KEYS });
    }
    const rawEnabled = req.body && req.body.enabled;
    if (typeof rawEnabled !== 'boolean') {
      return res.status(400).json({ error: 'enabled must be a boolean' });
    }

    const current = await getScheduledEmailRules(groupSlug);
    const updated = { ...current, [ruleKey]: rawEnabled };
    await Settings.findOneAndUpdate(
      scopedSettingQuery(groupSlug, 'scheduledEmailRules'),
      { groupSlug, key: 'scheduledEmailRules', value: updated },
      { upsert: true, new: true }
    );
    scheduledEmailRulesCache.set(normalizeGroupSlug(groupSlug), { value: updated, ts: Date.now() });
    res.json({ ok: true, rules: updated, groupSlug });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

/* Admin - List Subscribers */
app.get('/api/admin/subscribers', async (req, res) => {
  if (!isSiteAdmin(req)) {
    return res.status(403).json({ error: 'Forbidden' });
  }
  
  try {
    if (!Subscriber) return res.status(500).json({ error: 'Subscriber model not available' });
    
    const groupSlug = getGroupSlug(req);
    // Migration: Add tokens to existing subscribers without them
    const crypto = require('crypto');
    const subsWithoutToken = await Subscriber.find({ ...groupScopeFilter(groupSlug), unsubscribeToken: { $exists: false } });
    for (const sub of subsWithoutToken) {
      sub.unsubscribeToken = crypto.randomBytes(32).toString('hex');
      await sub.save();
    }
    
    const subscribers = await Subscriber.find({ ...groupScopeFilter(groupSlug) }).sort({ createdAt: -1 }).lean();
    res.json(subscribers);
  } catch (e) {
    console.error('List subscribers error:', e);
    res.status(500).json({ error: e.message });
  }
});

/* Admin - Delete Subscriber */
app.delete('/api/admin/subscribers/:id', async (req, res) => {
  if (!isSiteAdmin(req) && !isAdminDelete(req)) {
    return res.status(403).json({ error: 'Delete code required' });
  }
  
  try {
    if (!Subscriber) return res.status(500).json({ error: 'Subscriber model not available' });
    const deleted = await Subscriber.findOneAndDelete({ ...groupScopeFilter(getGroupSlug(req)), _id: req.params.id });
    if (!deleted) return res.status(404).json({ error: 'Subscriber not found' });
    res.json({ ok: true });
  } catch (e) {
    console.error('Delete subscriber error:', e);
    res.status(500).json({ error: e.message });
  }
});

/* Seniors roster */
app.get('/api/admin/seniors-roster', async (req, res) => {
  if (requireSeniorsGroupAdmin(req, res)) return;
  try {
    if (!SeniorsGolfer) return res.status(500).json({ error: 'Seniors golfer model not available' });
    const golfers = await SeniorsGolfer.find({ groupSlug: 'seniors' }).sort({ active: -1, nameKey: 1 }).lean();
    return res.json(golfers);
  } catch (error) {
    return res.status(500).json({ error: error && error.message ? error.message : 'Failed to load Seniors roster' });
  }
});

app.post('/api/admin/seniors-roster', async (req, res) => {
  if (requireSeniorsGroupAdmin(req, res)) return;
  try {
    if (!SeniorsGolfer) return res.status(500).json({ error: 'Seniors golfer model not available' });
    const payload = normalizeSeniorsGolferInput(req.body || {});
    const created = await SeniorsGolfer.create({ groupSlug: 'seniors', ...payload });
    return res.status(201).json({ ok: true, golfer: created });
  } catch (error) {
    if (error && error.code === 11000) return res.status(409).json({ error: 'Golfer already exists on the Seniors roster' });
    return res.status(500).json({ error: error && error.message ? error.message : 'Failed to add golfer' });
  }
});

app.put('/api/admin/seniors-roster/:id', async (req, res) => {
  if (requireSeniorsGroupAdmin(req, res)) return;
  try {
    if (!SeniorsGolfer) return res.status(500).json({ error: 'Seniors golfer model not available' });
    const payload = normalizeSeniorsGolferInput(req.body || {});
    const updated = await SeniorsGolfer.findOneAndUpdate(
      { _id: req.params.id, groupSlug: 'seniors' },
      { $set: payload },
      { new: true, runValidators: true }
    );
    if (!updated) return res.status(404).json({ error: 'Golfer not found' });
    return res.json({ ok: true, golfer: updated });
  } catch (error) {
    if (error && error.code === 11000) return res.status(409).json({ error: 'Golfer already exists on the Seniors roster' });
    return res.status(500).json({ error: error && error.message ? error.message : 'Failed to update golfer' });
  }
});

app.delete('/api/admin/seniors-roster/:id', async (req, res) => {
  if (requireSeniorsGroupAdmin(req, res)) return;
  try {
    if (!SeniorsGolfer) return res.status(500).json({ error: 'Seniors golfer model not available' });
    const deleted = await SeniorsGolfer.findOneAndDelete({ _id: req.params.id, groupSlug: 'seniors' });
    if (!deleted) return res.status(404).json({ error: 'Golfer not found' });
    return res.json({ ok: true });
  } catch (error) {
    return res.status(500).json({ error: error && error.message ? error.message : 'Failed to remove golfer' });
  }
});

app.get('/api/admin/seniors-roster/export.csv', async (req, res) => {
  if (requireSeniorsGroupAdmin(req, res)) return;
  try {
    if (!SeniorsGolfer) return res.status(500).json({ error: 'Seniors golfer model not available' });
    const golfers = await SeniorsGolfer.find({ groupSlug: 'seniors' }).sort({ active: -1, nameKey: 1 }).lean();
    const exportRows = [
      ['Roster Number', 'Name', 'First Name', 'Last Name', 'Preferred First', 'Preferred Last', 'Email', 'Phone', 'Address', 'GHIN', 'Handicap Gold', 'Handicap Red', 'Handicap', 'Active', 'Notes', 'Updated At'],
      ...golfers.map((golfer) => [
        Number.isFinite(golfer.rosterNumber) ? golfer.rosterNumber : '',
        golfer.name,
        golfer.firstName || '',
        golfer.lastName || '',
        golfer.preferredFirstName || '',
        golfer.preferredLastName || '',
        golfer.email || '',
        golfer.phone || '',
        golfer.address || '',
        golfer.ghinNumber || '',
        Number.isFinite(golfer.handicapGold) ? golfer.handicapGold : '',
        Number.isFinite(golfer.handicapRed) ? golfer.handicapRed : '',
        Number.isFinite(golfer.handicapIndex) ? golfer.handicapIndex : '',
        golfer.active ? 'Yes' : 'No',
        golfer.notes || '',
        golfer.updatedAt ? new Date(golfer.updatedAt).toISOString() : '',
      ]),
    ];
    const csv = buildCsv(exportRows);
    res.setHeader('Content-Type', 'text/csv; charset=utf-8');
    res.setHeader('Content-Disposition', 'attachment; filename="seniors-roster.csv"');
    return res.send(`\ufeff${csv}`);
  } catch (error) {
    return res.status(500).json({ error: error && error.message ? error.message : 'Failed to export Seniors roster' });
  }
});

app.get('/api/admin/seniors-roster/export.xlsx', async (req, res) => {
  if (requireSeniorsGroupAdmin(req, res)) return;
  try {
    if (!SeniorsGolfer) return res.status(500).json({ error: 'Seniors golfer model not available' });
    const golfers = await SeniorsGolfer.find({ groupSlug: 'seniors' }).sort({ active: -1, nameKey: 1 }).lean();
    const workbook = buildWorkbookBuffer('Seniors Roster', [
      ['Roster Number', 'Name', 'First Name', 'Last Name', 'Preferred First', 'Preferred Last', 'Email', 'Phone', 'Address', 'GHIN', 'Handicap Gold', 'Handicap Red', 'Handicap', 'Active', 'Notes', 'Updated At'],
      ...golfers.map((golfer) => [
        Number.isFinite(golfer.rosterNumber) ? golfer.rosterNumber : '',
        golfer.name,
        golfer.firstName || '',
        golfer.lastName || '',
        golfer.preferredFirstName || '',
        golfer.preferredLastName || '',
        golfer.email || '',
        golfer.phone || '',
        golfer.address || '',
        golfer.ghinNumber || '',
        Number.isFinite(golfer.handicapGold) ? golfer.handicapGold : '',
        Number.isFinite(golfer.handicapRed) ? golfer.handicapRed : '',
        Number.isFinite(golfer.handicapIndex) ? golfer.handicapIndex : '',
        golfer.active ? 'Yes' : 'No',
        golfer.notes || '',
        golfer.updatedAt ? new Date(golfer.updatedAt).toISOString() : '',
      ]),
    ]);
    res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
    res.setHeader('Content-Disposition', 'attachment; filename="seniors-roster.xlsx"');
    return res.send(workbook);
  } catch (error) {
    return res.status(500).json({ error: error && error.message ? error.message : 'Failed to export Seniors roster' });
  }
});

app.get('/api/admin/seniors-roster/extract.txt', async (req, res) => {
  if (requireSeniorsGroupAdmin(req, res)) return;
  try {
    if (!SeniorsGolfer) return res.status(500).json({ error: 'Seniors golfer model not available' });
    const format = String(req.query.format || 'name-email').trim().toLowerCase();
    const golfers = await SeniorsGolfer.find({ groupSlug: 'seniors', active: true }).sort({ nameKey: 1 }).lean();
    const lines = golfers.map((golfer) => {
      if (format === 'emails') return golfer.email || '';
      if (format === 'names') return golfer.name || '';
      return golfer.email ? `${golfer.name} <${golfer.email}>` : golfer.name;
    }).filter(Boolean);
    res.setHeader('Content-Type', 'text/plain; charset=utf-8');
    res.setHeader('Content-Disposition', `attachment; filename="seniors-roster-${format}.txt"`);
    return res.send(lines.join('\n'));
  } catch (error) {
    return res.status(500).json({ error: error && error.message ? error.message : 'Failed to extract Seniors roster' });
  }
});

app.post('/api/admin/seniors-roster/import', upload.single('file'), async (req, res) => {
  if (requireSeniorsGroupAdmin(req, res)) return;
  try {
    if (!SeniorsGolfer) return res.status(500).json({ error: 'Seniors golfer model not available' });
    if (!req.file) return res.status(400).json({ error: 'file required' });

    const rows = parseSpreadsheetRows(req.file);
    if (!rows.length) return res.status(400).json({ error: 'Empty file' });

    let dataRows = rows;
    let rowOffset = 1;
    let useTemplateRosterLayout = false;
    const templateHeaderRow = rows.findIndex((values) => String(values[2] || '').trim().toUpperCase() === 'PLAYER ROSTER');
    const templateFieldRow = templateHeaderRow >= 0 ? templateHeaderRow + 1 : -1;
    if (templateHeaderRow >= 0 && rows[templateFieldRow] && String(rows[templateFieldRow][6] || '').trim().toUpperCase() === 'FIRST NAME') {
      useTemplateRosterLayout = true;
      dataRows = rows.slice(templateFieldRow + 1);
      rowOffset = templateFieldRow + 2;
    }

    let pick = () => '';
    let hasNameColumn = false;
    if (!useTemplateRosterLayout) {
      const headers = rows[0].map((value) => String(value || '').trim().toLowerCase());
      const headerIndex = Object.fromEntries(headers.map((header, index) => [header, index]));
      pick = (values, keys) => {
        for (const key of keys) {
          const idx = headerIndex[key];
          if (Number.isInteger(idx)) return values[idx] !== undefined ? values[idx] : '';
        }
        return '';
      };
      hasNameColumn = headers.includes('name') || headers.includes('full_name');
      if (!hasNameColumn) {
        return res.status(400).json({ error: 'Spreadsheet must include a Name/full_name column or use the BRS Seniors roster workbook format' });
      }
    }

    let processed = 0;
    let created = 0;
    let updated = 0;
    const errors = [];

    for (let i = 0; i < dataRows.length; i += 1) {
      const values = dataRows[i];
      if (!values || values.every((value) => !String(value || '').trim())) continue;
      processed += 1;
      try {
        const payload = useTemplateRosterLayout
          ? normalizeSeniorsGolferInput({
            rosterNumber: values[0],
            name: values[2],
            firstName: values[6],
            lastName: values[7],
            preferredFirstName: values[9],
            preferredLastName: values[10],
            address: values[20],
            email: values[25],
            ghinNumber: values[8],
            handicapGold: values[11],
            handicapRed: values[12],
            handicapIndex: values[11] !== '' && values[11] !== null && values[11] !== undefined ? values[11] : values[12],
            notes: '',
            active: true,
          })
          : normalizeSeniorsGolferInput({
            rosterNumber: pick(values, ['roster_number', 'roster', 'number']),
            name: pick(values, ['name', 'full_name', 'golfer', 'player']),
            firstName: pick(values, ['first_name', 'firstname']),
            lastName: pick(values, ['last_name', 'lastname']),
            preferredFirstName: pick(values, ['preferred_first_name', 'preferred_first', 'fn']),
            preferredLastName: pick(values, ['preferred_last_name', 'preferred_last', 'ln']),
            email: pick(values, ['email', 'email_address']),
            phone: pick(values, ['phone', 'phone_number', 'mobile']),
            address: pick(values, ['address', 'street_address']),
            ghinNumber: pick(values, ['ghin', 'ghin_number']),
            handicapGold: pick(values, ['handicap_gold', 'hdcp_gold']),
            handicapRed: pick(values, ['handicap_red', 'hdcp_red']),
            handicapIndex: pick(values, ['handicap', 'handicap_index']),
            notes: pick(values, ['notes']),
            active: !['no', 'false', '0', 'inactive'].includes(String(pick(values, ['active'])).trim().toLowerCase()),
          });
        if (!payload.name) continue;
        const nameKey = normalizeSeniorsGolferName(payload.name).toLowerCase();
        const existing = await SeniorsGolfer.findOne({ groupSlug: 'seniors', nameKey });
        if (existing) {
          existing.rosterNumber = payload.rosterNumber;
          existing.name = payload.name;
          existing.firstName = payload.firstName;
          existing.lastName = payload.lastName;
          existing.preferredFirstName = payload.preferredFirstName;
          existing.preferredLastName = payload.preferredLastName;
          existing.email = payload.email;
          existing.phone = payload.phone;
          existing.address = payload.address;
          existing.ghinNumber = payload.ghinNumber;
          existing.handicapGold = payload.handicapGold;
          existing.handicapRed = payload.handicapRed;
          existing.handicapIndex = payload.handicapIndex;
          existing.notes = payload.notes;
          existing.active = payload.active;
          await existing.save();
          updated += 1;
        } else {
          await SeniorsGolfer.create({ groupSlug: 'seniors', ...payload });
          created += 1;
        }
      } catch (error) {
        errors.push({ rowNumber: i + rowOffset, error: error && error.message ? error.message : 'Invalid row' });
      }
    }

    return res.json({
      ok: true,
      processed,
      created,
      updated,
      errorCount: errors.length,
      errors: errors.slice(0, 100),
      importedFileType: /\.(xlsx|xls)$/i.test(String(req.file.originalname || '')) ? 'excel' : 'csv',
    });
  } catch (error) {
    return res.status(500).json({ error: error && error.message ? error.message : 'Failed to import Seniors roster' });
  }
});

app.get('/api/admin/tee-time-recovery', async (req, res) => {
  if (!isSiteAdmin(req)) {
    return res.status(403).json({ error: 'Forbidden' });
  }
  if (!DeletedTeeTimeArchive) {
    return res.status(500).json({ error: 'Delete archive model not available' });
  }
  try {
    const dateISO = String(req.query.date || '').trim();
    if (!/^\d{4}-\d{2}-\d{2}$/.test(dateISO)) {
      return res.status(400).json({ error: 'date is required (YYYY-MM-DD)' });
    }
    const normalizedGroupSlug = normalizeGroupSlug(getGroupSlug(req));
    const archives = await DeletedTeeTimeArchive.find({
      groupSlug: normalizedGroupSlug,
      eventDateISO: dateISO,
      deletedAt: { $gte: getTeeTimeAuditWindowStart() },
    }).sort({ deletedAt: -1 }).limit(200).lean();
    const eventIds = Array.from(new Set(
      archives
        .flatMap((entry) => [entry && entry.originalEventId, entry && entry.restoredEventId])
        .map((value) => String(value || '').trim())
        .filter(Boolean)
    ));
    const activeEvents = eventIds.length
      ? await Event.find({ ...groupScopeFilter(normalizedGroupSlug), _id: { $in: eventIds } })
        .select({ _id: 1, teeTimes: 1, isTeamEvent: 1 })
        .lean()
      : [];
    const entries = buildTeeTimeRecoveryEntries({ archives, activeEvents });
    return res.json({ days: TEE_TIME_AUDIT_RETENTION_DAYS, date: dateISO, entries });
  } catch (error) {
    console.error('Tee time recovery load error:', error);
    return res.status(500).json({ error: error.message });
  }
});

app.post('/api/admin/tee-time-recovery/:archiveId/restore', async (req, res) => {
  if (!isSiteAdmin(req)) {
    return res.status(403).json({ error: 'Forbidden' });
  }
  if (!DeletedTeeTimeArchive) {
    return res.status(500).json({ error: 'Delete archive model not available' });
  }
  try {
    const normalizedGroupSlug = normalizeGroupSlug(getGroupSlug(req));
    const archive = await DeletedTeeTimeArchive.findOne({
      _id: req.params.archiveId,
      groupSlug: normalizedGroupSlug,
    });
    if (!archive) return res.status(404).json({ error: 'Recovery item not found' });

    const archiveType = String(archive.archiveType || '').trim().toLowerCase();
    const slotLabelRaw = String(
      archive.slotLabel
        || (archive.snapshot && archive.snapshot.name)
        || (archive.snapshot && archive.snapshot.time)
        || ''
    ).trim();
    const slotDisplay = formatAuditSlotLabel(slotLabelRaw, !!archive.isTeamEvent);
    let restoredEvent = null;
    let restoredTeeId = '';
    let createdEvent = false;
    let restoredWholeEvent = false;

    if (archiveType === 'event') {
      const existing = archive.originalEventId
        ? await Event.findOne({ ...groupScopeFilter(normalizedGroupSlug), _id: archive.originalEventId })
        : null;
      if (existing) {
        archive.restoredAt = archive.restoredAt || new Date();
        archive.restoredEventId = String(existing._id);
        await archive.save();
        return res.json({
          ok: true,
          alreadyExists: true,
          archiveType,
          eventId: existing._id,
          course: existing.course || archive.eventCourse || '',
          dateISO: fmt.dateISO(existing.date) || archive.eventDateISO || '',
        });
      }
      const restored = await restoreEventFromArchivedSnapshot(archive.eventSnapshot || archive.snapshot, normalizedGroupSlug);
      restoredEvent = restored.event;
      createdEvent = restored.created;
      if (createdEvent) {
        await logAudit(restoredEvent._id, 'restore_event', 'SYSTEM', {
          ...auditContextFromEvent(restoredEvent),
          message: `Restored deleted event ${restoredEvent.course || 'event'} on ${fmt.dateISO(restoredEvent.date) || archive.eventDateISO || 'the selected date'}.`,
          details: {
            archiveId: String(archive._id),
            source: 'admin_recovery',
          },
        });
      }
    } else if (archiveType === 'tee_time') {
      const slotSnapshot = normalizeArchivedSlotSnapshot(archive.snapshot || {});
      if (!slotSnapshot) {
        return res.status(409).json({ error: 'Archived tee time data is missing' });
      }
      let targetEvent = archive.originalEventId
        ? await Event.findOne({ ...groupScopeFilter(normalizedGroupSlug), _id: archive.originalEventId })
        : null;
      if (!targetEvent && archive.restoredEventId) {
        targetEvent = await Event.findOne({ ...groupScopeFilter(normalizedGroupSlug), _id: archive.restoredEventId });
      }
      if (!targetEvent) {
        const restored = await restoreEventFromArchivedSnapshot(archive.eventSnapshot, normalizedGroupSlug);
        targetEvent = restored.event;
        createdEvent = restored.created;
        restoredWholeEvent = true;
        if (createdEvent) {
          await logAudit(targetEvent._id, 'restore_event', 'SYSTEM', {
            ...auditContextFromEvent(targetEvent),
            message: `Restored event ${targetEvent.course || 'event'} on ${fmt.dateISO(targetEvent.date) || archive.eventDateISO || 'the selected date'} from deleted tee-time recovery.`,
            details: {
              archiveId: String(archive._id),
              source: 'admin_recovery',
            },
          });
        }
        const matchingSlot = findArchivedSlotConflict(targetEvent, slotSnapshot, !!archive.isTeamEvent);
        restoredTeeId = String(matchingSlot && matchingSlot._id || archive.originalTeeId || '').trim();
      } else {
        if (!!targetEvent.isTeamEvent !== !!archive.isTeamEvent) {
          return res.status(409).json({ error: 'Live event format no longer matches the deleted tee time' });
        }
        const conflict = findArchivedSlotConflict(targetEvent, slotSnapshot, !!archive.isTeamEvent);
        if (conflict) {
          archive.restoredAt = archive.restoredAt || new Date();
          archive.restoredEventId = String(targetEvent._id);
          archive.restoredTeeId = String(conflict._id || '');
          await archive.save();
          return res.json({
            ok: true,
            alreadyExists: true,
            archiveType,
            eventId: targetEvent._id,
            teeId: conflict._id || '',
            course: targetEvent.course || archive.eventCourse || '',
            dateISO: fmt.dateISO(targetEvent.date) || archive.eventDateISO || '',
          });
        }
        const inserted = insertArchivedSlotIntoEvent(targetEvent, slotSnapshot, archive.slotIndex);
        if (!inserted.inserted) {
          return res.status(409).json({ error: 'Tee time already exists in the live event' });
        }
        await targetEvent.save();
        restoredTeeId = String(inserted.slot && inserted.slot._id || archive.originalTeeId || '').trim();
      }
      restoredEvent = targetEvent;
      await logAudit(restoredEvent._id, 'restore_tee_time', 'SYSTEM', {
        ...auditContextFromEvent(restoredEvent),
        teeId: restoredTeeId || archive.originalTeeId || null,
        teeLabel: slotLabelRaw,
        message: restoredWholeEvent
          ? `Restored deleted ${archive.isTeamEvent ? 'team' : 'tee time'} ${slotDisplay} by recreating the event snapshot.`
          : `Restored deleted ${archive.isTeamEvent ? 'team' : 'tee time'} ${slotDisplay}.`,
        details: {
          archiveId: String(archive._id),
          source: 'admin_recovery',
          restoredWholeEvent,
        },
      });
    } else {
      return res.status(400).json({ error: 'Unsupported recovery item type' });
    }

    archive.restoredAt = new Date();
    archive.restoredEventId = String(restoredEvent && restoredEvent._id || archive.restoredEventId || '').trim();
    archive.restoredTeeId = String(restoredTeeId || archive.restoredTeeId || '').trim();
    await archive.save();

    return res.json({
      ok: true,
      archiveType,
      eventId: restoredEvent && restoredEvent._id ? restoredEvent._id : '',
      teeId: restoredTeeId || '',
      course: restoredEvent && restoredEvent.course ? restoredEvent.course : archive.eventCourse || '',
      dateISO: restoredEvent && restoredEvent.date ? fmt.dateISO(restoredEvent.date) : archive.eventDateISO || '',
      createdEvent,
    });
  } catch (error) {
    console.error('Tee time recovery restore error:', error);
    return res.status(500).json({ error: error.message });
  }
});

app.delete('/api/admin/tee-time-recovery/month', async (req, res) => {
  if (!isAdminDelete(req)) {
    return res.status(403).json({ error: 'Delete code required' });
  }
  const normalizedGroupSlug = normalizeGroupSlug(getGroupSlug(req));
  if (!hasDeleteActionConfirmed(req)) {
    return res.status(400).json({ error: 'Bulk delete confirmation required' });
  }
  if (!hasDestructiveConfirmForGroup(req, normalizedGroupSlug)) {
    return res.status(403).json({ error: 'Confirm code required' });
  }
  if (!DeletedTeeTimeArchive) {
    return res.status(500).json({ error: 'Delete archive model not available' });
  }
  try {
    const month = String(req.query.month || req.body?.month || '').trim();
    const range = parseYearMonthRange(month);
    if (!range) {
      return res.status(400).json({ error: 'month is required (YYYY-MM)' });
    }
    if (!isPastYearMonth(range.month, new Date())) {
      return res.status(400).json({ error: 'Only past months can be bulk deleted' });
    }

    const events = await Event.find({
      ...groupScopeFilter(normalizedGroupSlug),
      date: { $gte: range.start, $lt: range.end },
    }).sort({ date: 1, createdAt: 1 }).lean();

    if (!events.length) {
      return res.json({ ok: true, month: range.month, deletedCount: 0 });
    }

    for (const event of events) {
      await archiveDeletedEvent(event, {
        deletedBy: 'SYSTEM',
        deleteSource: 'bulk_month_delete',
        notes: `Bulk deleted live event data for ${range.month}.`,
      });
    }

    const eventIds = events.map((event) => event && event._id).filter(Boolean);
    const deleteResult = await Event.deleteMany({
      ...groupScopeFilter(normalizedGroupSlug),
      _id: { $in: eventIds },
    });

    for (const event of events) {
      await logAudit(event._id, 'delete_event', 'SYSTEM', {
        ...auditContextFromEvent(event),
        message: `Deleted event ${event.course || 'event'} on ${fmt.dateISO(event.date) || 'the selected date'} during month cleanup for ${range.month}.`,
        details: {
          source: 'bulk_month_delete',
          month: range.month,
          teeCount: Array.isArray(event.teeTimes) ? event.teeTimes.length : 0,
        },
      });
    }

    return res.json({
      ok: true,
      month: range.month,
      deletedCount: deleteResult && Number.isFinite(Number(deleteResult.deletedCount))
        ? Number(deleteResult.deletedCount)
        : events.length,
    });
  } catch (error) {
    console.error('Tee time recovery month delete error:', error);
    return res.status(500).json({ error: error.message });
  }
});

// Admin - Tee time change log
app.get('/api/admin/tee-time-log', async (req, res) => {
  if (!isSiteAdmin(req)) {
    return res.status(403).json({ error: 'Forbidden' });
  }
  if (!TeeTimeLog) return res.status(500).json({ error: 'TeeTimeLog model not available' });
  try {
    const logs = await TeeTimeLog.find({
      groupSlug: getGroupSlug(req),
      createdAt: { $gte: getTeeTimeAuditWindowStart() },
    }).sort({ createdAt: -1 }).limit(200).lean();
    res.json(logs);
  } catch (e) {
    console.error('Tee time log error:', e);
    res.status(500).json({ error: e.message });
  }
});

app.get('/api/admin/tee-time-audit-log', async (req, res) => {
  if (!isSiteAdmin(req)) {
    return res.status(403).json({ error: 'Forbidden' });
  }
  if (!AuditLog && !TeeTimeLog) {
    return res.status(500).json({ error: 'Audit log models not available' });
  }
  try {
    const normalizedGroupSlug = normalizeGroupSlug(getGroupSlug(req));
    const cutoff = getTeeTimeAuditWindowStart();
    const [auditLogs, teeTimeLogs] = await Promise.all([
      AuditLog
        ? AuditLog.find({ groupSlug: normalizedGroupSlug, timestamp: { $gte: cutoff } }).sort({ timestamp: -1 }).limit(1000).lean()
        : Promise.resolve([]),
      TeeTimeLog
        ? TeeTimeLog.find({ groupSlug: normalizedGroupSlug, createdAt: { $gte: cutoff } }).sort({ createdAt: -1 }).limit(1000).lean()
        : Promise.resolve([]),
    ]);
    const eventIds = Array.from(new Set(
      auditLogs
        .concat(teeTimeLogs)
        .map((entry) => String(entry && entry.eventId || '').trim())
        .filter(Boolean)
    ));
    const activeEvents = eventIds.length
      ? await Event.find({ ...groupScopeFilter(normalizedGroupSlug), _id: { $in: eventIds } }).select({ _id: 1 }).lean()
      : [];
    const activeEventIds = new Set((activeEvents || []).map((entry) => String(entry && entry._id || '')));
    const entries = buildAdminTeeTimeAuditEntries({ auditLogs, teeTimeLogs, activeEventIds });
    res.json({ days: TEE_TIME_AUDIT_RETENTION_DAYS, entries });
  } catch (e) {
    console.error('Tee time audit log error:', e);
    res.status(500).json({ error: e.message });
  }
});

app.get('/api/admin/inbound-tee-time-email-log', async (req, res) => {
  if (!isSiteAdmin(req)) {
    return res.status(403).json({ error: 'Forbidden' });
  }
  if (!InboundTeeTimeEmailLog) return res.status(500).json({ error: 'InboundTeeTimeEmailLog model not available' });
  try {
    const logs = await InboundTeeTimeEmailLog.find({ groupSlug: getGroupSlug(req) })
      .sort({ emailReceivedAt: -1, loggedAt: -1 })
      .limit(500)
      .lean();
    res.json(logs);
  } catch (e) {
    console.error('Inbound tee-time email log error:', e);
    res.status(500).json({ error: e.message });
  }
});

/* Admin - Create/List/Download Backups */
app.get('/api/admin/backups', async (req, res) => {
  if (!isMainSiteAdminRequest(req)) {
    return res.status(403).json({ error: 'Forbidden' });
  }
  try {
    const backups = await listAdminBackups();
    const settings = await getBackupSettings();
    const status = await getBackupStatus();
    return res.json({
      backupRoot: BACKUP_ROOT,
      backupInProgress: Boolean(backupJobPromise),
      restoreInProgress: Boolean(restoreJobPromise),
      settings,
      status,
      overview: buildBackupOverview(backups, settings, status),
      auth: {
        separateDeleteCode: ADMIN_DESTRUCTIVE_CODE !== SITE_ADMIN_WRITE_CODE,
        destructiveConfirmRequired: Boolean(ADMIN_DESTRUCTIVE_CONFIRM_CODE),
      },
      backups,
    });
  } catch (e) {
    return res.status(500).json({ error: e.message });
  }
});

app.get('/api/admin/settings/backups', async (req, res) => {
  if (!isMainSiteAdminRequest(req)) {
    return res.status(403).json({ error: 'Forbidden' });
  }
  try {
    const settings = await getBackupSettings();
    const status = await getBackupStatus();
    return res.json({ settings, status, overview: buildBackupOverview(await listAdminBackups(), settings, status) });
  } catch (e) {
    return res.status(500).json({ error: e.message });
  }
});

app.put('/api/admin/settings/backups', async (req, res) => {
  if (!isMainSiteAdminRequest(req)) {
    return res.status(403).json({ error: 'Forbidden' });
  }
  try {
    if (!Settings) return res.status(500).json({ error: 'Settings model not available' });
    const settings = normalizeBackupSettings(req.body || {});
    await Settings.findOneAndUpdate(
      scopedSettingQuery(DEFAULT_SITE_GROUP_SLUG, 'backupSettings'),
      { groupSlug: DEFAULT_SITE_GROUP_SLUG, key: 'backupSettings', value: settings },
      { upsert: true, new: true }
    );
    backupSettingsCache = { value: settings, ts: Date.now() };
    return res.json({ ok: true, settings });
  } catch (e) {
    return res.status(500).json({ error: e.message });
  }
});

app.post('/api/admin/backups', async (req, res) => {
  if (!isMainSiteAdminRequest(req)) {
    return res.status(403).json({ error: 'Forbidden' });
  }
  if (backupJobPromise) {
    return res.status(409).json({ error: 'A backup is already in progress' });
  }
  try {
    backupJobPromise = createAdminBackupBundle();
    const manifest = await backupJobPromise;
    return res.json({
      ok: true,
      message: 'Backup created successfully',
      manifest,
    });
  } catch (e) {
    await recordBackupFailure(e).catch(() => {});
    return res.status(500).json({ error: e.message });
  } finally {
    backupJobPromise = null;
  }
});

app.post('/api/admin/backups/:backupId/restore', async (req, res) => {
  if (!isAdminDeleteCode(getDestructiveAdminCode(req), DEFAULT_SITE_GROUP_SLUG)) {
    return res.status(403).json({ error: 'Delete code required' });
  }
  if (!hasDestructiveConfirmForGroup(req, DEFAULT_SITE_GROUP_SLUG)) {
    return res.status(403).json({ error: 'Destructive confirmation code required' });
  }
  if (backupJobPromise || restoreJobPromise) {
    return res.status(409).json({ error: 'Another backup or restore job is already in progress' });
  }
  const backupId = String(req.params.backupId || '').trim();
  const target = String(req.body?.target || 'both').trim().toLowerCase();
  const confirmBackupId = String(req.body?.confirmBackupId || '').trim();
  if (!['primary', 'secondary', 'both'].includes(target)) {
    return res.status(400).json({ error: 'target must be primary, secondary, or both' });
  }
  if (!backupId || confirmBackupId !== backupId) {
    return res.status(400).json({ error: 'confirmBackupId must exactly match the selected backup id' });
  }
  try {
    restoreJobPromise = (async () => {
      await loadBackupManifest(backupId);
      const backupDir = path.join(BACKUP_ROOT, backupId);
      const results = {};
      if (target === 'primary' || target === 'both') {
        const snapshot = await loadDatabaseSnapshotFile(path.join(backupDir, 'primary-db.ejson.gz'));
        results.primary = await restoreDatabaseFromSnapshot(mongoose.connection, snapshot, 'primary');
      }
      if (target === 'secondary' || target === 'both') {
        const secondaryConn = getSecondaryConn();
        if (!secondaryConn) throw new Error('Secondary database connection is unavailable');
        const snapshot = await loadDatabaseSnapshotFile(path.join(backupDir, 'secondary-db.ejson.gz'));
        results.secondary = await restoreDatabaseFromSnapshot(secondaryConn, snapshot, 'secondary');
      }
      return results;
    })();
    const results = await restoreJobPromise;
    return res.json({
      ok: true,
      message: `Restore completed for ${target}`,
      backupId,
      target,
      results,
    });
  } catch (e) {
    return res.status(500).json({ error: e.message });
  } finally {
    restoreJobPromise = null;
  }
});

app.get('/api/admin/backups/:backupId/files/:fileName', async (req, res) => {
  if (!isMainSiteAdminRequest(req)) {
    return res.status(403).json({ error: 'Forbidden' });
  }
  const backupId = String(req.params.backupId || '').trim();
  const fileName = String(req.params.fileName || '').trim();
  if (!isSafeBackupSegment(backupId) || !isSafeBackupSegment(fileName)) {
    return res.status(400).json({ error: 'Invalid backup path' });
  }
  const filePath = path.join(BACKUP_ROOT, backupId, fileName);
  try {
    await fsp.access(filePath);
    return res.download(filePath);
  } catch (_err) {
    return res.status(404).json({ error: 'Backup file not found' });
  }
});

/* Admin - Send Custom Message to All Subscribers */
app.post('/api/admin/send-custom-message', async (req, res) => {
  const { code, subject, message } = req.body;
  const groupSlug = getGroupSlug(req);
  
  if (!isSiteAdminCode(code, groupSlug)) {
    return res.status(403).json({ error: 'Forbidden' });
  }
  
  if (!subject || !message) {
    return res.status(400).json({ error: 'Subject and message are required' });
  }
  
  try {
    if (!Subscriber) return res.status(500).json({ error: 'Subscriber model not available' });
    
    const subscribers = await Subscriber.find({ ...groupScopeFilter(groupSlug) }).sort({ createdAt: -1 }).lean();
    if (subscribers.length === 0) {
      return res.json({ count: 0, message: 'No subscribers' });
    }
    // Send email to each subscriber
    let successCount = 0;
    const errors = [];
    for (const subscriber of subscribers) {
      try {
        const unsubLink = `${SITE_URL}unsubscribe?token=${subscriber.unsubscribeToken}`;
        const htmlContent = `
          <div style="font-family: Arial, sans-serif; max-width: 600px; margin: 0 auto; padding: 20px;">
            <div style="background: linear-gradient(135deg, #1a5a1a 0%, #2d7a2d 100%); color: white; padding: 30px; border-radius: 12px 12px 0 0; text-align: center;">
              <h1 style="margin: 0; font-size: 28px;">⛳ ${subject}</h1>
            </div>
            <div style="background: white; padding: 30px; border: 2px solid #2d7a2d; border-top: none; border-radius: 0 0 12px 12px;">
              <div style="color: #1a5a1a; font-size: 16px; line-height: 1.6; white-space: pre-wrap;">${message}</div>
              <hr style="border: none; border-top: 2px solid #e5e7eb; margin: 30px 0;">
              <p style="color: #6b7280; font-size: 14px; margin: 0;">
                This message was sent to all ${esc(groupSlug)} subscribers.
              </p>
              <p style="color: #6b7280; font-size: 12px; margin: 16px 0 0 0;">
                <a href="${unsubLink}" style="color: #dc2626;">Unsubscribe from notifications</a>
              </p>
            </div>
          </div>
        `;
        
        await sendEmail(subscriber.email, subject, htmlContent);
        successCount++;
      } catch (emailError) {
        console.error(`Failed to send to ${subscriber.email}:`, emailError);
        errors.push({ email: subscriber.email, error: emailError.message });
      }
    }
    
    console.log(`Custom message sent for group ${groupSlug}: "${subject}" to ${successCount}/${subscribers.length} subscribers`);
    
    res.json({ 
      groupSlug,
      count: successCount,
      total: subscribers.length,
      errors: errors.length > 0 ? errors : undefined
    });
  } catch (e) {
    console.error('Send custom message error:', e);
    res.status(500).json({ error: e.message });
  }
});

/* ---------------- Reminder logic ---------------- */
async function sendAdminAlert(subject, htmlBody, groupSlug = DEFAULT_SITE_GROUP_SLUG) {
  const { adminEmails } = await getGroupContactTargets(groupSlug);
  if (!adminEmails || adminEmails.length === 0) {
    console.log('No admin emails configured');
    return { ok: false, reason: 'no admins' };
  }
  
  let sent = 0;
  for (const adminEmail of adminEmails) {
    try {
      await sendEmail(adminEmail, subject, frame('Admin Alert', htmlBody));
      sent++;
    } catch (e) {
      console.error(`Failed to send admin alert to ${adminEmail}:`, e.message);
    }
  }
  return { ok: true, sent };
}

function shouldSendBrsTeeTimeChangeAlert(ev = {}) {
  if (!ev) return false;
  if (normalizeGroupSlug(ev.groupSlug) !== DEFAULT_SITE_GROUP_SLUG) return false;
  if (ev.isTeamEvent) return false;
  return BRS_TEE_TIME_CHANGE_ALERT_EMAILS.length > 0;
}

async function sendBrsTeeTimeChangeAlert(action = 'added', ev = {}, teeLabel = '') {
  if (!shouldSendBrsTeeTimeChangeAlert(ev)) {
    return { ok: false, skipped: true };
  }
  const normalizedAction = String(action || '').trim().toLowerCase() === 'removed' ? 'removed' : 'added';
  const rawTeeLabel = String(teeLabel || '').trim();
  const displayTeeLabel = formatAuditSlotLabel(rawTeeLabel, false);
  const eventUrl = buildSiteEventUrl(
    ev.groupSlug,
    ev._id,
    normalizedAction === 'added' && /^\d{1,2}:\d{2}$/.test(rawTeeLabel)
      ? { time: rawTeeLabel }
      : {}
  );
  const subject = `BRS tee time ${normalizedAction}: ${ev.course || 'Course'} (${fmt.dateISO(ev.date) || 'date TBD'})`;
  const body = frame(
    `BRS Tee Time ${normalizedAction === 'added' ? 'Added' : 'Removed'}`,
    `<p>A tee time was ${normalizedAction} on the BRS tee time page.</p>
     <p><strong>Event:</strong> ${esc(ev.course || '')}</p>
     <p><strong>Date:</strong> ${esc(fmt.dateLong(ev.date) || fmt.dateISO(ev.date) || '')}</p>
     <p><strong>Tee Time:</strong> ${esc(displayTeeLabel)}</p>
     ${btn('View Event', eventUrl)}`
  );
  let sent = 0;
  for (const email of BRS_TEE_TIME_CHANGE_ALERT_EMAILS) {
    try {
      const response = await sendEmail(email, subject, body);
      if (response && response.ok) sent += 1;
    } catch (error) {
      console.error('[tee-time] Failed to send BRS tee-time alert', {
        action: normalizedAction,
        email,
        error: error && error.message ? error.message : String(error),
      });
    }
  }
  return { ok: sent > 0, sent };
}

function ymdLocalPlusDays(days=1){
  const now = new Date();
  const ymd = ymdInTZ(now, LOCAL_TZ);
  const [y,m,d] = ymd.split('-').map(Number);
  const baseUTCNoon = new Date(Date.UTC(y, m-1, d, 12, 0, 0));
  const targetUTCNoon = addDaysUTC(baseUTCNoon, days);
  return ymdInTZ(targetUTCNoon, LOCAL_TZ);
}
async function findEmptyTeeTimesForDay(daysAhead = 1, groupSlug = DEFAULT_SITE_GROUP_SLUG){
  const ymd = ymdLocalPlusDays(daysAhead); // 'YYYY-MM-DD' in local TZ
  // Robust window: include events from noon UTC previous day to noon UTC next day
  const base = new Date(ymd + 'T00:00:00' + 'Z');
  const start = new Date(base.getTime() - 12*60*60*1000); // noon previous day UTC
  const end = new Date(base.getTime() + 36*60*60*1000 - 1); // just before noon next day UTC
  const scopedGroupSlug = normalizeGroupSlug(groupSlug);
  const events = await Event.find({ ...groupScopeFilter(scopedGroupSlug), isTeamEvent: false, date: { $gte: start, $lte: end } }).lean();
  const blocks = [];
  for (const ev of events) {
    const eventDateYMD = fmt.dateISO(ev.date);
    const empties = [];
    const malformed = [];
    for (const tt of (ev.teeTimes || [])) {
      if (!Array.isArray(tt.players)) {
        empties.push(fmt.tee(tt.time||''));
        malformed.push({ time: tt.time, players: tt.players });
      } else if (tt.players.length === 0) {
        empties.push(fmt.tee(tt.time||''));
      }
    }
    console.log('[reminder-check]', {
      eventId: ev._id,
      course: ev.course,
      eventDate: ev.date,
      eventDateYMD,
      ymd,
      teeTimes: (ev.teeTimes||[]).map(tt => ({ time: tt.time, players: Array.isArray(tt.players) ? tt.players.length : 'MALFORMED', rawPlayers: tt.players })),
      empties,
      malformed
    });
    if (empties.length) {
      blocks.push({ course: ev.course||'Course', dateISO: eventDateYMD, dateLong: fmt.dateLong(ev.date), empties });
    }
  }
  return blocks;
}


async function runReminderIfNeeded(label, daysAhead = 1, groupSlug = DEFAULT_SITE_GROUP_SLUG){
  const scopedGroupSlug = normalizeGroupSlug(groupSlug);
  const blocks = await findEmptyTeeTimesForDay(daysAhead, scopedGroupSlug);
  if (!blocks.length) {
    console.log(JSON.stringify({ t:new Date().toISOString(), level:'info', msg:'reminder-skip', reason:'no empty tees', label, daysAhead, groupSlug: scopedGroupSlug }));
    return { ok:true, sent:0, groupSlug: scopedGroupSlug };
  }
  const html = reminderEmail(blocks, { daysAhead });
  const subj = daysAhead === 2 ? 'Reminder: Empty Tee Times in 2 Days' : 'Reminder: Empty Tee Times Tomorrow';
  const res = await sendEmailToAll(subj, html, { groupSlug: scopedGroupSlug });
  console.log(JSON.stringify({ t:new Date().toISOString(), level:'info', msg:'reminder-sent', sent:res.sent, label, daysAhead, groupSlug: scopedGroupSlug }));
  return { ...res, groupSlug: scopedGroupSlug };
}

/* manual trigger: GET /admin/run-reminders?code=... */
app.get('/admin/run_reminders', async (req, res) => {
  const code = req.query.code || '';
  if (!ADMIN_DELETE_CODE || code !== ADMIN_DELETE_CODE) return res.status(403).json({ error: 'Forbidden' });
  try {
    const groupSlug = getGroupSlug(req);
    const r48 = await runReminderIfNeeded('manual-48hr', 2, groupSlug);
    const r24 = await runReminderIfNeeded('manual-24hr', 1, groupSlug);
    return res.json({ r48, r24 });
  }
  catch (e) { return res.status(500).json({ error: e.message }); }
});

/* manual trigger: GET /admin/run_brian_empty_alert?code=... */
app.get('/admin/run_brian_empty_alert', async (req, res) => {
  const code = req.query.code || '';
  if (!ADMIN_DELETE_CODE || code !== ADMIN_DELETE_CODE) return res.status(403).json({ error: 'Forbidden' });
  try {
    const result = await runBrianJonesTomorrowEmptyTeeAlert('manual', getGroupSlug(req));
    return res.json(result);
  } catch (e) {
    return res.status(500).json({ error: e.message });
  }
});



/* Admin: GET /admin/empty-tee-report?code=... */
app.get('/admin/empty-tee-report', async (req, res) => {
  const code = req.query.code || '';
  if (!ADMIN_DELETE_CODE || code !== ADMIN_DELETE_CODE) return res.status(403).json({ error: 'Forbidden' });
  try {
    const groupSlug = getGroupSlug(req);
    const now = new Date();
    const nowPlus1 = new Date(now.getTime() + 24 * 60 * 60 * 1000);
    const nowPlus2 = new Date(now.getTime() + 48 * 60 * 60 * 1000);
    const nowPlus3 = new Date(now.getTime() + 72 * 60 * 60 * 1000);
    // Only non-team events
    const events = await Event.find({ ...groupScopeFilter(groupSlug), isTeamEvent: false }).lean();
    const within1Day = [];
    const within2Days = [];
    const within3Days = [];
    for (const ev of events) {
      if (!Array.isArray(ev.teeTimes)) continue;
      for (const tt of ev.teeTimes) {
        if (!tt.time) continue;
        const [hh, mm] = tt.time.split(':');
        const eventDate = asUTCDate(ev.date);
        if (isNaN(eventDate)) continue;
        const teeDate = new Date(Date.UTC(
          eventDate.getUTCFullYear(),
          eventDate.getUTCMonth(),
          eventDate.getUTCDate(),
          parseInt(hh, 10),
          parseInt(mm, 10)
        ));
        const isEmpty = !Array.isArray(tt.players) || tt.players.length === 0;
        if (!isEmpty) continue;
        if (teeDate > now && teeDate <= nowPlus1) {
          within1Day.push({
            eventId: String(ev._id),
            course: ev.course || '',
            dateISO: fmt.dateISO(ev.date),
            dateLong: fmt.dateLong(ev.date),
            teeTime: tt.time
          });
        } else if (teeDate > nowPlus1 && teeDate <= nowPlus2) {
          within2Days.push({
            eventId: String(ev._id),
            course: ev.course || '',
            dateISO: fmt.dateISO(ev.date),
            dateLong: fmt.dateLong(ev.date),
            teeTime: tt.time
          });
        } else if (teeDate > nowPlus2 && teeDate <= nowPlus3) {
          within3Days.push({
            eventId: String(ev._id),
            course: ev.course || '',
            dateISO: fmt.dateISO(ev.date),
            dateLong: fmt.dateLong(ev.date),
            teeTime: tt.time
          });
        }
      }
    }
    res.json({
      ok: true,
      groupSlug,
      within1Day,
      within2Days,
      within3Days,
      counts: {
        within1Day: within1Day.length,
        within2Days: within2Days.length,
        within3Days: within3Days.length
      }
    });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});


/* Verify golf course data quality: GET /admin/verify-courses?code=... */
app.get('/admin/verify-courses', async (req, res) => {
  if (!isMainSiteAdminRequest(req)) return res.status(403).json({ error: 'Forbidden' });
  
  if (!GOLF_API_KEY && !GOLF_API_KEY_BACKUP) {
    return res.json({ 
      message: 'Using fallback course list (no API keys)', 
      courses: [],
      issues: 0 
    });
  }
  
  try {
    const url = `${GOLF_API_BASE}/search?search_query=Virginia`;
    const { response, keyUsed } = await fetchGolfAPI(url);
    
    if (!response.ok) {
      throw new Error(`Golf API error: ${response.status}`);
    }
    
    const data = await response.json();
    const validationResults = (data.courses || [])
      .slice(0, 50) // Limit to 50 for verification
      .filter(c => c.club_name || c.course_name)
      .map(c => {
        const course = {
          id: c.id,
          name: c.club_name || c.course_name || 'Unknown',
          city: c.location?.city || null,
          state: c.location?.state || null,
          phone: null,
          website: null,
          holes: 18,
          par: null
        };
        const validation = validateCourseData(course);
        return {
          course,
          valid: validation.isValid,
          issues: validation.issues
        };
      });
    
    const coursesWithIssues = validationResults.filter(r => !r.valid);
    
    return res.json({
      message: `Verified ${validationResults.length} courses from Golf API (using ${keyUsed} key)`,
      totalCourses: validationResults.length,
      coursesWithIssues: coursesWithIssues.length,
      issues: coursesWithIssues,
      keyUsed: keyUsed,
      timestamp: new Date().toISOString()
    });
  } catch (e) {
    console.error('Golf course verification error:', e);
    return res.status(500).json({ error: e.message });
  }
});

/* Helper: refresh weather for events in next 7 days */
async function refreshWeatherForUpcomingEvents(groupSlug = DEFAULT_SITE_GROUP_SLUG) {
  try {
    const scopedGroupSlug = normalizeGroupSlug(groupSlug);
    const now = new Date();
    const sevenDaysFromNow = new Date(now.getTime() + 7 * 24 * 60 * 60 * 1000);
    
    const events = await Event.find({
      ...groupScopeFilter(scopedGroupSlug),
      date: { $gte: now, $lte: sevenDaysFromNow }
    });
    
    let updated = 0;
    for (const ev of events) {
      try {
        const weatherData = await fetchWeatherForEvent(ev);
        assignWeatherToEvent(ev, weatherData);
        await ev.save();
        updated++;
      } catch (e) {
        console.error(`Weather refresh failed for event ${ev._id}:`, e.message);
      }
    }
    
    console.log(JSON.stringify({ t:new Date().toISOString(), level:'info', msg:'weather-refresh', updated, total: events.length, groupSlug: scopedGroupSlug }));
    return { ok: true, updated, total: events.length, groupSlug: scopedGroupSlug };
  } catch (e) {
    console.error('Weather refresh error:', e);
    return { ok: false, error: e.message };
  }
}

/* Scheduler for reminders, admin alerts, and weather refresh
   Only enable when running as the entry point (not when imported by tests)
   and when ENABLE_SCHEDULER is not explicitly disabled. */
if (require.main === module && !SCHEDULER_ENV_DISABLED) {
  const lastRunForYMD24ByGroup = new Map();
  const lastRunForYMD48ByGroup = new Map();
  const lastBrianAlertForYMDByGroup = new Map();
  const lastNearlyFullAlertForYMDByGroup = new Map();
  const lastAdminCheckHourByGroup = new Map();
  const lastWeatherRefreshHourByGroup = new Map();
  let lastMonthlyBackupKey = null;
  let lastWeeklyBackupKey = null;
  let lastDailyBackupKey = null;
  let lastSchedulerDisabledLogHour = null;

  setInterval(async () => {
    try {
      const now = new Date();
      const parts = new Intl.DateTimeFormat('en-US', { timeZone: LOCAL_TZ, hour:'2-digit', minute:'2-digit', hour12:false }).format(now).split(':');
      const hour = Number(parts[0]), minute = Number(parts[1]);
      const enabledGroups = [];
      for (const groupSlug of await getAllManagedGroupSlugs()) {
        if (await areSchedulerJobsEnabled(groupSlug)) {
          enabledGroups.push(groupSlug);
        }
      }
      if (!enabledGroups.length) {
        if (lastSchedulerDisabledLogHour !== hour) {
          lastSchedulerDisabledLogHour = hour;
          console.log(JSON.stringify({ t:new Date().toISOString(), level:'info', msg:'scheduler-paused', reason:'admin-disabled' }));
        }
        return;
      }
      lastSchedulerDisabledLogHour = null;
      const backupSettings = await getBackupSettings();
      const todayLocalYMD = ymdInTZ(now, LOCAL_TZ);
      const ymdTomorrow = ymdLocalPlusDays(1);
      const ymd48 = ymdLocalPlusDays(2);
      const todayParts = todayLocalYMD.split('-').map(Number);
      const dayOfMonth = Number(todayParts[2] || 0);
      const currentMonthKey = monthKeyInTZ(now, LOCAL_TZ);
      const currentWeekKey = weekKeyInTZ(now, LOCAL_TZ);
      for (const groupSlug of enabledGroups) {
        const emailRules = await getScheduledEmailRules(groupSlug);

        if (emailRules.brianTomorrowEmptyClubAlert && hour === 16 && minute === 0 && lastBrianAlertForYMDByGroup.get(groupSlug) !== ymdTomorrow) {
          lastBrianAlertForYMDByGroup.set(groupSlug, ymdTomorrow);
          if (await claimScheduledJobRunOnce(groupSlug, 'brianTomorrowEmptyClubAlert', ymdTomorrow)) {
            const result = await runBrianJonesTomorrowEmptyTeeAlert('auto-16:00', groupSlug);
            console.log(JSON.stringify({ t:new Date().toISOString(), level:'info', msg:'brian-empty-alert-complete', targetYMD: ymdTomorrow, result, groupSlug }));
          }
        }

        if (hour === 17 && minute === 0) {
          if (emailRules.reminder48Hour && lastRunForYMD48ByGroup.get(groupSlug) !== ymd48) {
            lastRunForYMD48ByGroup.set(groupSlug, ymd48);
            if (await claimScheduledJobRunOnce(groupSlug, 'reminder48Hour', ymd48)) {
              await runReminderIfNeeded('auto-17:00-48hr', 2, groupSlug);
            }
            if (groupSlug === 'seniors' && await claimScheduledJobRunOnce(groupSlug, 'seniorsRegistrantReminder48Hour', ymd48)) {
              await sendSeniorsTwoDayRegistrantReminders(groupSlug);
            }
          }
          if (emailRules.reminder24Hour && lastRunForYMD24ByGroup.get(groupSlug) !== todayLocalYMD) {
            lastRunForYMD24ByGroup.set(groupSlug, todayLocalYMD);
            if (await claimScheduledJobRunOnce(groupSlug, 'reminder24Hour', todayLocalYMD)) {
              await runReminderIfNeeded('auto-17:00-24hr', 1, groupSlug);
            }
          }
          if (emailRules.nearlyFullTeeTimes && lastNearlyFullAlertForYMDByGroup.get(groupSlug) !== todayLocalYMD) {
            lastNearlyFullAlertForYMDByGroup.set(groupSlug, todayLocalYMD);
            if (await claimScheduledJobRunOnce(groupSlug, 'nearlyFullTeeTimes', todayLocalYMD)) {
              await alertNearlyFullTeeTimes(groupSlug);
            }
          }
        }

        if (emailRules.adminEmptyTeeAlerts && [0, 6, 12, 18].includes(hour) && minute === 0 && lastAdminCheckHourByGroup.get(groupSlug) !== hour) {
          lastAdminCheckHourByGroup.set(groupSlug, hour);
          if (await claimScheduledJobRunOnce(groupSlug, 'adminEmptyTeeAlerts', `${todayLocalYMD}:${hour}`)) {
            const result = await checkEmptyTeeTimesForAdminAlert(groupSlug);
            console.log(JSON.stringify({ t:new Date().toISOString(), level:'info', msg:'admin-check-complete', result, groupSlug }));
          }
          if (hour === 0) lastAdminCheckHourByGroup.delete(groupSlug);
        }

        if (hour % 2 === 0 && minute === 0 && lastWeatherRefreshHourByGroup.get(groupSlug) !== hour) {
          lastWeatherRefreshHourByGroup.set(groupSlug, hour);
          await refreshWeatherForUpcomingEvents(groupSlug);
          if (hour === 0) lastWeatherRefreshHourByGroup.delete(groupSlug);
        }
      }

      // Automated backups and retention cleanup
      if (
        backupSettings.monthlyEnabled
        && dayOfMonth === Number(backupSettings.monthlyDay)
        && hour === Number(backupSettings.monthlyHour)
        && minute === Number(backupSettings.monthlyMinute)
        && lastMonthlyBackupKey !== currentMonthKey
        && !backupJobPromise
        && !restoreJobPromise
      ) {
        lastMonthlyBackupKey = currentMonthKey;
        try {
          backupJobPromise = createAdminBackupBundle();
          const manifest = await backupJobPromise;
          console.log(JSON.stringify({ t:new Date().toISOString(), level:'info', msg:'monthly-backup-complete', backupId: manifest.id, currentMonthKey }));
        } catch (error) {
          await recordBackupFailure(error).catch(() => {});
          console.error('monthly backup error', error);
        } finally {
          backupJobPromise = null;
        }
      }

      if (
        backupSettings.weeklyEnabled
        && now.getDay() === Number(backupSettings.weeklyDay)
        && hour === Number(backupSettings.weeklyHour)
        && minute === Number(backupSettings.weeklyMinute)
        && lastWeeklyBackupKey !== currentWeekKey
        && !backupJobPromise
        && !restoreJobPromise
      ) {
        lastWeeklyBackupKey = currentWeekKey;
        try {
          backupJobPromise = createAdminBackupBundle();
          const manifest = await backupJobPromise;
          console.log(JSON.stringify({ t:new Date().toISOString(), level:'info', msg:'weekly-backup-complete', backupId: manifest.id, currentWeekKey }));
        } catch (error) {
          await recordBackupFailure(error).catch(() => {});
          console.error('weekly backup error', error);
        } finally {
          backupJobPromise = null;
        }
      }

      if (
        backupSettings.dailyEnabled
        && monthInActiveSeason(backupSettings, now, LOCAL_TZ)
        && hour === Number(backupSettings.dailyHour)
        && minute === Number(backupSettings.dailyMinute)
        && lastDailyBackupKey !== todayLocalYMD
        && !backupJobPromise
        && !restoreJobPromise
      ) {
        lastDailyBackupKey = todayLocalYMD;
        try {
          backupJobPromise = createAdminBackupBundle();
          const manifest = await backupJobPromise;
          console.log(JSON.stringify({ t:new Date().toISOString(), level:'info', msg:'daily-backup-complete', backupId: manifest.id, todayLocalYMD }));
        } catch (error) {
          await recordBackupFailure(error).catch(() => {});
          console.error('daily backup error', error);
        } finally {
          backupJobPromise = null;
        }
      }
    } catch (e) {
      console.error('scheduler tick error', e);
    }
  }, 60 * 1000); // check once per minute

  console.log(JSON.stringify({
    t: new Date().toISOString(),
    level: 'info',
    msg: 'scheduler-enabled',
    emptyTeeAlertHourLocal: 16,
    reminderHourLocal: 17,
    reminderWindowsHours: [24, 48],
    adminAlertIntervalHours: 6,
    weatherRefreshIntervalHours: 2,
    backups: 'scheduled-by-backup-settings',
  }));
}

if (require.main === module) {
  app.listen(PORT, () => console.log(JSON.stringify({ t:new Date().toISOString(), level:'info', msg:'listening', port:PORT })));
}
module.exports = app;
// Export helpers for testing
module.exports.nextTeamNameForEvent = nextTeamNameForEvent;
module.exports.nextTeeTimeForEvent = nextTeeTimeForEvent;
module.exports.buildInitialGroupedSlots = buildInitialGroupedSlots;
module.exports.buildWeekendSkinsPopsDraw = buildWeekendSkinsPopsDraw;
module.exports.skinsPopsUnlockAt = skinsPopsUnlockAt;
module.exports.weekendGameEligibleEvent = weekendGameEligibleEvent;
module.exports.buildAuditMessage = buildAuditMessage;
module.exports.buildLegacyTeeAuditEntry = buildLegacyTeeAuditEntry;
module.exports.buildAdminTeeTimeAuditEntries = buildAdminTeeTimeAuditEntries;
module.exports.buildEventUpdateAuditMessage = buildEventUpdateAuditMessage;
module.exports.buildEventIcs = buildEventIcs;
module.exports.eventCalendarTiming = eventCalendarTiming;
module.exports.buildEventsIcs = buildEventsIcs;
module.exports.clubCancelCcRecipientsForEvent = clubCancelCcRecipientsForEvent;
module.exports.enrichCourseInfo = enrichCourseInfo;
module.exports.resolveWeatherCoordinates = resolveWeatherCoordinates;
module.exports.fetchWeatherForEvent = fetchWeatherForEvent;
module.exports.getTeeTimeAuditWindowStart = getTeeTimeAuditWindowStart;
module.exports.shouldSendBrsTeeTimeChangeAlert = shouldSendBrsTeeTimeChangeAlert;
module.exports.parseYearMonthRange = parseYearMonthRange;
module.exports.isPastYearMonth = isPastYearMonth;
module.exports.normalizeArchivedEventSnapshot = normalizeArchivedEventSnapshot;
module.exports.findArchivedSlotConflict = findArchivedSlotConflict;
module.exports.buildTeeTimeRecoveryEntries = buildTeeTimeRecoveryEntries;
