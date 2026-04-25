# Render Integration Snapshot

This folder mirrors the Tin Cup code that currently powers the live Render-integrated site in `tee-time-brs-3-2-26`.

`main-repo-snapshot/` preserves the current file layout from that app so the full Tin Cup feature set is stored here in one place without breaking the live site:

- `public/tin-cup/` mobile and browser pages
- `public/assets/tin-cup.png`
- `routes/trips.js` for live Tin Cup trip endpoints
- `services/tinCupLiveService.js` scoring and leaderboard engine
- `models/Trip*.js` for trip, participant, and audit persistence
- `scripts/seed_tin_cup_trip.js`
- `tests/test_tin_cup_live_service.js`
- `tests/e2e_tin_cup_browser.js`
- `docs/tin-cup-spreadsheet-match-map.md`
- `server.js` and `package.json` snapshots for integration context

The live Render app remains the runtime owner today. This repo now acts as the dedicated Tin Cup repository and source bundle for future extraction or shared-module work.

Use [main-repo-snapshot/.env.example](main-repo-snapshot/.env.example) for the Tin Cup-specific shared env surface only. The broader BRS site runtime still owns additional non-Tin-Cup env vars in the main `tee-time-brs` repo.
