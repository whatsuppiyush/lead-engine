// ph-resolve: turns Product Hunt /r/ tracking links into the product's real website.
// Product Hunt's Cloudflare returns 403 to GitHub Actions and AWS (Neon Functions); requests from a Worker get the 301.
//   POST /  Authorization: Bearer <RESOLVER_SECRET>  {"urls": [...]}   (max 40: free-plan subrequest limit is 50)
//   -> {"results": [{"url": "...", "status": 301, "location": "https://site.com/?ref=producthunt"}]}
// It also starts the daily run: the 11:30 UTC timer dispatches sync.yml, the 11:50 one dispatches again if nothing started.
//   POST /daily-status  (same bearer)  -> whether today's run has started, without starting anything
//   POST /morning-report  (same bearer)  {"send": true} -> the 09:00 IST Telegram report (no body = preview only)
//   POST /dispatch      (same bearer)  {"task": "producthunt", "args": "--day 2026-10-12"} -> starts one run (one at a time!)
import { buildReport, sendTelegram } from './report.js';

const WORKFLOW = 'https://api.github.com/repos/whatsuppiyush/pp-daily-sync/actions/workflows/sync.yml';
const gh = (env, path, init = {}) => fetch(WORKFLOW + path, {
  ...init,
  headers: { authorization: `Bearer ${env.GITHUB_TOKEN}`, accept: 'application/vnd.github+json', 'user-agent': 'ph-resolve', ...init.headers },
});

async function runsToday(env) {
  const since = new Date().toISOString().slice(0, 10) + 'T11:00:00Z';   // the daily window, so a morning backfill doesn't count
  const r = await gh(env, `/runs?created=>=${since}&per_page=20`);
  if (!r.ok) throw new Error(`list runs: ${r.status} ${await r.text()}`);
  return (await r.json()).workflow_runs.filter((x) => x.conclusion !== 'cancelled');
}

async function dispatch(env, task = 'daily', args = '', limit = '') {
  const r = await gh(env, '/dispatches', { method: 'POST', body: JSON.stringify({ ref: 'main', inputs: { task, limit, args } }) });
  if (r.status !== 204) throw new Error(`dispatch: ${r.status} ${await r.text()}`);
}

export default {
  async scheduled(event, env) {
    if (event.cron === '30 3 * * *') { await sendTelegram(env, (await buildReport(env)).text); return; }
    if (event.cron === '30 11 * * *') { await dispatch(env); console.log('daily run dispatched'); return; }
    const runs = await runsToday(env);
    if (runs.length) { console.log(`daily run already started: ${runs[0].html_url}`); return; }
    await dispatch(env); console.log('no run found at the check, dispatched again');
  },

  async fetch(req, env) {
    if (req.method !== 'POST') return Response.json({ ok: true, service: 'ph-resolve' });
    if (req.headers.get('authorization') !== `Bearer ${env.RESOLVER_SECRET}`) return Response.json({ error: 'unauthorized' }, { status: 401 });
    if (new URL(req.url).pathname === '/daily-status') {
      try {
        const runs = await runsToday(env);
        return Response.json({ started: runs.length > 0, runs: runs.map((x) => ({ status: x.status, conclusion: x.conclusion, event: x.event, created_at: x.created_at })) });
      } catch (e) { return Response.json({ error: String(e) }, { status: 502 }); }
    }
    if (new URL(req.url).pathname === '/morning-report') {
      const { send } = await req.json().catch(() => ({}));
      const report = await buildReport(env);
      if (send) await sendTelegram(env, report.text);
      return Response.json({ ...report, sent: !!send });
    }
    if (new URL(req.url).pathname === '/dispatch') {
      const { task, args, limit } = await req.json().catch(() => ({}));
      try { await dispatch(env, task || 'daily', args || '', limit || ''); return Response.json({ dispatched: task || 'daily' }); }
      catch (e) { return Response.json({ error: String(e) }, { status: 502 }); }
    }
    const { urls } = await req.json().catch(() => ({}));
    if (!Array.isArray(urls) || !urls.length || urls.length > 40) return Response.json({ error: 'urls: 1-40' }, { status: 400 });
    const one = async (url) => {
      if (!url.startsWith('https://www.producthunt.com/r/')) return { url, status: 400, location: '' };
      for (let attempt = 0; attempt < 3; attempt++) {           // Product Hunt answers 429 when pushed: back off
        try {
          const r = await fetch(url, { method: 'HEAD', redirect: 'manual', headers: { 'user-agent': 'curl/8.7' } });
          if (r.status === 429 && attempt < 2) { await new Promise((s) => setTimeout(s, 1500 * (attempt + 1))); continue; }
          return { url, status: r.status, location: r.headers.get('location') || '' };
        } catch (e) { return { url, status: 0, location: '', error: String(e) }; }
      }
    };
    const results = [];
    for (let i = 0; i < urls.length; i += 5) results.push(...await Promise.all(urls.slice(i, i + 5).map(one)));
    return Response.json({ results });
  },
};
