// Morning report: yesterday's scrape, source by source, plus everything that broke or is running out.
// Runs inside the Worker so it still arrives when GitHub or the scraper itself is the thing that broke.
// Secrets: NEON_READONLY_URL, TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, APIFY_TOKENS, REOON_API_KEY, GITHUB_TOKEN.

const SOURCES = [   // engine_runs.list, leads.first_source prefix, short name, when daily.sh runs it (UTC weekday 1=Mon)
  ['producthunt', 'engine:producthunt', 'PH', 'daily'],
  ['chrome', 'engine:chrome', 'Chrome', 'daily'],
  ['shopify', 'engine:shopify', 'Shopify', 'daily'],
  ['showhn', 'engine:showhn', 'ShowHN', 'daily'],
  ['play', 'engine:play', 'Play', 'daily'],
  ['yelp', 'engine:yelp', 'Yelp', 'daily'],
  ['launches', 'engine:launches', 'Launch', [1]],
  ['stores', 'engine:stores', 'Stores', [1, 4]],
  ['revenue', 'engine:trustmrr', 'TrustMRR', [2]],
  ['hiring', 'engine:wellfound', 'Hiring', [3, 6]],
  ['firefox', 'engine:firefox', 'Firefox', [7]],
  ['funded', 'engine:yc', 'YC', 'first'],
];
const APIFY_MIN_USD = 3, REOON_MIN = 5000, MIN_EMAILS = 100;
const esc = (s) => String(s).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');

async function sql(env, query, params = []) {
  const host = new URL(env.NEON_READONLY_URL.replace(/^postgres(ql)?:/, 'https:')).host;
  const r = await fetch(`https://${host}/sql`, {
    method: 'POST',
    headers: { 'Neon-Connection-String': env.NEON_READONLY_URL, 'content-type': 'application/json' },
    body: JSON.stringify({ query, params }),
  });
  const d = await r.json();
  if (!r.ok) throw new Error(`neon: ${d.message || r.status}`);
  return d.rows;
}

const scheduled = (when, day) => when === 'daily' || (when === 'first' ? day.getUTCDate() === 1 : when.includes(day.getUTCDay() || 7));

async function githubRun(env, day) {
  const since = day.toISOString().slice(0, 10) + 'T11:00:00Z';
  const r = await fetch(`https://api.github.com/repos/whatsuppiyush/pp-daily-sync/actions/runs?created=>=${since}&per_page=30`, {
    headers: { authorization: `Bearer ${env.GITHUB_TOKEN}`, accept: 'application/vnd.github+json', 'user-agent': 'ph-resolve' },
  });
  if (!r.ok) throw new Error(`github: ${r.status}`);
  const runs = (await r.json()).workflow_runs.filter((x) => x.conclusion !== 'cancelled' && x.created_at < since.slice(0, 10) + 'T23:59:59Z');
  return runs[runs.length - 1];   // the earliest one in the window is the timer's daily run
}

async function apifyLeft(env) {
  let left = 0, total = 0, n = 0;
  for (const t of (env.APIFY_TOKENS || '').split(',').map((s) => s.trim()).filter(Boolean)) {
    const d = (await (await fetch(`https://api.apify.com/v2/users/me/limits?token=${t}`)).json()).data;
    if (!d) continue;
    left += d.limits.maxMonthlyUsageUsd - d.current.monthlyUsageUsd; total += d.limits.maxMonthlyUsageUsd; n++;
  }
  return { left, total, n };
}

async function reoonLeft(env) {
  const d = await (await fetch(`https://emailverifier.reoon.com/api/v1/check-account-balance/?key=${env.REOON_API_KEY}`)).json();
  return Number(d.remaining_daily_credits || 0) + Number(d.remaining_instant_credits || 0);
}

export async function buildReport(env, now = new Date()) {
  const day = new Date(now.getTime() - 24 * 3600e3);           // the daily run being reported on
  const since = day.toISOString().slice(0, 10) + 'T11:00:00Z';  // from yesterday's 11:00 UTC to now
  const alerts = [], lines = [];
  const label = day.toLocaleDateString('en-GB', { weekday: 'short', day: 'numeric', month: 'short', timeZone: 'UTC' });

  // 1. did the GitHub run happen and finish?
  try {
    const run = await githubRun(env, day);
    if (!run) alerts.push('No scraper run yesterday. The daily timer did not start it.');
    else if (run.status !== 'completed') alerts.push(`Yesterday's run is still ${run.status} (${run.html_url})`);
    else if (run.conclusion !== 'success') alerts.push(`Yesterday's run ended in ${run.conclusion}: ${run.html_url}`);
    else lines.push(`Run: ✅ finished in ${Math.round((new Date(run.updated_at) - new Date(run.run_started_at)) / 60000)} min`);
  } catch (e) { alerts.push(`Could not check GitHub: ${e.message}`); }

  // 2. source by source from Neon
  let table = '', totals = { scraped: 0, emails: 0, valid: 0, safe: 0 }, pending = null;
  try {
    const runs = await sql(env, `select list, ok, finished_at is not null as done,
        coalesce((stats->>'items')::int, 0) as items, stats->>'error' as error
      from engine_runs where started_at >= $1`, [since]);
    const leads = await sql(env, `select split_part(first_source, ':', 2) as src, count(*)::int as emails,
        count(*) filter (where email_status = 'valid')::int as valid,
        count(*) filter (where email_status = 'catch_all')::int as catch_all,
        count(*) filter (where email_status = 'role')::int as role,
        count(*) filter (where reoon_safe)::int as safe,
        count(*) filter (where reoon_checked_at is null)::int as unchecked
      from leads where first_source like 'engine:%' and created_at >= $1 group by 1`, [since]);
    pending = (await sql(env, `select count(*)::int as n from leads l where reoon_safe
        and not exists (select 1 from instantly_pushes p where p.email = l.email)
        and not exists (select 1 from suppression s where s.email = l.email)`))[0].n;

    const row = (c) => c.map((v, i) => (i ? String(v).padStart(6) : String(v).padEnd(8))).join('');
    const out = [row(['Source', 'Scrap', 'Email', 'Valid', 'Catch', 'Role'])];
    let unchecked = 0;
    for (const [list, prefix, name, when] of SOURCES) {
      const rs = runs.filter((r) => r.list === list || (list === 'chrome' && r.list === 'chrome-store'));
      const due = scheduled(when, day);
      if (!rs.length) { if (due) { alerts.push(`${name} did not run.`); out.push(row([name, '-', '-', '-', '-', '-'])); } continue; }
      for (const r of rs.filter((r) => r.ok === false || !r.done)) alerts.push(`${name} ${r.ok === false ? 'failed' : 'never finished'}${r.error ? ': ' + r.error.slice(0, 80) : ''}`);
      const l = leads.find((x) => prefix === 'engine:' + x.src) || { emails: 0, valid: 0, catch_all: 0, role: 0, safe: 0, unchecked: 0 };
      const scraped = rs.reduce((s, r) => s + r.items, 0);
      if (due && scraped === 0) alerts.push(`${name} ran but scraped nothing.`);
      out.push(row([name, scraped, l.emails, l.valid, l.catch_all, l.role]));
      totals.scraped += scraped; totals.emails += l.emails; totals.valid += l.valid; totals.safe += l.safe; unchecked += l.unchecked;
    }
    out.push(row(['Total', totals.scraped, totals.emails, totals.valid, '', '']));
    table = out.join('\n');
    if (totals.emails === 0) alerts.push('Nothing was scraped: 0 new emails.');
    else if (totals.emails < MIN_EMAILS) alerts.push(`Only ${totals.emails} new emails (normally 500+).`);
    if (!runs.some((r) => r.list === 'verify' && r.ok)) alerts.push('The Reoon email check did not run.');
    else if (unchecked > 50) alerts.push(`${unchecked} new emails were never checked by Reoon.`);
  } catch (e) { alerts.push(`Could not read the lead database: ${e.message}`); }

  // 3. credits
  const credits = [];
  try {
    const a = await apifyLeft(env);
    credits.push(`Apify: $${a.left.toFixed(2)} of $${a.total.toFixed(0)} left (${a.n} accounts)`);
    if (a.left < APIFY_MIN_USD) alerts.push(`Apify is nearly out: $${a.left.toFixed(2)} left.`);
  } catch (e) { alerts.push(`Could not check Apify: ${e.message}`); }
  try {
    const r = await reoonLeft(env);
    credits.push(`Reoon: ${r.toLocaleString('en-US')} credits left`);
    if (r < REOON_MIN) alerts.push(`Reoon credits are low: ${r} left.`);
  } catch (e) { alerts.push(`Could not check Reoon: ${e.message}`); }

  const head = alerts.length ? `🔴 <b>Lead engine · ${label}: ${alerts.length} problem${alerts.length > 1 ? 's' : ''}</b>` : `🟢 <b>Lead engine · ${label}: all good</b>`;
  const msg = [head];
  if (alerts.length) msg.push(alerts.map((a) => '• ' + esc(a)).join('\n'));
  msg.push(lines.map(esc).join('\n'));
  if (table) msg.push(`<pre>${esc(table)}</pre>`,
    `Safe to send: <b>${totals.safe}</b> · Valid: ${totals.valid} of ${totals.emails} emails` +
    (pending != null ? `\nWaiting to push to Instantly: ${pending.toLocaleString('en-US')}` : ''));
  msg.push(credits.map(esc).join('\n'));
  return { ok: !alerts.length, text: msg.filter(Boolean).join('\n\n') };
}

export async function sendTelegram(env, text) {
  const r = await fetch(`https://api.telegram.org/bot${env.TELEGRAM_BOT_TOKEN}/sendMessage`, {
    method: 'POST', headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ chat_id: env.TELEGRAM_CHAT_ID, text, parse_mode: 'HTML', disable_web_page_preview: true }),
  });
  if (!r.ok) throw new Error(`telegram: ${r.status} ${await r.text()}`);
}
