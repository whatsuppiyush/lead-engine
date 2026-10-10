"""Check step."""
import argparse, json, time, urllib.request
from core import DB, ENV, log_spend, start_run, finish_run

MAP = {'safe': 'valid', 'role_account': 'role', 'catch_all': 'catch_all', 'invalid': 'invalid', 'disabled': 'invalid',
       'disposable': 'invalid', 'inbox_full': 'invalid', 'spamtrap': 'invalid', 'unknown': 'unverified'}
API = 'https://emailverifier.reoon.com/api/v1'


def todo(max_n, recheck_days):
    return [r['email'] for r in DB.execute(f"""
      select email from leads
      where (first_source like 'engine:%%' or first_source like 'gumloop:%%' or email in
             (select email from lead_sources where source like 'engine:%%'))
        and coalesce(email_status, '') <> 'invalid'
        and (reoon_checked_at is null or reoon_checked_at < now() - interval '{int(recheck_days)} days')
        and not exists (select 1 from suppression s where s.email = leads.email)
      order by created_at desc limit %s""", (max_n,)).fetchall()]


def balance():
    d = json.load(urllib.request.urlopen(f"{API}/check-account-balance/?key={ENV['REOON_API_KEY']}", timeout=30))
    return int(d.get('remaining_daily_credits') or 0) + int(d.get('remaining_instant_credits') or 0)


def run(max_n=5000, recheck_days=60):
    emails = todo(max_n, recheck_days)
    if not emails:
        print('reoon: nothing to check'); return {'checked': 0}
    left = balance()
    if left < len(emails) + 50:
        emails = emails[:max(0, left - 50)]
        print(f'reoon: only {left} credits left, checking {len(emails)}')
    if not emails: return {'checked': 0, 'error': 'no Reoon credits'}
    run_id = start_run('verify', 'reoon')
    body = json.dumps({'name': f'pp-daily-sync {time.strftime("%m-%d %H:%M")}', 'emails': emails, 'key': ENV['REOON_API_KEY']}).encode()
    task = json.load(urllib.request.urlopen(urllib.request.Request(f'{API}/create-bulk-verification-task/', body,
                                                                    {'Content-Type': 'application/json'}), timeout=120))
    tid = task.get('task_id')
    if not tid:
        finish_run(run_id, {'error': task}, ok=False); raise SystemExit(f'reoon: could not create task: {task}')
    print(f'reoon: task {tid}, {len(emails)} emails', flush=True)
    t0 = time.time()
    while True:
        time.sleep(15)
        r = json.load(urllib.request.urlopen(f"{API}/get-result-bulk-verification-task/?key={ENV['REOON_API_KEY']}&task_id={tid}", timeout=120))
        if r.get('status') == 'completed' or time.time() - t0 > 3 * 3600: break
        print(f"  {r.get('status')} {r.get('count_checked', '?')}/{r.get('count_total', len(emails))}", flush=True)
    res = r.get('results') or {}
    stats = {'checked': len(res), 'task_id': tid}
    for email, v in res.items():
        st = v.get('status') or 'unknown'
        stats[st] = stats.get(st, 0) + 1
        DB.execute("""update leads set reoon_status = %s, reoon_safe = %s, reoon_score = %s, reoon_checked_at = now(),
                      email_status = %s, verified_at = now() where email = %s""",
                   (st, bool(v.get('is_safe_to_send')), int(v.get('overall_score') or 0) or None, MAP.get(st, 'unverified'), email.lower()))
    log_spend(run_id, 'reoon', len(res))
    finish_run(run_id, stats, ok=r.get('status') == 'completed')
    print('reoon:', json.dumps(stats))
    return stats


if __name__ == '__main__':
    ap = argparse.ArgumentParser()
    ap.add_argument('--max', type=int, default=5000)
    ap.add_argument('--recheck-days', type=int, default=60)
    a = ap.parse_args()
    run(a.max, a.recheck_days)
