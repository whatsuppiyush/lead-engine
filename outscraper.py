"""Paid fallback: companies on a list that still have no contact in Neon -> Outscraper domain contacts -> verify -> Neon.
Runs after the free path (site crawl + SMTP) so it only pays for what that missed. Dry run unless --apply.

  .venv/bin/python outscraper.py --signal mdb_week --limit 5            # what it would look up
  .venv/bin/python outscraper.py --signal mdb_week --apply --max-usd 1  # spend, capped
"""
import argparse, json, urllib.error, urllib.parse, urllib.request
import core
from core import DB, ENV, UA, Verifier, is_role, save_lead

USD_PER_DOMAIN = 0.003   # list price; the real charge shows in the account balance


def todo(signal, limit):
    return DB.execute("""
      select c.domain, c.name, c.lists, c.topics, c.signals from companies c
      where c.signals ? %s and not exists (select 1 from leads l where l.company_domain = c.domain)
        and coalesce((c.signals->>'outscraper_at')::timestamptz, 'epoch') < now() - interval '90 days'   -- asked recently: skip
      order by c.first_seen_at desc limit %s""", (signal, limit)).fetchall()


def lookup(domain):
    q = urllib.parse.urlencode({'query': domain, 'async': 'false', 'contactsPerCompany': 5, 'emailsPerContact': 3})
    req = urllib.request.Request(f'https://api.outscraper.cloud/contacts-and-leads?{q}',
                                 headers={'X-API-KEY': ENV['OUTSCRAPER_API_KEY'], 'User-Agent': UA})
    try:
        body = json.load(urllib.request.urlopen(req, timeout=120))
    except urllib.error.HTTPError as e:
        print(f'  {domain}: HTTP {e.code} {e.read()[:120]!r}'); return None
    except Exception as e:
        print(f'  {domain}: {e!r}'); return None
    data = body.get('data') or []
    recs = [r for x in data for r in (x if isinstance(x, list) else [x]) if isinstance(r, dict)]
    rec = next((r for r in recs if (r.get('domain') or '').removeprefix('www.').lower() == domain), recs[0] if recs else {})
    out = {}   # email -> (name, title)
    for e in rec.get('emails') or []:
        if e.get('value'): out.setdefault(e['value'].strip().lower(), ('', ''))
    for c in rec.get('contacts') or []:
        for e in c.get('emails') or []:
            if e.get('value'): out[e['value'].strip().lower()] = (c.get('full_name') or '', c.get('title') or '')
    return out


def balance():
    try:
        req = urllib.request.Request('https://api.outscraper.cloud/profile/balance', headers={'X-API-KEY': ENV['OUTSCRAPER_API_KEY']})
        return float(json.load(urllib.request.urlopen(req, timeout=30)).get('balance'))
    except Exception:
        return None


def run(signal, limit, apply, max_usd, per_company=2):
    if not ENV.get('OUTSCRAPER_API_KEY'):
        print('OUTSCRAPER_API_KEY not set, skipping'); return
    rows = todo(signal, limit)
    print(f'{len(rows)} companies with signal {signal!r} and no contact in Neon' + ('' if apply else ' (dry run)'))
    if not apply:
        for r in rows[:20]: print('  ', r['domain'], '-', r['name'])
        return
    b0 = balance()
    run_id = core.start_run('outscraper', f'outscraper:{signal}')
    ver, spent, st = Verifier(run_id), 0.0, {'looked_up': 0, 'with_contact': 0, 'leads': 0}
    try:
        for r in rows:
            if spent + USD_PER_DOMAIN > max_usd:
                print(f'  budget ${max_usd} reached'); break
            d = r['domain']
            found = lookup(d); spent += USD_PER_DOMAIN; st['looked_up'] += 1
            core.log_spend(run_id, 'outscraper', 1, USD_PER_DOMAIN)
            # own-domain addresses first, people before shared inboxes
            cands = sorted(found or {}, key=lambda e: (not e.endswith('@' + d), is_role(e), not found[e][0]))[:per_company * 2]
            saved = 0
            for e in cands:
                if saved >= per_company: break
                res = ver.check(e.split('@')[1], [e], stop_at_first=False)[0]
                if res.get(e) == 'invalid': continue
                status = 'role' if is_role(e) else (res.get(e) if res.get(e) in ('valid', 'catch_all') else 'unverified')
                name, title = found[e]
                first, last = core.person_name(name, r['name'], e)   # Outscraper names can be brands too
                products, why = core.fit(r['lists'] or [], r['topics'] or [], None, r['signals'] or {}, False)
                save_lead({'email': e, 'first_name': first or None, 'last_name': last or None, 'company': r['name'],
                           'website': f'https://{d}', 'domain': d, 'title': title or None, 'linkedin': None,
                           'x': (r['signals'] or {}).get('x'), 'audience': 'B2B founders', 'status': status, 'fit': products,
                           'fit_reason': why, 'contact_type': 'company' if is_role(e) else 'founder', 'origin': 'outscraper',
                           'email_type': 'role inbox' if is_role(e) else 'personal', 'launch_date': None, 'tagline': None},
                          'engine:outscraper', d, {'outscraper': found[e], 'signal': signal}, run_id)
                saved += 1; st['leads'] += 1; st[f'lead:{status}'] = st.get(f'lead:{status}', 0) + 1
            st['with_contact'] += bool(saved)
            DB.execute("""update companies set email_lookup_at = now(), email_lookup_result = %s,
                          signals = coalesce(signals, '{}') || jsonb_build_object('outscraper_at', now()::text) where domain = %s""",
                       ('found' if saved else 'none', d))
            print(f"  {d}: {len(found or {})} from outscraper, {saved} saved", flush=True)
    finally:
        b1 = balance()
        st['balance_before'], st['balance_after'] = b0, b1
        core.finish_run(run_id, st, ok=True)
        print(json.dumps(st, indent=1))


if __name__ == '__main__':
    ap = argparse.ArgumentParser()
    ap.add_argument('--signal', default='mdb_week', help='companies.signals key that marks the list (mdb_week = marketingdb)')
    ap.add_argument('--limit', type=int, default=500)
    ap.add_argument('--apply', action='store_true')
    ap.add_argument('--max-usd', type=float, default=1.0)
    a = ap.parse_args()
    run(a.signal, a.limit, a.apply, a.max_usd)
