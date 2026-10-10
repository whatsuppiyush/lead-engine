"""Entry point."""
import argparse, collections, concurrent.futures as cf, json, sys
import core
from core import DB, Verifier, crawl, domain_of, fit, guesses, is_role, known, on_producthunt, save_company, save_lead
from sources import SOURCES

BIG_DR = 70           # authority this high = established company, not our buyer
MAX_PEOPLE = 2        # contacts to chase per company
USE_DR = '--dr' in sys.argv   # opt in to treg domain-rating lookups


def split(name):
    p = (name or '').strip().split()
    return (p[0] if p else ''), (' '.join(p[1:]) if len(p) > 1 else '')


def process(list_, items, run_id, dry=False):
    st = collections.Counter()
    for it in items:
        it['domain'] = domain_of(it.get('website'))
    doms = sorted({it['domain'] for it in items if it['domain']})
    state = known(doms)
    ph = on_producthunt(doms)
    new = [d for d in doms if state.get(d) in ('new', 'retry')]
    st['items'], st['no_website'] = len(items), sum(1 for it in items if not it['domain'])
    if items and list_ == 'producthunt': st['ph_day'] = (items[0].get('signals') or {}).get('ph_day')
    st.update(f'neon:{s}' for s in state.values())
    print(f'  {len(items)} items, {len(doms)} domains: ' + ', '.join(f'{k} {v}' for k, v in sorted(collections.Counter(state.values()).items())))
    if dry:
        return st
    dr = core.dr_lookup(new, run_id) if (new and USE_DR) else {}   # DR off by default: treg costs money; fit treats unknown DR as low
    ver = Verifier(run_id)
    print(f"  verifying with {'local SMTP (free)' if ver.smtp else f'Reoon, {ver.reoon_left} daily credits left'}")
    # crawling is the slow part and costs nothing: do it up front, in parallel, for companies we will actually work
    todo = {}
    for it in items:
        d = it['domain']
        if d and d not in todo and state.get(d) in ('new', 'retry'):
            todo[d] = it['website']
    with cf.ThreadPoolExecutor(12) as ex:
        crawled = dict(zip(todo, ex.map(crawl, todo.values())))
    seen = set()
    for it in items:
        d = it['domain']
        if not d or d in seen: continue
        seen.add(d)
        base = {'domain': d, 'name': it.get('name'), 'lists': [list_], 'topics': [t for t in it.get('topics') or [] if t],
                'signals': {**(it.get('signals') or {}),
                            'makers': [{k: p.get(k) for k in ('name', 'x', 'linkedin', 'title') if p.get(k)}
                                       for p in (it.get('people') or []) if p.get('name') not in (None, '', '[REDACTED]')]},
                'dr': dr.get(d), 'title': None}
        payload = {k: v for k, v in it.items() if not k.startswith('_') and k not in ('people',)} | {'people': it.get('people')}
        if state.get(d) in ('has_contact', 'looked_recently'):
            save_company(base, run_id); core.note_appearance(d, it['source'], it['ref'], payload, run_id)
            st['reused_free'] += 1; continue
        if dr.get(d) is not None and dr[d] >= BIG_DR and list_ in ('producthunt', 'launches', 'chrome', 'shopify'):
            save_company({**base, 'lookup_at': 'now', 'lookup_result': 'skipped_big'}, run_id)
            st['skipped_big'] += 1; continue

        listed, socials = crawled.get(d) or crawl(it['website'])
        listed = list(dict.fromkeys([e.lower() for e in it.get('emails') or []] + listed))
        people = [p for p in (it.get('people') or []) if p.get('name') and p['name'] != '[REDACTED]'][:MAX_PEOPLE]
        contacts, ca, used = [], None, set()
        for p in people:
            first = p['name'].split()[0].lower()
            mine = [e for e in listed if e.endswith('@' + d) and e.split('@')[0].startswith(first[:3]) and not is_role(e)]
            cands = list(dict.fromkeys(mine + guesses(p['name'], d)))[:4]
            if not cands: continue
            res, ca_ = ver.check(d, cands)
            ca = ca if ca_ is None else ca_
            best = next((e for e in cands if res.get(e) in ('valid', 'role')), None)
            status = res.get(best) if best else None
            if not best:
                # catch-all, or a server that won't answer (Cloudflare/registrar forwarding): keep the likeliest
                # address, flagged. A published address beats a guess. Unverified rows are never pushed.
                best = (mine or [c for c in cands if res.get(c) != 'invalid'] or [None])[0]
                status = 'catch_all' if ca_ else 'unverified'
            if best:
                contacts.append((p, best, status, 'listed' if best in mine else 'guessed')); used.add(best)
        # addresses the company publishes on its own site, not yet used: people first, then shared inboxes
        published = [e for e in listed if e.endswith('@' + d) and e not in used]
        published = sorted(published, key=is_role)[:2 if not contacts else 1]
        if published:
            res, _ = ver.check(d, published, stop_at_first=False)
            for e in published:
                if res.get(e) == 'invalid': continue
                st_ = 'role' if is_role(e) else (res.get(e) if res.get(e) in ('valid', 'catch_all') else 'unverified')
                contacts.append(({'name': ''}, e, st_, 'listed'))
        products, why = fit([list_], base['topics'], dr.get(d), base['signals'], d in ph)
        save_company({**base, 'lookup_at': 'now', 'catch_all': ca,
                      'lookup_result': 'found' if contacts else ('catch_all' if ca else 'none')}, run_id)
        for p, email, status, origin in contacts:
            f, l = split(p.get('name'))
            x = (p.get('x') or '').rstrip('/').split('/')[-1].lstrip('@')
            save_lead({'email': email, 'first_name': f or None, 'last_name': l or None, 'company': it.get('name'),
                       'website': it.get('website'), 'domain': d, 'title': p.get('title') or None,
                       'linkedin': p.get('linkedin') or next((s for s in socials if '/in/' in s), None), 'x': x or None,
                       'audience': 'B2B founders', 'status': status, 'fit': products, 'fit_reason': why,
                       'contact_type': 'founder' if p.get('name') or not is_role(email) else 'company', 'origin': origin,
                       'email_type': 'role inbox' if is_role(email) else 'personal (own domain)',
                       'launch_date': it.get('launch_date') if list_ == 'producthunt' else None,
                       'tagline': it.get('tagline') if list_ == 'producthunt' else None},
                      it['source'], it['ref'], payload, run_id)
            st[f'lead:{status}'] += 1
        st['crawled'] += 1
        st['with_contact' if contacts else 'no_contact'] += 1
    st['reoon_credits'] = ver.spent
    return st


def backfill(start, end, min_votes, budget_min):
    """Product Hunt day by day from start to end (inclusive), launches above min_votes only. Stops after budget_min
    minutes and returns the next day still to do (None when finished), so CI can hand the rest to a fresh job."""
    import datetime as dt, os, time
    t0, day, end_d = time.time(), dt.date.fromisoformat(start), dt.date.fromisoformat(end)
    while day <= end_d:
        if (time.time() - t0) / 60 > budget_min:
            break
        run_id = core.start_run('producthunt', 'backfill')
        try:
            items = SOURCES['producthunt'](run_id, day=day.isoformat(), min_votes=min_votes)
            stats = process('producthunt', items, run_id)
            core.finish_run(run_id, {**dict(stats), 'backfill': True}, ok=True)
            print(f'{day}: {dict(stats).get("items", 0)} launches > {min_votes - 1} votes, {dict(stats).get("with_contact", 0)} with contact', flush=True)
        except Exception as e:
            core.finish_run(run_id, {'error': repr(e), 'day': day.isoformat()}, ok=False)
            print(f'{day}: FAILED {e!r}', flush=True)
        day += dt.timedelta(days=1)
    nxt = day.isoformat() if day <= end_d else None
    if os.environ.get('GITHUB_OUTPUT'):
        open(os.environ['GITHUB_OUTPUT'], 'a').write(f'next_from={nxt or ""}\n')
    print('backfill next_from:', nxt or 'done')
    return nxt


def selftest():
    """Check every dependency from wherever this runs. Prints one line per check; exit 1 if a required one fails."""
    import socket, urllib.request, urllib.error
    from core import ENV, Verifier, _NOFOLLOW
    res = {}
    try: DB.execute('select 1'); res['neon'] = 'ok'
    except Exception as e: res['neon'] = f'FAIL {e}'
    try:
        body = json.dumps({'query': '{posts(first:1){edges{node{name website}}}}'}).encode()
        j = json.load(urllib.request.urlopen(urllib.request.Request('https://api.producthunt.com/v2/api/graphql', body,
                      {'Authorization': f"Bearer {ENV['PH_TOKEN']}", 'Content-Type': 'application/json'}), timeout=30))
        link = j['data']['posts']['edges'][0]['node']['website']; res['producthunt_api'] = 'ok'
        from core import resolve_ph_links
        res['ph_resolve_worker'] = 'ok -> ' + resolve_ph_links([link]).get(link, '')[:50] if ENV.get('PH_RESOLVER_URL') else 'not configured'
        try:
            _NOFOLLOW.open(urllib.request.Request(link, method='HEAD', headers={'User-Agent': 'curl/8.7'}), timeout=15)
            res['ph_redirect'] = 'no redirect'
        except urllib.error.HTTPError as e:
            res['ph_redirect'] = f"ok -> {e.headers.get('Location', '')[:60]}" if e.code in (301, 302) else f'FAIL HTTP {e.code}'
    except Exception as e: res['producthunt_api'] = f'FAIL {e}'
    res['port25_smtp'] = 'open (free verification)' if Verifier._port25() else 'blocked (Reoon used)'
    res['reoon_daily_credits'] = Verifier._reoon_balance()
    try:
        d = json.load(urllib.request.urlopen(f"https://api.apify.com/v2/users/me/limits?token={ENV['APIFY_TOKEN']}", timeout=30))['data']
        res['apify_month_usd'] = f"{d['current']['monthlyUsageUsd']:.2f} of {d['limits']['maxMonthlyUsageUsd']}"
    except Exception as e: res['apify_month_usd'] = f'FAIL {e}'
    for k, v in res.items(): print(f'{k:22} {v}')
    return 0 if all(not str(v).startswith('FAIL') for k, v in res.items() if k in ('neon', 'producthunt_api')) else 1


def migrate_gumloop():
    """Copy rows the old Gumloop agent wrote to leads.leads into the shared tables (idempotent)."""
    rows = DB.execute("select * from leads.leads where email <> ''").fetchall()
    for r in rows:
        d = domain_of(r['product_url']) or r['email'].split('@')[1]
        f, l = split(r['founder_name'])
        status = {'safe': 'valid', 'valid': 'valid', 'role_account': 'role', 'catch_all': 'catch_all', 'disabled': 'invalid',
                  'invalid': 'invalid'}.get(r['email_status'] or '', 'unverified')
        save_company({'domain': d, 'name': r['product_name'], 'lists': ['producthunt'], 'topics': [], 'signals': {'upvotes': r['upvotes']},
                      'dr': None, 'title': None}, 0)
        save_lead({'email': r['email'].lower(), 'first_name': f or None, 'last_name': l or None, 'company': r['product_name'],
                   'website': r['product_url'], 'domain': d, 'title': None, 'linkedin': r['founder_linkedin'] or None,
                   'x': (r['founder_x'] or '').rstrip('/').split('/')[-1] or None, 'audience': 'B2B founders', 'status': status,
                   'fit': r['fit_product'] or [], 'fit_reason': r['fit_reason'], 'contact_type': r['email_type'],
                   'origin': 'gumloop', 'email_type': 'role inbox' if is_role(r['email']) else 'personal (own domain)',
                   'launch_date': r['launch_date'] or r['found_date'], 'tagline': None},
                  'gumloop:producthunt', r['source_url'] or r['product_url'], {k: str(v) for k, v in r.items()}, 0)
    print(f'migrated {len(rows)} rows from leads.leads')


if __name__ == '__main__':
    ap = argparse.ArgumentParser()
    ap.add_argument('list', choices=list(SOURCES) + ['migrate-gumloop', 'retag', 'selftest', 'backfill'])
    ap.add_argument('--limit', type=int)
    ap.add_argument('--day')
    ap.add_argument('--min-votes', type=int, default=0)
    ap.add_argument('--dry', action='store_true')
    ap.add_argument('--from', dest='start')
    ap.add_argument('--to', dest='end')
    ap.add_argument('--budget-min', type=int, default=300)
    ap.add_argument('--dr', action='store_true', help='look up domain rating via treg (paid)')
    a = ap.parse_args()
    if a.list == 'migrate-gumloop':
        migrate_gumloop(); sys.exit()
    if a.list == 'backfill':
        backfill(a.start, a.end, a.min_votes or 31, a.budget_min); sys.exit()
    if a.list == 'selftest':
        sys.exit(selftest())
    if a.list == 'retag':
        print('retagged', core.retag(), 'leads'); sys.exit()
    run_id = core.start_run(a.list, a.list)
    kw = {}
    if a.limit: kw['limit'] = a.limit
    if a.list == 'producthunt':
        kw.update(day=a.day, min_votes=a.min_votes)
    try:
        items = SOURCES[a.list](run_id, **kw)
        stats = process(a.list, items, run_id, dry=a.dry)
        core.finish_run(run_id, dict(stats), ok=True)
        print(json.dumps(dict(stats), indent=1))
    except Exception as e:
        core.finish_run(run_id, {'error': repr(e)}, ok=False); raise
