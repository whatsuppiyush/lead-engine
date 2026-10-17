"""Fill founder names on leads that have none, from the people every source already listed for the same company
(raw payloads in lead_sources + companies.signals.makers). Same rule as a live run (core.name_for). Free, no scraping.
Never overwrites a name. Dry run unless --apply.

  .venv/bin/python names.py --since 2026-01-01            # counts + samples
  .venv/bin/python names.py --since 2026-01-01 --apply
  .venv/bin/python names.py --ph --apply                  # first read makers off PH pages for unnamed PH leads (free, slow)
  .venv/bin/python names.py --site --apply                # first read founders from company sites' schema.org data (free)
"""
import argparse, collections, json
from core import DB, domain_of, http_get, name_for, ph_makers_direct, site_people

SKIP = ('file:%creators%',)   # creator lists are people already, not founder lists


def _uniq(ps):
    seen, out = set(), []   # one entry per person, the richest record first
    for p in sorted(ps, key=lambda p: -len([v for v in p.values() if v])):
        k = (p.get('name') or '').strip().lower()
        if k and k not in seen: seen.add(k); out.append(p)
    return out


def _named(ps):
    return [p for p in ps or [] if isinstance(p, dict) and (p.get('name') or '').strip() not in ('', '[REDACTED]')]


def people_for(rows):
    """{email: people} for leads [{email, d}]. An email's own records always count. Other leads at the same company
    count only when the company is a real domain: the old PH scraper filed app-store products under apps.apple.com /
    play.google.com, and pooling those would hand one app's maker to every other app. An email linked to more than 3
    Product Hunt products is a scrape artefact (help@zuuks.com sat on 200), so its records are ignored."""
    emails = [r['email'] for r in rows]
    junk = {r['email'] for r in DB.execute("""select email from lead_sources where email = any(%s) and source like '%%producthunt%%'
                                              group by 1 having count(distinct source_ref) > 3""", (emails,)).fetchall()}
    own = collections.defaultdict(list)
    for r in DB.execute("""select email, payload from lead_sources where email = any(%s) and jsonb_typeof(payload->'people') = 'array'""",
                        (emails,)).fetchall():
        if r['email'] not in junk: own[r['email']] += _named(r['payload']['people'])
    real = sorted({r['d'] for r in rows if r['d'] and domain_of(r['d']) == r['d'].removeprefix('www.')})
    dom = collections.defaultdict(list)
    for r in DB.execute("""select l.company_domain d, s.payload from leads l join lead_sources s using (email)
                           where l.company_domain = any(%s) and jsonb_typeof(s.payload->'people') = 'array'""", (real,)).fetchall():
        site = domain_of(r['payload'].get('website'))
        if site and site != r['d']: continue   # an address listed for another product: its people are that product's
        dom[r['d']] += _named(r['payload']['people'])
    for r in DB.execute("select domain d, signals->'makers' m from companies where domain = any(%s) and jsonb_typeof(signals->'makers') = 'array'",
                        (real,)).fetchall():
        dom[r['d']] += _named(r['m'])
    return {r['email']: _uniq(own[r['email']] + (dom.get(r['d']) or [])) for r in rows}


def run(since, apply):
    rows = DB.execute(f"""
      select l.email, l.company_domain d, coalesce(c.name, l.company) co,
             (select string_agg(distinct split_part(s.source, ':', 2), ',') from lead_sources s where s.email = l.email) srcs
      from leads l left join companies c on c.domain = l.company_domain
      where l.first_name is null and l.company_domain is not null and l.created_at >= %s
        and not exists (select 1 from lead_sources s where s.email = l.email and ({' or '.join(['s.source like %s'] * len(SKIP))}))""",
                      (since, *SKIP)).fetchall()
    ppl = people_for(rows)
    done, by_src, sample = [], collections.Counter(), []
    for r in rows:
        f, l = name_for(r['email'], ppl.get(r['email']), r['co'])
        if not f: continue
        done.append((r['email'], f, l)); by_src[r['srcs']] += 1
        if len(sample) < 25: sample.append(f"{r['email']} -> {f} {l}".strip())
    print(f'{len(rows)} unnamed leads since {since}; {sum(1 for r in rows if ppl.get(r["email"]))} have people on record; '
          f'{len(done)} get a name')
    for k, v in by_src.most_common(15): print(f'  {v:6}  {k}')
    print('\n'.join('  ' + s for s in sample))
    if apply and done:
        n = DB.execute("""update leads l set first_name = v.f, last_name = coalesce(l.last_name, nullif(v.l, ''))
                          from unnest(%s::text[], %s::text[], %s::text[]) v(e, f, l) where l.email = v.e and l.first_name is null""",
                       ([e for e, _, _ in done], [f for _, f, _ in done], [l for _, _, l in done])).rowcount
        print('named', n)
    return done


def ph_pages(since, apply, batch=150):
    """Unnamed leads from a Product Hunt source: read each product's makers off its public page (the API hid them),
    store them as an 'engine:ph-makers' record so the naming rule can use them. Fetched from this machine, paced
    (the Worker gets rate-limited at this volume). Checkpointed per batch: a rerun skips products already read."""
    rows = DB.execute("""
      select distinct on (s.source_ref) s.source_ref url from leads l join lead_sources s using (email)
      where l.first_name is null and l.created_at >= %s and s.source like '%%producthunt%%' and s.source_ref like 'https://www.producthunt.com/%%'
        and not exists (select 1 from lead_sources x where x.source = 'engine:ph-makers' and x.source_ref = s.source_ref)""", (since,)).fetchall()
    urls = [r['url'] for r in rows]
    print(f'{len(urls)} PH products to read' + ('' if apply else ' (dry run)'))
    if not apply: return
    import concurrent.futures as cf, time
    got = done = 0
    def one(url):
        time.sleep(1.2)                       # ~1.5 pages/s across both threads: PH tolerates that from a home IP
        return url, ph_makers_direct(url)
    for i in range(0, len(urls), batch):
        with cf.ThreadPoolExecutor(2) as ex:
            found = {u: m for u, m in ex.map(one, urls[i:i + batch]) if m}
        for url, makers in found.items():
            for r in DB.execute("""select distinct l.email, l.website, l.company_domain from leads l join lead_sources s using (email)
                                   where s.source_ref = %s and s.source like '%%producthunt%%'""", (url,)).fetchall():
                payload = json.dumps({'website': r['website'] if domain_of(r['website']) else '', 'people': makers})
                DB.execute("""insert into lead_sources (email, source, source_ref, run_id, payload) values (%s, 'engine:ph-makers', %s, 'names.py', %s)
                              on conflict (email, source, source_ref) do update set payload = excluded.payload, received_at = now()""",
                           (r['email'], url, payload))
        got += len(found); done = min(i + batch, len(urls))
        print(f'  {done}/{len(urls)} read, {got} with makers', flush=True)

def site_founders(since, apply):
    """Unnamed leads at companies with nobody on record: read the homepage and /about for founders the site declares in
    its structured data (schema.org founder, or a Person titled founder/CEO). Free. Stored as 'engine:site-founders'."""
    import concurrent.futures as cf
    rows = DB.execute(f"""
      select distinct l.company_domain d from leads l
      where l.first_name is null and l.created_at >= %s and l.company_domain is not null
        and not exists (select 1 from lead_sources s where s.email = l.email and ({' or '.join(['s.source like %s'] * len(SKIP))}))
        and not exists (select 1 from lead_sources s where s.email = l.email and s.source = 'engine:site-founders')""",
                      (since, *SKIP)).fetchall()
    doms = [r['d'] for r in rows]
    print(f'{len(doms)} company sites to read' + ('' if apply else ' (dry run)'))
    if not apply: return
    def read(d):
        found = []
        for path in ('', '/about', '/about-us'):
            _, page = http_get(f'https://{d}{path}', timeout=10, limit=600_000)
            if page: found += [p for p in site_people(page) if p['name'].lower() not in {x['name'].lower() for x in found}]
            elif not path: break
        return d, found
    hit = 0
    with cf.ThreadPoolExecutor(16) as ex:
        for n, (d, found) in enumerate(ex.map(read, doms), 1):
            for r in DB.execute('select email from leads where company_domain = %s and first_name is null', (d,)).fetchall():
                DB.execute("""insert into lead_sources (email, source, source_ref, run_id, payload) values (%s, 'engine:site-founders', %s, 'names.py', %s)
                              on conflict (email, source, source_ref) do update set payload = excluded.payload, received_at = now()""",
                           (r['email'], d, json.dumps({'website': f'https://{d}', 'people': found})))
            hit += bool(found)
            if n % 500 == 0: print(f'  {n}/{len(doms)} sites read, {hit} declare a founder', flush=True)
    print(f'  {len(doms)} sites read, {hit} declare a founder')


if __name__ == '__main__':
    ap = argparse.ArgumentParser()
    ap.add_argument('--since', default='2026-01-01')
    ap.add_argument('--apply', action='store_true')
    ap.add_argument('--ph', action='store_true', help='read makers off PH pages for unnamed PH leads first')
    ap.add_argument('--site', action='store_true', help='read founders from company sites\' structured data first')
    a = ap.parse_args()
    if a.ph: ph_pages(a.since, a.apply)
    if a.site: site_founders(a.since, a.apply)
    run(a.since, a.apply)
