"""Input adapters."""
import concurrent.futures as cf, datetime as dt, json, re, time, urllib.error, urllib.parse, urllib.request
from core import ENV, log_spend, resolve_redirect

APIFY = 'https://api.apify.com/v2'


def pick(d, *keys, default=''):
    for k in keys:
        cur = d
        for part in k.split('.'):
            cur = cur.get(part) if isinstance(cur, dict) else None
        if cur not in (None, '', [], {}): return cur
    return default


def _tokens():
    toks = [t.strip() for t in (ENV.get('APIFY_TOKENS') or '').split(',') if t.strip()]
    return toks or [ENV['APIFY_TOKEN']]


def _left(tok):
    try:
        d = json.load(urllib.request.urlopen(f'{APIFY}/users/me/limits?token={tok}', timeout=30))['data']
        return float(d['limits']['maxMonthlyUsageUsd']) - float(d['current']['monthlyUsageUsd'])
    except Exception:
        return 0.0


def apify(actor, run_input, run_id, max_usd=0.50, wait_s=1800):
    """Run an actor on whichever Apify account has the most credit left; on a credit error, try the next."""
    ranked = sorted(_tokens(), key=_left, reverse=True)
    last = None
    for tok in ranked:
        q = urllib.parse.urlencode({'token': tok, 'maxTotalChargeUsd': max_usd, 'waitForFinish': 60})
        req = urllib.request.Request(f'{APIFY}/acts/{actor.replace("/", "~")}/runs?{q}', json.dumps(run_input).encode(),
                                     {'Content-Type': 'application/json'})
        try:
            run = json.load(urllib.request.urlopen(req, timeout=120))['data']
        except urllib.error.HTTPError as e:
            last = f'HTTP {e.code} {e.read()[:160]!r}'
            if e.code in (402, 403): print('  apify account out of credit, rotating'); continue
            raise
        t0 = time.time()
        while run['status'] in ('READY', 'RUNNING') and time.time() - t0 < wait_s:
            time.sleep(10)
            run = json.load(urllib.request.urlopen(f"{APIFY}/actor-runs/{run['id']}?token={tok}", timeout=60))['data']
        usd = run.get('usageTotalUsd')
        log_spend(run_id, 'apify', 1, usd)
        if run['status'] != 'SUCCEEDED':
            print(f'  apify {actor}: {run["status"]} (items so far are still used)')
        items = json.load(urllib.request.urlopen(
            f"{APIFY}/datasets/{run['defaultDatasetId']}/items?token={tok}&clean=1&format=json", timeout=300))
        print(f'  apify {actor}: {len(items)} items, ${usd or 0:.4f} (account {ranked.index(tok) + 1} of {len(ranked)})')
        return items
    raise RuntimeError(f'all Apify accounts failed: {last}')


# ------------------------------------------------------------------------------- 1. Product Hunt
PH_QUERY = """query($a:DateTime!,$b:DateTime!,$c:String){posts(postedAfter:$a,postedBefore:$b,order:VOTES,first:50,after:$c){
  pageInfo{hasNextPage endCursor} edges{node{id name tagline slug votesCount website createdAt
  topics{edges{node{name}}} makers{name username twitterUsername}}}}}"""


def producthunt(run_id, day=None, limit=1000, min_votes=0):
    """One Product Hunt day (Pacific time). Default: yesterday, the last fully ranked day. Free API."""
    day = day or (dt.datetime.now(dt.timezone.utc) - dt.timedelta(hours=7) - dt.timedelta(days=1)).date().isoformat()
    a = f'{day}T07:00:00Z'
    b = (dt.datetime.fromisoformat(day) + dt.timedelta(days=1)).date().isoformat() + 'T07:00:00Z'
    out, cur = [], None
    while len(out) < limit:
        body = json.dumps({'query': PH_QUERY, 'variables': {'a': a, 'b': b, 'c': cur}}).encode()
        for attempt in range(6):               # PH meters "complexity" per 15 min; wait for the reset it reports
            try:
                r = json.load(urllib.request.urlopen(urllib.request.Request('https://api.producthunt.com/v2/api/graphql', body,
                              {'Authorization': f"Bearer {ENV['PH_TOKEN']}", 'Content-Type': 'application/json'}), timeout=60))
                break
            except urllib.error.HTTPError as e:
                if e.code != 429: raise
                reset = e.headers.get('x-rate-limit-reset') or e.headers.get('Retry-After') or ''
                wait = min(int(reset) + 2, 900) if reset.isdigit() else 60 * (attempt + 1)
                print(f'  PH API rate limit; waiting {wait}s', flush=True); time.sleep(wait)
        else:
            raise RuntimeError('PH API still rate-limited after retries')
        posts = r['data']['posts']
        for e in posts['edges']:
            n = e['node']
            if n['votesCount'] < min_votes:      # results come sorted by votes: nothing below this qualifies
                posts['pageInfo']['hasNextPage'] = False; break
            out.append({'list': 'producthunt', 'source': 'engine:producthunt', 'ref': f"https://www.producthunt.com/products/{n['slug']}",
                        'name': n['name'], 'website': n['website'], 'tagline': n['tagline'],
                        'topics': [t['node']['name'] for t in n['topics']['edges']],
                        'people': [{'name': m['name'], 'x': m.get('twitterUsername') or '', 'ph': m.get('username')} for m in n['makers']],
                        'emails': [], 'signals': {'upvotes': n['votesCount'], 'ph_day': day}, 'launch_date': day,
                        '_redirect': True})
        if not posts['pageInfo']['hasNextPage']: break
        cur = posts['pageInfo']['endCursor']; time.sleep(1)
    out = out[:limit]
    from core import resolve_ph_links      # PH gives tracking links; resolve to the real site (free, via our Worker)
    sites = resolve_ph_links([it['website'] for it in out])
    for it in out:
        it['website'] = sites.get(it['website'], '')
    return out


# --------------------------------------------------------------------------- 2. launch directories
def launches(run_id, limit=200, per_source=40):
    # these actors stop at 100 items per run on a free Apify plan, so big pulls run one source per run
    srcs = ['tinylaunch', 'microlaunch', 'launchingnext', 'betalist', 'uneed', 'startupfast']
    groups = [srcs] if limit <= 100 else [[x] for x in srcs]
    items = []
    for g in groups:
        n = min(limit, per_source * len(g))
        items += apify('memo23/startup-launch-scraper', {'sources': g, 'maxItems': n, 'maxItemsPerSource': per_source,
                       'resolveWebsites': True, 'enrichEmails': False}, run_id, max_usd=max(0.40, n * 0.003))
    return [{'list': 'launches', 'source': f"engine:launches:{pick(i, 'source', 'directory', default='directory')}",
             'ref': pick(i, 'listingUrl', 'url', 'launchUrl', 'directoryUrl', 'link'), 'name': pick(i, 'name', 'title'),
             'website': pick(i, 'website', 'websiteUrl', 'resolvedWebsite', 'homepage'), 'tagline': pick(i, 'tagline', 'description'),
             'topics': [x for x in ((i.get('categories') or []) + (i.get('tags') or []) + [i.get('category')]) if isinstance(x, str) and x],
             'people': [{'name': pick(i, 'makerName', 'maker', 'founder', 'author'), 'handle': pick(i, 'makerHandle')}]
                       if pick(i, 'makerName', 'maker', 'founder', 'author') else [],
             'emails': pick(i, 'emails', default=[]) if isinstance(pick(i, 'emails', default=[]), list) else [],
             'signals': {k: i.get(k) for k in ('upvotes', 'rank', 'commentCount', 'source') if i.get(k) is not None},
             'launch_date': (pick(i, 'launchDate', 'launchedAt', 'date') or '')[:10] or None, '_raw': i} for i in items]


# --------------------------------------------------------------------------------- 3. revenue
def _trustmrr_page(url):
    """TrustMRR's startup page carries schema.org JSON-LD (@graph > Organization): website, founders with their X,
    category, country, founding date and verified revenue figures. Free."""
    import re
    from core import http_get
    _, page = http_get(url, timeout=20, limit=600_000)
    for blob in re.findall(r'<script type="application/ld\+json">(.*?)</script>', page, re.S):
        try: d = json.loads(blob)
        except Exception: continue
        for o in (d.get('@graph') or [d]) if isinstance(d, dict) else d:
            if isinstance(o, dict) and o.get('@type') == 'Organization':
                props = {p.get('name'): p.get('value') for p in o.get('additionalProperty') or [] if isinstance(p, dict)}
                fs = o.get('founder') or []
                return {'name': o.get('name') or '', 'description': o.get('description') or '',
                        'website': o.get('url') or '', 'category': o.get('category'), 'keywords': o.get('keywords') or [],
                        'country': (o.get('foundingLocation') or {}).get('identifier'), 'founded': o.get('foundingDate'),
                        'founders': [{'name': f.get('name', ''), 'x': f.get('sameAs') if isinstance(f.get('sameAs'), str) else ''}
                                     for f in (fs if isinstance(fs, list) else [fs]) if isinstance(f, dict)],
                        'mrr': props.get('Current MRR'), 'revenue_30d': props.get('Verified revenue, last 30 days'),
                        'revenue_all_time': props.get('Verified revenue, all time'),
                        'customers': props.get('Current customers/subscriptions')}
    return {}


def revenue(run_id, limit=100, mode='leaderboard'):
    """List of startups from Apify (fractions of a cent); every detail from TrustMRR's own pages (free)."""
    import re
    if mode == 'all':   # the actor's own 'all' mode finds nothing, and TrustMRR's sitemap lists every startup for free
        from core import http_get
        _, xml = http_get('https://trustmrr.com/startup-sitemap.xml', timeout=60, limit=20_000_000)
        urls = list(dict.fromkeys(u for u in re.findall(r'<loc>(https://trustmrr\.com/startup/[^/<]+)</loc>', xml)))
        items = [{'url': u} for u in urls]
        print(f'  trustmrr sitemap: {len(urls)} startups; keeping the {limit} with the most verified revenue')
    else:
        items = apify('zerobreak/trustmrr-startup-scraper', {'mode': mode, 'maxItems': min(limit, 1000)}, run_id,
                      max_usd=max(0.10, min(limit, 1000) * 0.0005))
    with cf.ThreadPoolExecutor(8) as ex:
        pages = list(ex.map(lambda i: _trustmrr_page(i['url']), items))
    if mode == 'all':
        rev = lambda pg: float(pg.get('revenue_30d') or 0) + float(pg.get('mrr') or 0)
        pairs = sorted((x for x in zip(items, pages) if rev(x[1]) > 0), key=lambda x: -rev(x[1]))[:limit]
        items, pages = [x[0] for x in pairs], [x[1] for x in pairs]
    out = []
    for i, pg in zip(items, pages):
        out.append({'list': 'revenue', 'source': 'engine:trustmrr', 'ref': i['url'],
                    'name': re.sub(r' - \$.*$', '', i.get('name') or pg.get('name') or '').strip(), 'website': pg.get('website', ''),
                    'tagline': (i.get('description') or pg.get('description') or '')[:300],
                    'topics': [x for x in [pg.get('category')] + list(pg.get('keywords') or []) if x and x not in ('Other', 'Both')],
                    'people': pg.get('founders') or [], 'emails': [],
                    'signals': {k: pg.get(k) for k in ('mrr', 'revenue_30d', 'revenue_all_time', 'customers', 'country', 'founded')
                                if pg.get(k) is not None},
                    'launch_date': None})
    return out


# ---------------------------------------------------------------------------------- 4. funded
def funded(run_id, limit=50, batches=('Summer 2026', 'Spring 2026')):
    if isinstance(batches, (str, int)) and str(batches).isdigit():      # '2026' -> every batch that year
        y = str(batches); batches = (f'Winter {y}', f'Spring {y}', f'Summer {y}', f'Fall {y}')
    groups = [list(batches)] if limit <= 100 else [[b] for b in batches]   # 100 items per run on a free plan
    items = []
    for g in groups:
        items += apify('memo23/y-combinator-scraper', {'mode': 'companies', 'batch': g, 'maxItems': limit,
                                                       'scrapeFounderDetails': True}, run_id, max_usd=max(0.30, limit * 0.002))
    out = []
    for i in items:
        founders = pick(i, 'founders', default=[])
        out.append({'list': 'funded', 'source': 'engine:yc', 'ref': pick(i, 'url', 'ycUrl', 'slug'), 'name': pick(i, 'name'),
                    'website': pick(i, 'website', 'websiteUrl'), 'tagline': pick(i, 'oneLiner', 'one_liner', 'description'),
                    'topics': (pick(i, 'industries', 'tags', default=[]) or []) if isinstance(pick(i, 'industries', 'tags', default=[]), list) else [],
                    'people': [{'name': pick(f, 'name', 'full_name'), 'title': pick(f, 'title'),
                                'linkedin': pick(f, 'linkedin', 'linkedinUrl', 'linkedin_url'),
                                'x': pick(f, 'twitter', 'twitterUrl', 'x')} for f in founders if isinstance(f, dict)],
                    'emails': [], 'signals': {'yc_batch': pick(i, 'batch'), 'team_size': pick(i, 'teamSize', 'team_size', default=None),
                                              'hiring': pick(i, 'isHiring', 'is_hiring', default=None)},
                    'launch_date': None, '_raw': i})
    return out


# ----------------------------------------------------------------------------- 5. Chrome Web Store
def chrome(run_id, keywords=('productivity', 'seo', 'ai assistant', 'email', 'social media'), per=10, max_users=50000, limit=None):
    per = max(1, (limit or per * len(keywords)) // len(keywords))
    items = apify('calm_builder/chrome-web-store-scraper', {'searchUrls': list(keywords), 'maxExtensionsPerSearch': per,
                  'fetchDetails': True, 'maxUsers': max_users, 'proxyCountry': 'us'}, run_id, max_usd=0.50)
    return [{'list': 'chrome', 'source': 'engine:chrome', 'ref': pick(i, 'url', 'extensionUrl', 'id'), 'name': pick(i, 'name', 'title'),
             'website': pick(i, 'website', 'developerWebsite', 'publisherWebsite', 'homepage'), 'tagline': pick(i, 'shortDescription', 'description'),
             'topics': [pick(i, 'category')] if pick(i, 'category') else [],
             'people': [{'name': pick(i, 'developerName', 'publisher', 'author')}] if pick(i, 'developerName', 'publisher', 'author') else [],
             'emails': [e for e in [pick(i, 'developerEmail', 'email', 'publisherEmail')] if e],
             'signals': {'users': pick(i, 'users', 'userCount', default=None), 'rating': pick(i, 'rating', default=None)},
             'launch_date': None, '_raw': i} for i in items]


# ---------------------------------------------------------------------------- 6. Shopify App Store
def _shopify_page(url):
    from core import http_get
    return _shopify_page_html(http_get(url, timeout=20, limit=800_000)[1])


def _shopify_page_html(page):
    """The public app page links to the developer's own site many times; the most-linked outside domain is it. Free."""
    import collections, re
    from core import domain_of
    doms = collections.Counter(domain_of(h) for h in re.findall(r'href="(https?://[^"]+)"', page))
    doms.pop('', None)
    for d in list(doms):
        if 'shopify' in d or d.endswith(('cdn.com', 'gstatic.com', 'googleapis.com', 'cloudfront.net')): doms.pop(d)
    brand = (re.search(r'"brand":"([^"]+)"', page) or [None, ''])[1]
    return (f'https://{doms.most_common(1)[0][0]}' if doms else ''), brand


def shopify(run_id, keywords=('reviews', 'upsell', 'email marketing', 'loyalty', 'shipping'), pages=1, limit=None):
    """Search results from Apify (~free); developer site from Shopify's public app page (free)."""
    keywords = keywords[:max(1, (limit or 99) // 20)] if limit else keywords      # ~20 apps per keyword page
    items = apify('applora/shopify-appstore-scraper', {'scrapeType': 'keyword', 'keywords': list(keywords),
                                                       'maxKeywordPages': pages}, run_id, max_usd=0.50)
    items = [i for i in items if not i.get('isSponsored')][:limit or None]
    with cf.ThreadPoolExecutor(4) as ex:
        pages_ = list(ex.map(lambda i: _shopify_page(i['url']), items))
    return [{'list': 'shopify', 'source': 'engine:shopify', 'ref': i['url'], 'name': brand or i.get('name'),
             'website': site, 'tagline': (i.get('description') or '')[:300], 'topics': ['e-commerce', 'shopify app'],
             'people': [], 'emails': [],
             'signals': {'reviews': i.get('reviewsCount'), 'rating': i.get('rating'), 'pricing': i.get('pricing'),
                         'built_for_shopify': i.get('isBuiltForShopify'), 'app': i.get('name')},
             'launch_date': None} for i, (site, brand) in zip(items, pages_)]


# ----------------------------------------------------- free store sources: public sitemaps + listing pages
def _seen_refs(source):
    from core import DB
    return {r['source_ref'] for r in DB.execute('select distinct source_ref from lead_sources where source = %s', (source,)).fetchall()}


def _sitemap_urls(url, pattern):
    import re
    from core import http_get
    _, xml = http_get(url, timeout=60, limit=40_000_000)
    return list(dict.fromkeys(re.findall(pattern, xml)))


def _date(s):
    import datetime
    try: return datetime.datetime.strptime(s.strip(), '%B %d, %Y').date().isoformat()
    except Exception: return None


def _paced_get(url, tries=6):
    """Shopify answers 429 when hit fast: back off and retry instead of losing the page."""
    for n in range(tries):
        try:
            r = urllib.request.urlopen(urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0', 'Accept': 'text/html'}), timeout=25)
            return r.read(1_500_000).decode('utf-8', 'ignore')
        except urllib.error.HTTPError as e:
            if e.code != 429: return ''
            time.sleep(min(60, int(e.headers.get('Retry-After') or 0) or 5 * 2 ** n))
        except Exception:
            time.sleep(3)
    return ''


def _shopify_listing(url):
    import re
    p = _paced_get(url)
    if not p: return None
    site, brand = _shopify_page_html(p)
    em = re.search(r'data-developer-support-email="([^"]+)"', p)
    la = re.search(r'Launched\s*(?:<[^>]+>\s*)*([A-Z][a-z]+ \d{1,2}, \d{4})', p)
    rc = re.search(r'"ratingCount":\s*"?(\d+)', p)
    name = re.search(r'<title>([^<|]+)', p)
    return {'url': url, 'email': em[1].strip().lower() if em else '', 'launched': _date(la[1]) if la else None,
            'reviews': int(rc[1]) if rc else 0, 'website': site, 'brand': brand,
            'name': (name[1].strip() if name else url.rsplit('/', 1)[-1])}


def shopify_sitemap(run_id, limit=500, launched_days=None):
    """Every app in Shopify's public sitemap (~28k), each page carries the developer's support email. Free.
    Fetches up to `limit` apps not seen before; launched_days keeps only apps launched in the last N days."""
    import datetime
    urls = _sitemap_urls('https://apps.shopify.com/sitemap_apps_en.xml', r'<loc>(https://apps\.shopify\.com/[^/<?]+)</loc>')
    seen = _seen_refs('engine:shopify')
    todo = [u for u in urls if u not in seen][:limit]
    print(f'  shopify sitemap: {len(urls)} apps, {len(seen)} seen, fetching {len(todo)}')
    with cf.ThreadPoolExecutor(2) as ex:
        pages = [p for p in ex.map(_shopify_listing, todo) if p]
    if launched_days:
        cut = (datetime.date.today() - datetime.timedelta(days=int(launched_days))).isoformat()
        pages = [p for p in pages if (p['launched'] or '') >= cut]
    return [{'list': 'shopify', 'source': 'engine:shopify', 'ref': p['url'], 'name': p['brand'] or p['name'],
             'website': p['website'], 'tagline': p['name'][:300], 'topics': ['e-commerce', 'shopify app'], 'people': [],
             'emails': [p['email']] if p['email'] else [],
             'signals': {'reviews': p['reviews'], 'launched': p['launched'], 'app': p['name']},
             'launch_date': p['launched']} for p in pages]


def _chrome_listing(url):
    import re, html as h
    from core import http_get
    _, p = http_get(url + '?hl=en', timeout=25, limit=2_000_000)
    if not p: return None
    t = re.sub(r'<style.*?</style>|<script.*?</script>', '', p, flags=re.S)
    txt = h.unescape(re.sub(r'\n\s*\n+', '\n', re.sub(r'<[^>]+>', '\n', t)))
    users = re.search(r'\n([\d,]+)\+? users\n', txt)
    upd = re.search(r'\nUpdated\n([A-Z][a-z]+ \d{1,2}, \d{4})', txt)
    em = re.search(r'class="NnJuub">([^<@\s]+@[^<\s]+)</div>', p) or re.search(r'\n\s*Email\s*\n([^\s@]+@[^\s]+)\n', txt)
    site = re.search(r'href="(https?://[^"]+)"[^>]*>(?:(?!</a>).){0,600}?Website</a>', p, re.S)
    title = re.search(r'<title>([^<]+?)(?: - Chrome Web Store)?</title>', p)
    return {'url': url, 'users': int(users[1].replace(',', '')) if users else 0, 'updated': _date(upd[1]) if upd else None,
            'email': em[1].strip().lower() if em else '', 'website': h.unescape(site[1]) if site else '',
            'trader': '\nTrader\n' in txt, 'name': h.unescape(title[1]).strip() if title else ''}


def chrome_sitemap(run_id, limit=500, min_users=1000, shards=None):
    """Chrome Web Store's public sitemap (~375k listings, 44 shards). Free. Keeps businesses: enough users,
    a website or a non-Gmail email, updated in the last 6 months."""
    import datetime, random
    seen = _seen_refs('engine:chrome')
    order = list(range(44)); random.shuffle(order)
    out, fetched = [], 0
    cut = (datetime.date.today() - datetime.timedelta(days=183)).isoformat()
    free_mail = ('gmail.com', 'outlook.com', 'hotmail.com', 'yahoo.com', 'qq.com', '163.com', 'icloud.com', 'proton.me', 'protonmail.com')
    for s in ([int(x) for x in str(shards).split(',')] if shards else order):
        urls = [u for u in _sitemap_urls(f'https://chromewebstore.google.com/sitemap?shard={s}',
                                         r'<loc>(https://chromewebstore\.google\.com/detail/[^<]+)</loc>') if u not in seen]
        batch = urls[:max(0, limit - fetched)]
        with cf.ThreadPoolExecutor(8) as ex:
            pages = [p for p in ex.map(_chrome_listing, batch) if p]
        fetched += len(batch)
        for p in pages:
            biz = p['website'] or (p['email'] and not p['email'].endswith(free_mail))
            if int(min_users) <= p['users'] <= 1_000_000 and biz and (p['updated'] or '') >= cut:
                out.append(p)
        print(f'  chrome shard {s}: fetched {len(batch)}, kept {len(out)} so far')
        if fetched >= limit: break
    return [{'list': 'chrome', 'source': 'engine:chrome', 'ref': p['url'], 'name': p['name'], 'website': p['website'],
             'tagline': p['name'][:300], 'topics': ['chrome extension'], 'people': [],
             'emails': [p['email']] if p['email'] else [],
             'signals': {'users': p['users'], 'updated': p['updated'], 'trader': p['trader']},
             'launch_date': None} for p in out]


# ----------------------------------------------------------------- growth-stage sources (2026-10-10)
# Each keeps companies that are growing but not already winning: enough users or traction to be real,
# not so many that they need nobody's help. The numbers are stored in signals so fit rules can use them.
def _lst(v):
    return [x.strip() for x in (v if isinstance(v, (list, tuple)) else str(v or '').split(',')) if str(x).strip()]


def _get_json(url, timeout=30):
    req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0 (pp-daily-sync)', 'Accept': 'application/json'})
    return json.load(urllib.request.urlopen(req, timeout=timeout))


FREE_MAIL = {'gmail.com', 'googlemail.com', 'outlook.com', 'hotmail.com', 'live.com', 'yahoo.com', 'icloud.com', 'me.com',
             'proton.me', 'protonmail.com', 'gmx.de', 'gmx.net', 'web.de', 'qq.com', '163.com', 'yandex.ru', 'mail.ru', 'aol.com'}


CODE_HOSTS = ('github.com', 'gitlab.com', 'huggingface.co', 'bitbucket.org', 'codeberg.org', 'sr.ht', 'npmjs.com', 'pypi.org',
              'apps.apple.com', 'play.google.com', 'chromewebstore.google.com', 'youtube.com', 'x.com', 'twitter.com', 'medium.com',
              'substack.com', 'reddit.com', 'news.ycombinator.com', 'docs.google.com', 'notion.site', 'vercel.app', 'netlify.app')


def showhn(run_id, limit=1000, days=7, min_points=3):
    """Show HN posts from the last N days via HN's public Algolia API. Free. Keeps product sites, not code repos."""
    import time as _t
    since = int(_t.time()) - int(days) * 86400
    hits, page = [], 0
    while len(hits) < int(limit) * 3 and page < 50:
        d = _get_json(f'https://hn.algolia.com/api/v1/search_by_date?tags=show_hn&hitsPerPage=1000&page={page}'
                      f'&numericFilters=created_at_i>{since},points>={int(min_points)}')
        hits += d.get('hits') or []; page += 1
        if page >= (d.get('nbPages') or 0): break
    out = []
    for h in hits:
        url = h.get('url') or ''
        host = urllib.parse.urlparse(url).netloc.lower().removeprefix('www.')
        if not url or any(host == c or host.endswith('.' + c) for c in CODE_HOSTS): continue
        title = re.sub(r'^Show HN:\s*', '', h.get('title') or '')
        out.append({'list': 'showhn', 'source': 'engine:showhn', 'ref': f"https://news.ycombinator.com/item?id={h['objectID']}",
                    'name': re.split(r'\s[–—-]\s|:\s', title)[0][:80], 'website': url, 'tagline': title[:300], 'topics': [],
                    'people': [{'name': '', 'hn': h.get('author')}], 'emails': [],
                    'signals': {'hn_points': h.get('points'), 'hn_comments': h.get('num_comments'), 'hn_author': h.get('author')},
                    'launch_date': (h.get('created_at') or '')[:10] or None})
    print(f'  show hn: {len(hits)} posts with >= {min_points} points, {len(out)} product sites')
    return out[:int(limit)]


def stores(run_id, limit=300, technologies='Facebook Pixel', country='US', category=None, start_page=None):
    """Shopify stores already paying for Meta ads (Facebook Pixel installed), emails included. ~$0.60 per 100.
    Pages advance by themselves: the next run starts after the last page fetched for the same filter."""
    from core import DB
    key = f"stores:{technologies}:{country}:{category or ''}"
    last = DB.execute("select max((stats->>'page_to')::int) p from engine_runs where list = 'stores' and stats->>'filter' = %s", (key,)).fetchone()['p']
    page = int(start_page or (last or 0) + 1)
    items = []
    for p in range(page, page + max(1, int(limit) // 100)):
        inp = {'platform': 'shopify', 'page': p, 'emails': True, 'technologies': technologies}
        if country: inp['country'] = country
        if category: inp['category'] = category
        got = apify('ecommerce_leads/store-leads-14m-e-commerce-leads', inp, run_id, max_usd=0.65)
        items += got
        if len(got) < 100: break
    STORES_PAGES.update({'filter': key, 'page_from': page, 'page_to': p})
    out = []
    for i in items:
        tech = ' '.join(i.get('technologies') or []) if isinstance(i.get('technologies'), list) else str(i.get('technologies') or '')
        emails = i.get('emails') or []
        emails = [e.strip().lower() for e in (emails if isinstance(emails, list) else re.split(r'[,|;\s]+', str(emails))) if '@' in e]
        dom = (i.get('domain') or '').lower()
        if dom.endswith('myshopify.com') and emails:    # the store's real domain is the one its email lives on
            dom = emails[0].split('@')[1]
        cats = [c for c in (i.get('categories') or '').split('|') if c] if isinstance(i.get('categories'), str) else (i.get('categories') or [])
        out.append({'list': 'stores', 'source': 'engine:stores', 'ref': f"https://{i.get('domain')}", 'name': i.get('merchant') or dom,
                    'website': f"https://{dom}", 'tagline': ', '.join(cats)[:300], 'topics': ['e-commerce', 'shopify store'] + cats,
                    'people': [], 'emails': [e.lower() for e in emails][:3],
                    'signals': {'meta_pixel': 'facebook pixel' in tech.lower(), 'tiktok_pixel': 'tiktok pixel' in tech.lower(),
                                'instagram': i.get('instagram'), 'tiktok': i.get('tiktok'), 'country': i.get('country'), 'city': i.get('city')},
                    'launch_date': None})
    return out


STORES_PAGES = {}


def hiring(run_id, limit=300, query='marketing', location='', roles=''):
    """Startups hiring on Wellfound, one row per company. Hiring for marketing/growth = they want customers now.
    ~$3 per 1,000 job rows (residential proxy inside the actor)."""
    inp = {'query': query, 'companyOnlyMode': True, 'enrichCompany': True, 'enrichEmails': True, 'maxResults': int(limit), 'maxPages': max(1, int(limit) // 40)}
    if location: inp['location'] = _lst(location)
    if roles: inp['roles'] = _lst(roles)
    items = apify('blackfalcondata/wellfound-scraper', inp, run_id, max_usd=max(0.30, int(limit) * 0.004))
    out = []
    for i in items:
        site = pick(i, 'companyWebsite', 'website', 'companyUrl', 'companyHomepage')
        locs = pick(i, 'locationNames', 'locations', default=[]) or []
        out.append({'list': 'hiring', 'source': 'engine:wellfound', 'ref': pick(i, 'companyPortalUrl', 'companyUrl', 'portalUrl') or site,
                    'name': pick(i, 'companyName', 'name'), 'website': site or '', 'tagline': (pick(i, 'companyTagline', 'companyHighConcept', 'tagline') or '')[:300],
                    'topics': [x for x in (pick(i, 'companyMarkets', 'markets', default=[]) or []) if isinstance(x, str)], 'people': [],
                    'emails': [e.lower() for e in i.get('companyEmails') or [] if isinstance(e, str)],
                    'signals': {'hiring': True, 'hiring_for': query, 'jobs_open': pick(i, 'jobCount', default=None),
                                'team_size': pick(i, 'companySizeLabel', 'companySize', default=None),
                                'funding_stage': i.get('companyFundingStage'), 'growing_fast': i.get('companyGrowingFast'),
                                'yc': i.get('companyYcFunded'), 'jobs': (i.get('sampleJobTitles') or [])[:5],
                                'hq': (', '.join(locs) if isinstance(locs, list) else str(locs)) or ', '.join(_lst(location))},
                    'launch_date': None})
    return out


def firefox(run_id, limit=300, min_users=300, max_users=100_000, months=6):
    """Firefox add-ons from Mozilla's public API, by user count. Free. Keeps the growing middle: real users,
    not a household name, updated recently, with a homepage or support email."""
    import datetime
    cut = (datetime.date.today() - datetime.timedelta(days=30 * int(months))).isoformat()
    out, page = [], 1
    while len(out) < int(limit) and page <= 600:
        d = _get_json(f'https://addons.mozilla.org/api/v5/addons/search/?type=extension&sort=users&page_size=50&page={page}&lang=en-US')
        rows = d.get('results') or []
        if not rows: break
        for a in rows:
            users = int(a.get('average_daily_users') or 0)
            if users > int(max_users): continue
            if users < int(min_users): rows = []; break
            hp = a.get('homepage') or {}
            hp = (hp.get('url') or {}).get('en-US') if isinstance(hp, dict) else hp
            hp = hp if isinstance(hp, str) else (list(hp.values())[0] if isinstance(hp, dict) and hp else '')
            em = a.get('support_email')
            em = em.get('en-US') if isinstance(em, dict) else em
            if (a.get('last_updated') or '')[:10] < cut or not (hp or em): continue
            nm = a.get('name') or {}
            nm = nm.get('en-US') or next(iter(nm.values()), '') if isinstance(nm, dict) else nm
            if not hp and em and em.split('@')[-1].lower() not in FREE_MAIL: hp = 'https://' + em.split('@')[-1].lower()
            out.append({'list': 'firefox', 'source': 'engine:firefox', 'ref': a.get('url'), 'name': nm, 'website': hp or '',
                        'tagline': ((a.get('summary') or {}).get('en-US') or '')[:300] if isinstance(a.get('summary'), dict) else '',
                        'topics': ['browser extension'] + list(a.get('categories') or []),
                        'people': [{'name': x.get('name', '')} for x in a.get('authors') or []][:2],
                        'emails': [em.lower()] if em else [],
                        'signals': {'users': users, 'rating': (a.get('ratings') or {}).get('average'),
                                    'reviews': (a.get('ratings') or {}).get('count'), 'updated': (a.get('last_updated') or '')[:10]},
                        'launch_date': (a.get('created') or '')[:10] or None})
        if not rows: break
        page += 1
    print(f'  firefox: {len(out)} add-ons kept after {page} pages')
    return out[:int(limit)]


PLAY_QUERIES = ['habit tracker', 'budget planner', 'meditation', 'fitness coach', 'language learning', 'photo editor', 'video editor',
                'ai chat', 'ai assistant', 'note taking', 'to do list', 'calorie counter', 'workout', 'sleep tracker', 'pdf scanner',
                'invoice maker', 'crm', 'expense tracker', 'journal', 'flashcards', 'recipe', 'dating', 'pet care', 'baby tracker',
                'period tracker', 'parking', 'travel planner', 'resume builder', 'ai headshot', 'ai photo', 'wallpaper', 'keyboard',
                'vpn', 'password manager', 'podcast', 'music maker', 'drawing', 'kids learning', 'astrology', 'bible', 'quran',
                'stock tracker', 'crypto wallet', 'job search', 'social media planner', 'ai writer', 'translator', 'qr scanner']


def play(run_id, limit=300, queries=None, min_installs=1000, max_installs=1_000_000, months=6, years=3):
    """Google Play apps via the open-source google-play-scraper library (public pages, free). Every listing has a
    developer email. Keeps the growing middle: 1k to 1M installs, updated in the last 6 months, released in the
    last 3 years."""
    import datetime, time as _t
    from google_play_scraper import app as gp_app, search as gp_search
    from core import DB
    seen = {r['source_ref'] for r in DB.execute("select source_ref from lead_sources where source = 'engine:play'").fetchall()}
    qs = _lst(queries) or PLAY_QUERIES
    upd_cut = _t.time() - 30 * 86400 * int(months)
    rel_cut = datetime.date.today().year - int(years)
    ids = []
    for q in qs:
        try: ids += [r['appId'] for r in gp_search(q, lang='en', country='us', n_hits=30)]
        except Exception as e: print('  play search failed', q, str(e)[:80])
    ids = [i for i in dict.fromkeys(ids) if f'https://play.google.com/store/apps/details?id={i}' not in seen]
    def detail(i):
        try: return gp_app(i, lang='en', country='us')
        except Exception: return None
    with cf.ThreadPoolExecutor(6) as ex:
        apps = [a for a in ex.map(detail, ids) if a]
    out = []
    for a in apps:
        inst = int(a.get('realInstalls') or a.get('minInstalls') or 0)
        rel = re.search(r'(\d{4})', a.get('released') or '')
        if not (int(min_installs) <= inst <= int(max_installs)) or (a.get('updated') or 0) < upd_cut: continue
        if rel and int(rel[1]) < rel_cut: continue
        out.append({'list': 'play', 'source': 'engine:play', 'ref': f"https://play.google.com/store/apps/details?id={a['appId']}",
                    'name': a.get('developer') or a.get('title'), 'website': a.get('developerWebsite') or '',
                    'tagline': (a.get('summary') or a.get('title') or '')[:300], 'topics': ['mobile app', a.get('genre') or ''],
                    'people': [], 'emails': [a['developerEmail'].lower()] if a.get('developerEmail') else [],
                    'signals': {'installs': inst, 'rating': a.get('score'), 'reviews': a.get('ratings'), 'app': a.get('title'),
                                'genre': a.get('genre'), 'iap': a.get('offersIAP'), 'ads': a.get('containsAds'),
                                'released': a.get('released')},
                    'launch_date': None})
    print(f'  google play: {len(ids)} new apps found, {len(out)} in the growth window')
    return out[:int(limit)]


SOURCES = {'producthunt': producthunt, 'launches': launches, 'revenue': revenue, 'funded': funded,
           'chrome': chrome, 'shopify': shopify,
           'chrome-store': chrome_sitemap, 'shopify-store': shopify_sitemap,
           'showhn': showhn, 'stores': stores, 'hiring': hiring, 'firefox': firefox, 'play': play}
