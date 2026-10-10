"""Input adapters."""
import concurrent.futures as cf, datetime as dt, json, time, urllib.error, urllib.parse, urllib.request
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
    items = apify('memo23/startup-launch-scraper', {
        'sources': ['tinylaunch', 'microlaunch', 'launchingnext', 'betalist', 'uneed', 'startupfast'],
        'maxItems': limit, 'maxItemsPerSource': per_source, 'resolveWebsites': True, 'enrichEmails': False}, run_id, max_usd=0.40)
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
                return {'website': o.get('url') or '', 'category': o.get('category'), 'keywords': o.get('keywords') or [],
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
    items = apify('zerobreak/trustmrr-startup-scraper', {'mode': mode, 'maxItems': limit}, run_id, max_usd=0.10)
    with cf.ThreadPoolExecutor(4) as ex:
        pages = list(ex.map(lambda i: _trustmrr_page(i['url']), items))
    out = []
    for i, pg in zip(items, pages):
        out.append({'list': 'revenue', 'source': 'engine:trustmrr', 'ref': i['url'],
                    'name': re.sub(r' - \$.*$', '', i.get('name') or '').strip(), 'website': pg.get('website', ''),
                    'tagline': (i.get('description') or '')[:300],
                    'topics': [x for x in [pg.get('category')] + list(pg.get('keywords') or []) if x and x not in ('Other', 'Both')],
                    'people': pg.get('founders') or [], 'emails': [],
                    'signals': {k: pg.get(k) for k in ('mrr', 'revenue_30d', 'revenue_all_time', 'customers', 'country', 'founded')
                                if pg.get(k) is not None},
                    'launch_date': None})
    return out


# ---------------------------------------------------------------------------------- 4. funded
def funded(run_id, limit=50, batches=('Summer 2026', 'Spring 2026')):
    items = apify('memo23/y-combinator-scraper', {'mode': 'companies', 'batch': list(batches), 'maxItems': limit,
                                                   'scrapeFounderDetails': True}, run_id, max_usd=0.30)
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
    """The public app page links to the developer's own site many times; the most-linked outside domain is it. Free."""
    import collections, re
    from core import domain_of, http_get
    _, page = http_get(url, timeout=20, limit=800_000)
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


SOURCES = {'producthunt': producthunt, 'launches': launches, 'revenue': revenue, 'funded': funded,
           'chrome': chrome, 'shopify': shopify}
