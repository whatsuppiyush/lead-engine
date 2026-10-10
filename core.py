"""Shared helpers."""
import html, json, os, pathlib, random, re, smtplib, socket, string, subprocess, time, urllib.error, urllib.parse, urllib.request

HERE = pathlib.Path(__file__).parent
# secrets: .env locally, environment variables in CI (GitHub Actions)
ENV = {k: v for k, v in os.environ.items() if k in ('DATABASE_URL', 'APIFY_TOKEN', 'REOON_API_KEY', 'PH_TOKEN',
                                                      'INSTANTLY_API_KEY', 'AZURE_CHAT_KEY', 'PH_RESOLVER_URL', 'PH_RESOLVER_SECRET', 'TELEGRAM_BOT_TOKEN',
                                                      'TELEGRAM_CHAT_ID', 'GUMLOOP_API_KEY', 'GUMLOOP_USER_ID', 'GUMLOOP_AGENT_ID', 'APIFY_TOKENS', 'GITHUB_TOKEN', 'GITHUB_REPOSITORY')}
if (HERE / '.env').exists():
    ENV = {**{k: v.strip().strip('"') for k, v in (l.strip().split('=', 1) for l in open(HERE / '.env')
                                                   if '=' in l and not l.startswith('#'))}, **ENV}
UA = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 14_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126 Safari/537.36'
RECHECK_DAYS = 90                    # don't pay to look for a company's email again within this window
REOON_RESERVE = 25                   # leave this many daily Reoon power credits untouched

import psycopg                       # noqa: E402
from psycopg.rows import dict_row    # noqa: E402

class _DB:
    """Neon closes idle connections; reconnect once and retry instead of failing a long run."""
    conn = None

    def execute(self, *a):
        for attempt in (1, 2):
            try:
                if self.conn is None or self.conn.closed:
                    self.conn = psycopg.connect(ENV['DATABASE_URL'], autocommit=True, row_factory=dict_row)
                return self.conn.execute(*a)
            except psycopg.OperationalError:
                self.conn = None
                if attempt == 2: raise


DB = _DB()

# --------------------------------------------------------------------------------------- utils
PLATFORM_HOSTS = ('producthunt.com', 'apps.apple.com', 'play.google.com', 'chromewebstore.google.com', 'chrome.google.com',
                  'apps.shopify.com', 'github.com', 'linktr.ee', 'notion.site', 'x.com', 'twitter.com', 'linkedin.com',
                  'instagram.com', 'youtube.com', 'medium.com', 'substack.com', 'tiktok.com', 'facebook.com',
                  # the directories themselves: a startup listing one of these as its site has no site of its own
                  'trustmrr.com', 'uneed.best', 'betalist.com', 'microlaunch.net', 'tinylaunch.com', 'launchingnext.com',
                  'startupfa.st', 'ycombinator.com', 'crunchbase.com', 'indiehackers.com', 'theresanaiforthat.com')


def domain_of(url):
    d = re.sub(r'^[a-z]+://', '', (url or '').strip().lower()).split('/')[0].split('?')[0].split('#')[0].split(':')[0]
    d = d[4:] if d.startswith('www.') else d
    if not d or '.' not in d or any(d == p or d.endswith('.' + p) for p in PLATFORM_HOSTS):
        return ''
    return d


def http_get(url, timeout=12, limit=400_000):
    try:
        r = urllib.request.urlopen(urllib.request.Request(url, headers={'User-Agent': UA, 'Accept': 'text/html,*/*'}), timeout=timeout)
        return r.geturl(), r.read(limit).decode('utf-8', 'ignore')
    except Exception:
        return None, ''


class _NoFollow(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *a, **k): return None


_NOFOLLOW = urllib.request.build_opener(_NoFollow)


def resolve_ph_links(urls):
    """Product Hunt /r/ links -> real sites, through our ph-resolve Cloudflare Worker (PH's Cloudflare 403s GitHub and
    AWS IPs, but answers a Worker). Paced batches of 20, then up to 3 more passes over what is still unresolved,
    because PH rate-limits (429) a full day sent at once. Falls back to a direct HEAD (works from home connections)."""
    import collections
    out, todo = {}, list(dict.fromkeys(u for u in urls if u))
    if ENV.get('PH_RESOLVER_URL'):
        for p in range(4):
            seen = collections.Counter()
            for i in range(0, len(todo), 20):
                body = json.dumps({'urls': todo[i:i + 20]}).encode()
                try:
                    res = json.load(urllib.request.urlopen(urllib.request.Request(ENV['PH_RESOLVER_URL'], body, {
                        'Authorization': f"Bearer {ENV['PH_RESOLVER_SECRET']}", 'Content-Type': 'application/json',
                        'User-Agent': 'pp-daily-sync/1.0'}), timeout=120))['results']
                except Exception as e:
                    print('  ph-resolve worker call failed:', e); res = []
                for r in res:
                    seen[r.get('status')] += 1
                    if r.get('location'): out[r['url']] = r['location']
                time.sleep(2 if not seen.get(429) else 8)
            todo = [u for u in todo if u not in out]
            print(f'  ph-resolve pass {p + 1}: {len(out)} resolved, {len(todo)} left, statuses {dict(seen)}', flush=True)
            if not todo: break
            time.sleep(30)
    for u in urls:
        if u and u not in out: out[u] = resolve_redirect(u, tries=1)
    return out


def resolve_redirect(url, tries=4):
    """Product Hunt's /r/ tracking link: a HEAD request answers 301 with the real site in Location.
    A full GET gets rate-limited (429), so never follow it; back off on 429."""
    for attempt in range(tries):
        try:
            _NOFOLLOW.open(urllib.request.Request(url, method='HEAD', headers={'User-Agent': 'curl/8.7'}), timeout=15)
            return ''
        except urllib.error.HTTPError as e:
            if e.code in (301, 302, 303, 307, 308):
                return e.headers.get('Location', '')
            if e.code == 429:
                time.sleep(5 * (attempt + 1)); continue
            return ''
        except Exception:
            return ''
    return ''


# -------------------------------------------------------------------------------------- crawl
EMAIL_RX = re.compile(r'[a-z0-9][a-z0-9._%+-]{0,63}@[a-z0-9-]+(?:\.[a-z0-9-]+)*\.[a-z]{2,24}', re.I)
JUNK = re.compile(r'\.(png|jpe?g|gif|webp|svg|css|js)$|^(u00|x22|%)|example\.|sentry|wixpress|@(\d+x|2x|3x)|'
                  r'yourdomain|domain\.com|email\.com|test\.com|noreply|no-reply|donotreply|privacy@|abuse@|postmaster@|'
                  r'webmaster@|dmca@|legal@|security@|gdpr@', re.I)
PAGES = ['', '/about', '/about-us', '/team', '/contact', '/company']
ROLE = set('info hello contact support team admin sales hi help office enquiries inquiries marketing partnerships press '
           'media business founders founder general mail billing careers jobs hey yo feedback service care customer '
           'customerservice orders shop store partners partner growth sponsor ads advertising investors dev developers '
           'api app studio design community news newsletter social pr ops operations accounts hr legal booking '
           'bookings request requests apps product products contacto kontakt bonjour hola ai dpo official destek hallo halo main data'.split())


def crawl(website, max_pages=6):
    """Fetch up to 6 pages of the site, return emails (on-domain first) and social links. Free."""
    dom = domain_of(website)
    base = f'https://{dom}' if dom else ''
    emails, socials = {}, set()
    if not base:
        return [], []
    for i, p in enumerate(PAGES[:max_pages]):
        final, page = http_get(base + p)
        if not page:
            if i == 0: return [], []          # homepage unreachable: don't hammer the rest
            continue
        txt = html.unescape(page)
        for e in EMAIL_RX.findall(txt):
            e = e.lower().strip('.')
            if not JUNK.search(e): emails.setdefault(e, p or '/')
        for s in re.findall(r'https?://(?:www\.)?(?:x\.com|twitter\.com|linkedin\.com/(?:in|company))/[A-Za-z0-9_\-/%.]+', txt):
            socials.add(s.split('?')[0].rstrip('/'))
    on = [e for e in emails if e.endswith('@' + dom) or e.endswith('.' + dom)]
    return on + [e for e in emails if e not in on], sorted(socials)[:12]


# -------------------------------------------------------------------------------------- guess
def clean_name(n):
    n = re.sub(r'\(.*?\)|[^A-Za-zÀ-ÿ\s\'-]', ' ', n or '').strip()
    parts = [p for p in n.split() if len(p) > 1 or len(n.split()) == 1]
    return [p.lower().translate(str.maketrans('àáâäãåèéêëìíîïòóôöõùúûüñç', 'aaaaaaeeeeiiiiooooouuuunc')).replace("'", '') for p in parts]


def guesses(name, dom):
    p = clean_name(name)
    if not p or not dom: return []
    f, l = p[0], (p[-1] if len(p) > 1 else '')
    out = [f'{f}@{dom}']
    if l: out += [f'{f}.{l}@{dom}', f'{f[0]}{l}@{dom}', f'{f}{l}@{dom}', f'{f[0]}.{l}@{dom}']
    return out


def is_role(email):
    return email.split('@')[0].split('+')[0] in ROLE


# ------------------------------------------------------------------------------------- verify
class Verifier:
    """Free local SMTP probe when port 25 works (this Mac), else Reoon power mode on a daily budget.
    One catch-all test per domain decides whether guesses are worth checking at all."""

    def __init__(self, run_id, smtp=None):
        self.run_id, self.spent = run_id, 0
        self.smtp = self._port25() if smtp is None else smtp
        self.reoon_left = self._reoon_balance() if not self.smtp else None

    @staticmethod
    def _port25():
        try:
            socket.create_connection(('gmail-smtp-in.l.google.com', 25), timeout=6).close(); return True
        except Exception:
            return False

    @staticmethod
    def _reoon_balance():
        try:
            d = json.load(urllib.request.urlopen(f"https://emailverifier.reoon.com/api/v1/check-account-balance/?key={ENV['REOON_API_KEY']}", timeout=20))
            return int(d.get('remaining_daily_credits') or 0)
        except Exception:
            return 0

    @staticmethod
    def _mx(dom):
        out = subprocess.run(['dig', '+short', 'MX', dom], capture_output=True, text=True, timeout=15).stdout.split('\n')
        mx = sorted((int(l.split()[0]), l.split()[1].rstrip('.')) for l in out if len(l.split()) == 2 and l.split()[0].isdigit())
        return [h for _, h in mx]

    def _smtp_probe(self, dom, emails):
        """-> {email: valid|invalid|unknown}, catch_all bool|None. Control address first."""
        mxs = self._mx(dom)
        if not mxs: return {e: 'invalid' for e in emails}, None
        control = ''.join(random.choices(string.ascii_lowercase, k=14)) + '@' + dom
        try:
            s = smtplib.SMTP(mxs[0], 25, timeout=12); s.ehlo('mail.amplifyxlabs.com'); s.mail('check@amplifyxlabs.com')
            ca = s.rcpt(control)[0] == 250
            res = {}
            for e in emails:
                code = s.rcpt(e)[0]
                res[e] = 'catch_all' if ca else ('valid' if code == 250 else 'invalid' if code in (550, 551, 553) else 'unknown')
            s.quit()
            return res, ca
        except Exception:
            return {e: 'unknown' for e in emails}, None

    def _reoon(self, email):
        if self.reoon_left is not None and self.reoon_left - self.spent <= REOON_RESERVE:
            return 'unverified'
        try:
            d = json.load(urllib.request.urlopen(
                f"https://emailverifier.reoon.com/api/v1/verify?email={urllib.parse.quote(email)}&key={ENV['REOON_API_KEY']}&mode=power", timeout=60))
        except Exception:
            return 'unverified'
        self.spent += 1
        log_spend(self.run_id, 'reoon', 1)
        return {'safe': 'valid', 'role_account': 'role', 'catch_all': 'catch_all', 'invalid': 'invalid', 'disabled': 'invalid',
                'disposable': 'invalid', 'spamtrap': 'invalid', 'inbox_full': 'invalid'}.get(d.get('status'), 'unknown')

    def check(self, dom, candidates, stop_at_first=True):
        """candidates in priority order -> (results {email: status}, catch_all)."""
        if not candidates: return {}, None
        if self.smtp:
            res, ca = self._smtp_probe(dom, candidates)
            return res, ca
        res = {}
        # one credit tells us if the domain accepts everything; if so, guesses can't be proven
        ca_probe = self._reoon(''.join(random.choices(string.ascii_lowercase, k=12)) + '@' + dom)
        if ca_probe in ('catch_all', 'valid', 'role'):
            return {e: 'catch_all' for e in candidates}, True
        for e in candidates:
            res[e] = self._reoon(e)
            if stop_at_first and res[e] in ('valid', 'role'): break
        return res, False


# --------------------------------------------------------------------------------------- tags
CONSUMER = re.compile(r'consumer|lifestyle|health|fitness|wellness|game|gaming|e-?commerce|shopping|fashion|beauty|food|'
                      r'travel|pet|parent|music|photo|dating|social network|kids|home|sport|personal finance|meditation', re.I)
B2B = re.compile(r'saas|b2b|productivity|developer|dev ?tools|api|analytics|marketing|sales|crm|automation|no-?code|'
                 r'security|finance|fintech|hr|recruit|customer|support|data|infrastructure|ai agent|enterprise|workflow', re.I)
GTM = re.compile(r'marketing|sales|growth|seo|social media|twitter|lead|outreach|developer|ai|startup|creator|newsletter|community', re.I)


def fit(lists, topics, dr, signals, on_ph):
    """Rule-based product fit. Returns (products, reason). No AI. Order of evidence: list > revenue/funding > topics > DR."""
    t = ' '.join(topics or [])
    consumer, b2b, gtm = bool(CONSUMER.search(t)), bool(B2B.search(t)), bool(GTM.search(t))
    mrr = float(signals.get('mrr') or 0)
    lists = set(lists)
    funded = 'funded' in lists or bool(signals.get('funding_usd')) or (signals.get('funding_stage') or '').lower() in (
        'early stage', 'growth stage', 'late stage', 'series a', 'series b', 'seed') and bool(signals.get('growing_fast'))
    out, why = set(), []
    def add(p, r): out.add(p); why.append(f'{p}: {r}')
    # authority: directories for new sites, the outreach agent for established ones
    if dr is None or dr < 30:
        if lists & {'producthunt', 'launches', 'chrome', 'shopify', 'revenue', 'showhn', 'firefox', 'play', 'hiring'}:
            add('backlinkbot', f'DR {dr if dr is not None else "?"} < 30')
    elif dr < 60 and not consumer: add('backlinkbot', f'DR {dr}, outreach agent')
    # money decides the Reddit offer: done-for-you for companies with budget, self-serve below that
    if funded or mrr >= 5000: add('subredditmarketing', 'funded' if funded else f'MRR ${mrr:,.0f}')
    elif 'revenue' in lists or (b2b and (dr or 0) >= 30): add('growwithreddit', f'MRR ${mrr:,.0f}' if mrr else 'B2B with traction')
    if consumer or lists & {'shopify', 'play'}: add('hookads', 'consumer, merchant or mobile app')
    if 'stores' in lists and signals.get('meta_pixel'): add('hookads', 'store already runs Meta ads (pixel)')
    if 'stores' in lists and (signals.get('tiktok') or signals.get('tiktok_pixel')): add('tokgency', 'store on TikTok')
    if lists & {'hiring'} and not funded: add('growwithreddit', 'hiring for growth')
    if not consumer and (gtm or b2b or lists & {'revenue', 'funded', 'chrome', 'firefox', 'hiring'}): add('xautodm', 'sells to founders/marketers/devs on X')
    if lists & {'chrome', 'firefox'}: add('growwithreddit', 'extension looking for users')
    if lists & {'launches', 'chrome', 'showhn', 'firefox'} and not on_ph: add('phlaunchkit', 'not on Product Hunt yet')
    if consumer and (funded or mrr >= 10000): add('tokgency', 'consumer brand with budget')
    return sorted(out), '; '.join(why)


def retag():
    """Recompute fit for every lead whose company is on a list. Rules only; one bulk write."""
    rows = DB.execute("""select l.email, c.lists, c.topics, c.domain_rating, c.signals, c.domain
                         from leads l join companies c on c.domain = l.company_domain where cardinality(c.lists) > 0""").fetchall()
    ph = on_producthunt({r['domain'] for r in rows})
    emails, fits, whys = [], [], []
    for r in rows:
        p, why = fit(r['lists'], r['topics'], r['domain_rating'], r['signals'] or {}, r['domain'] in ph)
        emails.append(r['email']); fits.append('{' + ','.join(p) + '}'); whys.append(why)
    DB.execute("""update leads l set fit_products = v.f::text[], fit_reason = v.w
                  from unnest(%s::text[], %s::text[], %s::text[]) as v(e, f, w) where l.email = v.e""", (emails, fits, whys))
    # founder = a personal mailbox, company = a shared inbox (hello@, support@...); the two get different campaigns
    DB.execute("""update leads set contact_type = case when reoon_status = 'role_account'
                    or split_part(split_part(email, '@', 1), '+', 1) = any(%s) then 'company' else 'founder' end
                  where contact_type is null""", (sorted(ROLE),))
    DB.execute('select refresh_buckets()')
    return len(rows)


# ------------------------------------------------------------------------------------ storage
def start_run(list_, source):
    return DB.execute('insert into engine_runs (list, source) values (%s, %s) returning id', (list_, source)).fetchone()['id']


def finish_run(run_id, stats, ok=True):
    DB.execute('update engine_runs set finished_at = now(), stats = %s, ok = %s where id = %s', (json.dumps(stats), ok, run_id))


def log_spend(run_id, provider, units, usd=None):
    DB.execute('insert into spend_log (run_id, provider, units, usd) values (%s, %s, %s, %s)', (run_id or None, provider, units, usd))


def known(domains):
    """Check Neon before spending anything. -> {domain: state}
    state: new | has_contact | looked_recently | retry | suppressed"""
    if not domains: return {}
    rows = DB.execute(f"""
      select d.domain,
             c.domain is not null as exists,
             c.email_lookup_at > now() - interval '{RECHECK_DAYS} days' as recent,
             exists (select 1 from leads l where l.company_domain = d.domain
                     and coalesce(l.email_status, 'unverified') <> 'invalid') as has_contact,
             exists (select 1 from leads l join suppression s using (email) where l.company_domain = d.domain) as suppressed
      from unnest(%s::text[]) d(domain) left join companies c on c.domain = d.domain""", (list(domains),)).fetchall()
    out = {}
    for r in rows:
        out[r['domain']] = ('has_contact' if r['has_contact'] else 'looked_recently' if r['recent']
                            else 'retry' if r['exists'] else 'new')
    return out


def on_producthunt(domains):
    rows = DB.execute("""select distinct l.company_domain d from leads l join lead_sources s using (email)
                         where s.source in ('scraper:producthunt', 'gumloop:producthunt', 'engine:producthunt')
                           and l.company_domain = any(%s)""", (list(domains),)).fetchall()
    return {r['d'] for r in rows}


def dr_lookup(domains, run_id):
    """DataForSEO authority via treg for domains we have no rating for. ~$0.06 per 1,000. Skips quietly if treg fails."""
    have = {r['domain']: r['domain_rating'] for r in DB.execute(
        'select domain, domain_rating from companies where domain = any(%s)', (list(domains),)).fetchall()}
    todo = [d for d in domains if have.get(d) is None]
    for i in range(0, len(todo), 1000):
        batch = todo[i:i + 1000]
        out = subprocess.run(['treg', 'call', 'dataforseo.web.url.metrics', '--method', 'POST', '--data',
                              json.dumps([{'targets': batch, 'rank_scale': 'one_hundred'}])], capture_output=True, text=True)
        try:
            j = json.loads(out.stdout); j = j.get('raw', j)
            t = j['tasks'][0]
            for it in t['result'][0]['items']: have[it['target']] = it['rank']
            log_spend(run_id, 'treg', len(batch), t.get('cost'))
        except Exception:
            print('  DR lookup skipped (treg):', (out.stdout or out.stderr)[:160])
            break
    return have


def save_company(c, run_id):
    DB.execute("""
      insert into companies (domain, name, lists, topics, signals, domain_rating, email_lookup_at, email_lookup_result, catch_all, site_title)
      values (%(domain)s, %(name)s, %(lists)s, %(topics)s, %(signals)s, %(dr)s, %(lookup_at)s, %(lookup_result)s, %(catch_all)s, %(title)s)
      on conflict (domain) do update set
        name = coalesce(companies.name, excluded.name),
        lists = (select array(select distinct unnest(companies.lists || excluded.lists))),
        topics = (select array(select distinct unnest(companies.topics || excluded.topics))),
        signals = companies.signals || excluded.signals,
        domain_rating = coalesce(excluded.domain_rating, companies.domain_rating),
        email_lookup_at = coalesce(excluded.email_lookup_at, companies.email_lookup_at),
        email_lookup_result = coalesce(excluded.email_lookup_result, companies.email_lookup_result),
        catch_all = coalesce(excluded.catch_all, companies.catch_all),
        site_title = coalesce(companies.site_title, excluded.site_title),
        updated_at = now()""",
        {**c, 'signals': json.dumps(c.get('signals') or {}), 'lookup_at': c.get('lookup_at'),
         'lookup_result': c.get('lookup_result'), 'catch_all': c.get('catch_all'), 'title': c.get('title')})


def save_lead(l, source, ref, payload, run_id):
    """Merge one contact into leads (+ its lead_sources row). Identity fields fill blanks only."""
    DB.execute("""
      insert into leads (email, first_name, last_name, company, website, company_domain, title, linkedin, x_handle,
                         first_source, audience, best_bucket, email_status, verified_at, fit_products, fit_reason,
                         contact_type, email_origin, email_type, ph_launch_date, ph_tagline)
      values (%(email)s, %(first_name)s, %(last_name)s, %(company)s, %(website)s, %(domain)s, %(title)s, %(linkedin)s,
              %(x)s, %(source)s, %(audience)s, 'Not contacted', %(status)s, case when %(status)s <> 'unverified' then now() end,
              %(fit)s, %(fit_reason)s, %(contact_type)s, %(origin)s, %(email_type)s, %(launch_date)s, %(tagline)s)
      on conflict (email) do update set
        first_name = coalesce(leads.first_name, excluded.first_name), last_name = coalesce(leads.last_name, excluded.last_name),
        company = coalesce(leads.company, excluded.company), website = coalesce(leads.website, excluded.website),
        company_domain = coalesce(leads.company_domain, excluded.company_domain), title = coalesce(leads.title, excluded.title),
        linkedin = coalesce(leads.linkedin, excluded.linkedin), x_handle = coalesce(leads.x_handle, excluded.x_handle),
        email_status = case when coalesce(leads.email_status, 'unverified') in ('unverified', 'unknown')
                            then excluded.email_status else leads.email_status end,
        verified_at = coalesce(leads.verified_at, excluded.verified_at),
        fit_products = (select array(select distinct unnest(leads.fit_products || excluded.fit_products))),
        fit_reason = coalesce(excluded.fit_reason, leads.fit_reason),
        contact_type = coalesce(leads.contact_type, excluded.contact_type),
        email_origin = coalesce(leads.email_origin, excluded.email_origin),
        ph_launch_date = coalesce(leads.ph_launch_date, excluded.ph_launch_date),
        ph_tagline = coalesce(leads.ph_tagline, excluded.ph_tagline),
        last_seen_at = now()""", {**l, 'source': source})
    DB.execute("""insert into lead_sources (email, source, source_ref, run_id, payload) values (%s, %s, %s, %s, %s)
                  on conflict (email, source, source_ref) do update set payload = lead_sources.payload || excluded.payload,
                  received_at = now()""", (l['email'], source, ref or '', str(run_id), json.dumps(payload, default=str)))


def note_appearance(domain, source, ref, payload, run_id):
    """Company already known: record that it showed up here again, on every lead we hold for it. Free."""
    for r in DB.execute('select email from leads where company_domain = %s', (domain,)).fetchall():
        DB.execute("""insert into lead_sources (email, source, source_ref, run_id, payload) values (%s, %s, %s, %s, %s)
                      on conflict (email, source, source_ref) do nothing""",
                   (r['email'], source, ref or '', str(run_id), json.dumps(payload, default=str)))
