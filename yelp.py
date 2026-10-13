"""Local businesses from Yelp: phone first (for WhatsApp), email from the business's own website.

  python yelp.py --query "Plumbers,Dentists" --location "Austin, TX;Miami, FL" [--limit 240]
  python yelp.py --query Plumbers,Med_Spas --location Austin_TX/Miami_FL      (same, quote-free, for GitHub args)
  python yelp.py --from-file saved.json --query Plumbers --location "Austin, TX"     (load results already scraped)

Every business lands in yelp_businesses (one row per Yelp id). first_seen_at is the day we first saw it, which is
the closest we get to "newly listed": Yelp does not publish a listing date and blocks its Hot & New pages.
Emails found on the website also go to leads (source engine:yelp) so Reoon checks them like everything else.
"""
import argparse, concurrent.futures as cf, json
from core import DB, crawl, domain_of, is_role, save_company, save_lead, start_run, finish_run
from sources import apify

# daily rotation: each day the searches run longest ago go first (Tier 1 local businesses, fast-growing US metros)
CATEGORIES = ['Roofers', 'HVAC', 'Plumbers', 'Electricians', 'Remodeling Contractors', 'Landscapers', 'Personal Injury Lawyers',
              'Med Spas', 'Chiropractors', 'Auto Repair', 'Garage Door Services', 'Pest Control', 'Movers', 'Solar Installers',
              'Pool Services', 'Cosmetic Dentists', 'Physical Therapy', 'Home Cleaning', 'Real Estate Agents', 'Gyms']
CITIES = ['Austin, TX', 'Dallas, TX', 'Phoenix, AZ', 'Tampa, FL', 'Houston, TX', 'Atlanta, GA', 'Charlotte, NC', 'Nashville, TN',
          'Denver, CO', 'Orlando, FL', 'San Antonio, TX', 'Las Vegas, NV', 'Raleigh, NC', 'Miami, FL', 'Jacksonville, FL']

FREE_MAIL = ('gmail.com', 'yahoo.com', 'hotmail.com', 'outlook.com', 'aol.com', 'icloud.com', 'live.com', 'msn.com', 'att.net',
             'comcast.net', 'sbcglobal.net', 'verizon.net', 'me.com', 'protonmail.com', 'proton.me')

DB.execute("""create table if not exists yelp_businesses (
  yelp_id text primary key, name text, phone text, phone_display text, website text, domain text, emails text[],
  query text, location text, city text, state text, country text, address text, categories text[],
  rating numeric, review_count int, price text, is_advertiser bool, call_tracking bool, is_chain bool, is_closed bool,
  message_enabled bool, response_time text, yelp_url text, raw jsonb,
  whatsapp text default 'unknown',                 -- unknown | yes | no, set when someone checks
  first_seen_at timestamptz default now(), last_seen_at timestamptz default now())""")


DB.execute("""create table if not exists yelp_searches (query text, location text, last_run_at timestamptz, businesses int,
  primary key (query, location))""")


def next_searches(n):
    """The n category x city pairs run longest ago (never-run first)."""
    done = {(r['query'], r['location']): r['last_run_at'] for r in DB.execute('select * from yelp_searches').fetchall()}
    pairs = [(q, c) for c in CITIES for q in CATEGORIES]
    return sorted(pairs, key=lambda p: (done.get(p) is not None, done.get(p) or 0))[:n]


def own_emails(site, found):
    d = domain_of(site)
    keep = [e for e in found if d and e.endswith('@' + d)] or [e for e in found if e.split('@')[1] in FREE_MAIL]
    return list(dict.fromkeys(keep))[:3]


def run(queries, locations, limit, from_file=None, pairs=None):
    run_id = start_run('yelp', 'yelp')
    stats = {'items': 0, 'new': 0, 'with_phone': 0, 'with_website': 0, 'with_email': 0, 'advertisers': 0}
    for q, loc in (pairs or [(q, l) for q in queries for l in locations]):
        if True:
            items = json.load(open(from_file)) if from_file else apify(
                'api-ninja/yelp-ultimate-scraper', {'query': q, 'location': loc, 'numberOfResults': max(40, limit),
                                                    'details': 'advanced'}, run_id, max_usd=max(0.25, limit * 0.0055))
            items = [i for i in items if i.get('id') and not i.get('is_closed')]
            known = {r['yelp_id'] for r in DB.execute('select yelp_id from yelp_businesses where yelp_id = any(%s)',
                                                      ([i['id'] for i in items],)).fetchall()}
            have_email = {r['yelp_id'] for r in DB.execute(
                "select yelp_id from yelp_businesses where yelp_id = any(%s) and cardinality(emails) > 0", ([i['id'] for i in items],)).fetchall()}
            todo = [i for i in items if i['id'] not in have_email and i.get('display_url')]
            with cf.ThreadPoolExecutor(12) as ex:
                found = dict(zip([i['id'] for i in todo], ex.map(lambda i: crawl(i['display_url'] if i['display_url'].startswith('http')
                                                                                  else 'http://' + i['display_url'])[0], todo)))
            for i in items:
                site = i.get('display_url') or ''
                site = site if not site or site.startswith('http') else 'http://' + site
                emails = own_emails(site, found.get(i['id'], []))
                fb = i.get('from_this_business') or {}
                row = {'id': i['id'], 'name': i.get('name'), 'phone': i.get('phone'), 'phone_display': i.get('localized_phone'),
                       'website': site, 'domain': domain_of(site), 'emails': emails, 'query': q, 'location': loc,
                       'city': i.get('city'), 'state': i.get('state'), 'country': i.get('country'),
                       'address': ', '.join(x for x in (i.get('address1'), i.get('city'), i.get('state'), i.get('zip')) if x),
                       'categories': [c.get('name') for c in i.get('categories') or [] if isinstance(c, dict)],
                       'rating': i.get('avg_rating'), 'reviews': i.get('review_count'), 'price': i.get('price'),
                       'adv': bool(i.get('is_current_advertiser')), 'ct': bool(i.get('has_ad_driven_call_tracking_number')),
                       'chain': bool(i.get('is_chain_business')), 'closed': bool(i.get('is_closed')),
                       'msg': bool(i.get('is_message_to_business_enabled')),
                       'rt': (i.get('message_the_business') or {}).get('response_time'),
                       'url': (i.get('share_url') or '').split('?')[0], 'raw': json.dumps({'year_established': fb.get('year_established')})}
                DB.execute("""insert into yelp_businesses (yelp_id, name, phone, phone_display, website, domain, emails, query, location,
                    city, state, country, address, categories, rating, review_count, price, is_advertiser, call_tracking, is_chain,
                    is_closed, message_enabled, response_time, yelp_url, raw)
                  values (%(id)s, %(name)s, %(phone)s, %(phone_display)s, %(website)s, %(domain)s, %(emails)s, %(query)s, %(location)s,
                    %(city)s, %(state)s, %(country)s, %(address)s, %(categories)s, %(rating)s, %(reviews)s, %(price)s, %(adv)s, %(ct)s,
                    %(chain)s, %(closed)s, %(msg)s, %(rt)s, %(url)s, %(raw)s)
                  on conflict (yelp_id) do update set phone = excluded.phone, rating = excluded.rating, review_count = excluded.review_count,
                    is_advertiser = excluded.is_advertiser, call_tracking = excluded.call_tracking, is_closed = excluded.is_closed,
                    emails = case when cardinality(excluded.emails) > 0 then excluded.emails else yelp_businesses.emails end,
                    last_seen_at = now()""", row)
                stats['items'] += 1; stats['new'] += i['id'] not in known
                stats['with_phone'] += bool(row['phone']); stats['with_website'] += bool(site)
                stats['with_email'] += bool(emails); stats['advertisers'] += row['adv']
                if emails and row['domain']:
                    save_company({'domain': row['domain'], 'name': row['name'], 'lists': ['yelp'], 'topics': row['categories'],
                                  'signals': {'yelp_rating': row['rating'], 'yelp_reviews': row['reviews'], 'yelp_advertiser': row['adv'],
                                              'city': row['city'], 'country': row['country']},
                                  'dr': None, 'lookup_at': 'now', 'lookup_result': 'found', 'catch_all': None, 'title': None}, run_id)
                for e in (emails if row['domain'] else []):
                    save_lead({'email': e, 'first_name': None, 'last_name': None, 'company': row['name'], 'website': site,
                               'domain': row['domain'] or e.split('@')[1], 'title': None, 'linkedin': None, 'x': None,
                               'audience': 'Local businesses', 'status': 'role' if is_role(e) else 'unverified', 'fit': [],
                               'fit_reason': None, 'contact_type': 'company' if is_role(e) else 'founder', 'origin': 'listed',
                               'email_type': 'role inbox' if is_role(e) else 'listed on website', 'launch_date': None, 'tagline': None},
                              'engine:yelp', row['url'], {'yelp_id': i['id'], 'phone': row['phone'], 'query': q, 'location': loc}, run_id)
            DB.execute("""insert into yelp_searches values (%s, %s, now(), %s) on conflict (query, location)
                          do update set last_run_at = now(), businesses = excluded.businesses""", (q, loc, len(items)))
            print(f'  {q} in {loc}: {len(items)} businesses, {len(items) - len(known)} new', flush=True)
    finish_run(run_id, stats)
    print(json.dumps(stats))


if __name__ == '__main__':
    ap = argparse.ArgumentParser()
    ap.add_argument('--rotate', type=int, help='run the N searches from CATEGORIES x CITIES that ran longest ago')
    ap.add_argument('--query', default='', help='comma-separated Yelp searches, e.g. "Plumbers,Dentists"')
    ap.add_argument('--location', default='', help='semicolon-separated, e.g. "Austin, TX;Miami, FL"')
    ap.add_argument('--limit', type=int, default=40, help='businesses per search (Yelp stops around 240)')
    ap.add_argument('--from-file', help='load a saved actor result instead of scraping')
    a = ap.parse_args()
    # GitHub passes args without quotes, so "_" stands for a space and "/" separates locations: Austin,_TX/Dallas,_TX
    q = [x.replace('_', ' ').strip() for x in a.query.split(',') if x.strip()]
    locs = [x.replace('_', ' ').strip() for x in a.location.replace('/', ';').split(';') if x.strip()]
    run(q, locs, a.limit, a.from_file, next_searches(a.rotate) if a.rotate else None)
