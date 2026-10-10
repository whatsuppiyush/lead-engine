// ph-resolve: turns Product Hunt /r/ tracking links into the product's real website.
// Product Hunt's Cloudflare returns 403 to GitHub Actions and AWS (Neon Functions); requests from a Worker get the 301.
//   POST /  Authorization: Bearer <RESOLVER_SECRET>  {"urls": [...]}   (max 40: free-plan subrequest limit is 50)
//   -> {"results": [{"url": "...", "status": 301, "location": "https://site.com/?ref=producthunt"}]}
export default {
  async fetch(req, env) {
    if (req.method !== 'POST') return Response.json({ ok: true, service: 'ph-resolve' });
    if (req.headers.get('authorization') !== `Bearer ${env.RESOLVER_SECRET}`) return Response.json({ error: 'unauthorized' }, { status: 401 });
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
