JS_EXTRACT = """
// Get category from h1
    let category_name = null;
    const heading = document.querySelector("h1.a-size-large.a-spacing-medium.a-text-bold");
    if (heading && heading.textContent.includes("Best Sellers in")) {
        category_name = heading.textContent.replace("Best Sellers in", "").trim();
    }
(nodes) => nodes.map(node => {
    const q  = sel => node.querySelector(sel);
    const txt = sel => {
        const el = q(sel);
        return el ? el.textContent.trim() : null;
    };

    // ─────────────── Rank ───────────────
    let rank = null;
    let cur  = node;                                   // start at the card
    for (let i = 0; i < 5 && cur && !rank; i++) {      // climb ≤5 levels
        const badge = cur.querySelector('span.zg-bdg-text, .zg-bdg-text');
        if (badge && badge.textContent.trim()) {
            rank = badge.textContent.trim().replace('#', '');
            break;
        }
        cur = cur.parentElement;
    }

    // title
    const title = txt("div[class*='_cDEzb_p13n-sc-css-line-clamp']") ||
                  txt('.p13n-sc-truncated') ||
                  txt('h3 a span') || null;

    // price – skip “offers from…” rows
    let price = null;
    for (const sp of node.querySelectorAll("span[class*='p13n-sc-price']")) {
        const pc = sp.parentElement?.className || '';
        if (/color-secondary/.test(pc)) continue;
        price = sp.textContent.trim();
        break;
    }

    // rating
    const ratingFull = txt('span.a-icon-alt');
    const rating = ratingFull ? ratingFull.split(' ')[0] : null;

    // reviews
    let reviews = txt('.a-size-small') || txt("a[href*='#customerReviews']");
    if (reviews && !/[0-9]/.test(reviews)) reviews = null;

    // link / asin
    const linkEl = q("a[href*='/dp/']");
    const href    = linkEl ? linkEl.getAttribute('href') : null;
    const asin    = href ? (href.match(/\\/dp\\/([A-Z0-9]{10})/)||[])[1] : null;
    const product_url = href ? (href.startsWith('http') ? href : `https://www.amazon.com${href}`) : null;

    // image (pick widest entry in data‑a‑dynamic‑image if src is low‑res)
    let image_url = q('img')?.getAttribute('src') || null;
    const dynAttr = q('img')?.getAttribute('data-a-dynamic-image') || null;
    if (dynAttr) {
        try {
            const obj = JSON.parse(dynAttr);
            const best = Object.keys(obj).sort((a,b)=>obj[b][0]-obj[a][0])[0];
            if (best) image_url = best;
        } catch {}
    }

    const required = { title, price, rating };   // tweak list as needed
    const missing  = Object.entries(required)
                          .filter(([k, v]) => !v || v === 'null' || v === '0')
                          .map(([k]) => k);
    return { category_name, asin, rank, title, price, rating, reviews,
            product_url, image_url, _missing: missing };

})
"""

JS_SINGLE_NODE_EXTRACT = """
(node) => {
    /* ---------- category (from page heading) ---------- */
    let category_name = null;
    const heading = document.querySelector(
        "h1.a-size-large.a-spacing-medium.a-text-bold"
    );
    if (heading && heading.textContent.includes("Best Sellers in")) {
        category_name = heading.textContent.replace("Best Sellers in", "").trim();
    }

    /* ---------- helpers ---------- */
    const q  = sel => node.querySelector(sel);
    const txt = sel => {
        const el = q(sel);
        return el ? el.textContent.trim() : null;
    };

    /* ---------- rank ---------- */
    let rank = null;
    let cur  = node;
    for (let i = 0; i < 5 && cur && !rank; i++) {
        const badge = cur.querySelector("span.zg-bdg-text, .zg-bdg-text");
        if (badge && badge.textContent.trim()) {
            rank = badge.textContent.trim().replace("#", "");
            break;
        }
        cur = cur.parentElement;
    }

    /* ---------- title ---------- */
    const title =
        txt("div[class*='_cDEzb_p13n-sc-css-line-clamp']") ||
        txt(".p13n-sc-truncated") ||
        txt("h3 a span") ||
        null;

    /* ---------- price (skip secondary offers) ---------- */
    let price = null;
    for (const sp of node.querySelectorAll("span[class*='p13n-sc-price']")) {
        const pc = sp.parentElement?.className || "";
        if (/color-secondary/.test(pc)) continue;
        price = sp.textContent.trim();
        break;
    }

    /* ---------- rating & reviews ---------- */
    const ratingFull = txt("span.a-icon-alt");
    const rating = ratingFull ? ratingFull.split(" ")[0] : null;

    let reviews = txt(".a-size-small") || txt("a[href*='#customerReviews']");
    if (reviews && !/[0-9]/.test(reviews)) reviews = null;

    /* ---------- link / ASIN ---------- */
    const linkEl = q("a[href*='/dp/']");
    const href   = linkEl ? linkEl.getAttribute("href") : null;
    const asin   = href ? (href.match(/\\/dp\\/([A-Z0-9]{10})/) || [])[1] : null;
    const product_url = href
        ? href.startsWith("http")
            ? href
            : `https://www.amazon.com${href}`
        : null;

    /* ---------- image (choose widest) ---------- */
    let image_url = q("img")?.getAttribute("src") || null;
    const dynAttr = q("img")?.getAttribute("data-a-dynamic-image") || null;
    if (dynAttr) {
        try {
            const obj = JSON.parse(dynAttr);
            const best = Object.keys(obj).sort((a, b) => obj[b][0] - obj[a][0])[0];
            if (best) image_url = best;
        } catch (_) {}
    }

    /* ---------- completeness check ---------- */
    const required = { title, price, rating };
    const missing = Object.entries(required)
        .filter(([_, v]) => !v || v === "null" || v === "0")
        .map(([k]) => k);

    return {
        category_name,
        asin,
        rank,
        title,
        price,
        rating,
        reviews,
        product_url,
        image_url,
        _missing: missing,
    };
}
"""
