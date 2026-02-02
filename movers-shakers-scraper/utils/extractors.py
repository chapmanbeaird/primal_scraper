JS_EXTRACT = """
(nodes) => {
  const out = [];
  for (const node of nodes) {
    const q  = sel => node.querySelector(sel);
    const qa = sel => Array.from(node.querySelectorAll(sel));
    const txt = sel => { const el = q(sel); return el ? el.textContent.trim() : null; };

    // ---------- movers_rank ----------
    let movers_rank = null;
    { let cur = node;
      for (let i = 0; i < 6 && cur && !movers_rank; i++) {
        const badge = cur.querySelector('span.zg-bdg-text, .zg-bdg-text');
        if (badge && badge.textContent.trim()) { movers_rank = badge.textContent.trim().replace('#',''); break; }
        cur = cur.parentElement;
      }
    }

    // ---------- title ----------
    const title = txt("div[class*='_cDEzb_p13n-sc-css-line-clamp']") || txt("h3 a span") || null;

    // ---------- price ----------
    let price = null;
    const priceSpans = qa("span[class*='p13n-sc-price']");
    if (priceSpans.length) price = priceSpans[0].textContent.trim();
    if (!price) {
      const block = q("span.a-size-base.a-color-price");
      if (block) { const m = block.textContent.match(/\\$[\\d.,]+/g); if (m && m.length) price = m[0]; }
    }

    // ---------- rating & reviews ----------
    const ratingFull = txt("span.a-icon-alt");
    const rating = ratingFull ? ratingFull.split(" ")[0] : null;
    let reviews = txt(".a-size-small") || txt("a[href*='#customerReviews']");
    if (reviews && !/[0-9]/.test(reviews)) reviews = null;

    // ---------- link / ASIN ----------
    const linkEl = q("a[href*='/dp/']");
    const href   = linkEl ? linkEl.getAttribute("href") : null;
    const asin   = href ? (href.match(/\\/dp\\/([A-Z0-9]{10})/) || [])[1] : null;
    const product_url = href ? (href.startsWith("http") ? href : `https://www.amazon.com${href}`) : null;

    // ---------- product_image ----------
    let product_image = q("img")?.getAttribute("src") || null;
    const dynAttr = q("img")?.getAttribute("data-a-dynamic-image") || null;
    if (dynAttr) { try { const obj = JSON.parse(dynAttr);
      const best = Object.keys(obj).sort((a,b)=>obj[b][0]-obj[a][0])[0]; if (best) product_image = best; } catch {} }

    // ---------- meta (change & sales ranks) ----------
    let meta = null; { let cur2 = node;
      for (let i = 0; i < 6 && cur2 && !meta; i++) { meta = cur2.querySelector("span[class*='zg-grid-rank-metadata']"); cur2 = cur2.parentElement; }
    }

    // ---------- category_name from H1 ----------
    let category_name = null;
    const h = document.querySelector("h1.a-size-large.a-spacing-medium.a-text-bold");
    if (h) {
      let t = h.textContent.trim();
      t = t.replace(/^Best\\s*Sellers\\s*in\\s*/i, '')
           .replace(/^Movers\\s*&\\s*Shakers\\s*in\\s*/i, '')
           .replace(/^Amazon\\s*Movers\\s*&\\s*Shakers\\s*:?\s*/i, '')
           .trim();
      if (t) category_name = t;
    }

    let change = null, sales_rank_now = null, sales_rank_before = null;
    if (meta) {
      const changeEl = meta.querySelector(".zg-grid-pct-change");
      if (changeEl) change = changeEl.textContent.trim();
      const t = meta.textContent.replace(/\\s+/g, " ").trim();
      let mPair = t.match(/Sales\\s*rank:\\s*#?\\s*([0-9,]+)\\s*\\(\\s*was\\s*#?\\s*([0-9,]+)\\s*\\)/i);
      if (mPair) {
        sales_rank_now = mPair[1].replace(/,/g, ""); sales_rank_before = mPair[2].replace(/,/g, "");
      } else {
        const mNow = t.match(/Sales\\s*rank:\\s*#?\\s*([0-9,]+)/i); if (mNow) sales_rank_now = mNow[1].replace(/,/g, "");
        const mWas = t.match(/\\(\\s*was\\s*#?\\s*([0-9,]+)\\s*\\)/i); if (mWas) sales_rank_before = mWas[1].replace(/,/g, "");
        if (!sales_rank_now || !sales_rank_before) {
          const mLoose = t.match(/#?\\s*([0-9,]+)\\s*\\(\\s*was\\s*#?\\s*([0-9,]+)\\s*\\)/i);
          if (mLoose) { if (!sales_rank_now) sales_rank_now = mLoose[1].replace(/,/g, ""); if (!sales_rank_before) sales_rank_before = mLoose[2].replace(/,/g, ""); }
        }
      }
    }

    // ---------- completeness ----------
    const required = { asin, title, product_url };
    const missing  = Object.entries(required).filter(([,v]) => !v).map(([k])=>k);

    out.push({
      category_name, asin, title, price, movers_rank,
      sales_rank_now, sales_rank_before, change,
      product_url, product_image, rating, reviews, _missing: missing
    });
  }
  return out;
}
"""





JS_SINGLE_NODE_EXTRACT = """
(node) => {
  const q  = sel => node.querySelector(sel);
  const qa = sel => Array.from(node.querySelectorAll(sel));
  const txt = sel => { const el = q(sel); return el ? el.textContent.trim() : null; };

  // movers_rank
  let movers_rank = null, cur = node;
  for (let i = 0; i < 6 && cur && !movers_rank; i++) {
    const badge = cur.querySelector("span.zg-bdg-text, .zg-bdg-text");
    if (badge && badge.textContent.trim()) { movers_rank = badge.textContent.trim().replace("#",""); break; }
    cur = cur.parentElement;
  }

  // title
  const title = txt("div[class*='_cDEzb_p13n-sc-css-line-clamp']") || txt("h3 a span") || null;

  // price
  let price = null;
  const priceSpans = qa("span[class*='p13n-sc-price']");
  if (priceSpans.length) price = priceSpans[0].textContent.trim();
  if (!price) { const block = q("span.a-size-base.a-color-price");
    if (block) { const m = block.textContent.match(/\\$[\\d.,]+/g); if (m && m.length) price = m[0]; } }

  // rating/reviews
  const ratingFull = txt("span.a-icon-alt");
  const rating = ratingFull ? ratingFull.split(" ")[0] : null;
  let reviews = txt(".a-size-small") || txt("a[href*='#customerReviews']");
  if (reviews && !/[0-9]/.test(reviews)) reviews = null;

  // link/asin/url
  const linkEl = q("a[href*='/dp/']"); const href = linkEl ? linkEl.getAttribute("href") : null;
  const asin = href ? (href.match(/\\/dp\\/([A-Z0-9]{10})/) || [])[1] : null;
  const product_url = href ? (href.startsWith("http") ? href : `https://www.amazon.com${href}`) : null;

  // image
  let product_image = q("img")?.getAttribute("src") || null;
  const dynAttr = q("img")?.getAttribute("data-a-dynamic-image") || null;
  if (dynAttr) { try { const obj = JSON.parse(dynAttr);
    const best = Object.keys(obj).sort((a,b)=>obj[b][0]-obj[a][0])[0]; if (best) product_image = best; } catch {} }

  // meta (ascend)
  let meta = null, cur2 = node;
  for (let i = 0; i < 6 && cur2 && !meta; i++) { meta = cur2.querySelector("span[class*='zg-grid-rank-metadata']"); cur2 = cur2.parentElement; }

  // category_name from H1
  let category_name = null;
  const h = document.querySelector("h1.a-size-large.a-spacing-medium.a-text-bold");
  if (h) {
    let t = h.textContent.trim();
    t = t.replace(/^Best\\s*Sellers\\s*in\\s*/i, '')
         .replace(/^Movers\\s*&\\s*Shakers\\s*in\\s*/i, '')
         .replace(/^Amazon\\s*Movers\\s*&\\s*Shakers\\s*:?\s*/i, '')
         .trim();
    if (t) category_name = t;
  }

  let change = null, sales_rank_now = null, sales_rank_before = null;
  if (meta) {
    const changeEl = meta.querySelector(".zg-grid-pct-change");
    if (changeEl) change = changeEl.textContent.trim();
    const t = meta.textContent.replace(/\\s+/g, " ").trim();
    let mPair = t.match(/Sales\\s*rank:\\s*#?\\s*([0-9,]+)\\s*\\(\\s*was\\s*#?\\s*([0-9,]+)\\s*\\)/i);
    if (mPair) {
      sales_rank_now = mPair[1].replace(/,/g, ""); sales_rank_before = mPair[2].replace(/,/g, "");
    } else {
      const mNow = t.match(/Sales\\s*rank:\\s*#?\\s*([0-9,]+)/i); if (mNow) sales_rank_now = mNow[1].replace(/,/g, "");
      const mWas = t.match(/\\(\\s*was\\s*#?\\s*([0-9,]+)\\s*\\)/i); if (mWas) sales_rank_before = mWas[1].replace(/,/g, "");
      if (!sales_rank_now || !sales_rank_before) {
        const mLoose = t.match(/#?\\s*([0-9,]+)\\s*\\(\\s*was\\s*#?\\s*([0-9,]+)\\s*\\)/i);
        if (mLoose) { if (!sales_rank_now) sales_rank_now = mLoose[1].replace(/,/g, ""); if (!sales_rank_before) sales_rank_before = mLoose[2].replace(/,/g, ""); }
      }
    }
  }

  const required = { asin, title, product_url };
  const missing  = Object.entries(required).filter(([,v]) => !v).map(([k])=>k);

  return { category_name, asin, title, price, movers_rank,
           sales_rank_now, sales_rank_before, change,
           product_url, product_image, rating, reviews, _missing: missing };
}
"""