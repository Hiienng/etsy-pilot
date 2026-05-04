() => {
    const clean = s => s ? s.replace(/\s+/g, ' ').trim() : '';

    // VND uses '.' as thousand separator ("378.788₫"), USD uses ',' ("$18.50").
    const parsePrice = (text) => {
        if (!text) return null;
        const t = text.trim();
        const dots   = (t.match(/\./g)  || []).length;
        const commas = (t.match(/,/g) || []).length;
        let normalized;
        if (dots > 1) {
            normalized = t.replace(/\./g, '').replace(/,/g, '.');
        } else if (commas > 1) {
            normalized = t.replace(/,/g, '');
        } else if (dots === 1 && commas === 1) {
            normalized = t.lastIndexOf('.') > t.lastIndexOf(',')
                ? t.replace(/,/g, '')
                : t.replace(/\./g, '').replace(',', '.');
        } else {
            normalized = t.replace(/[^\d.,]/g, '');
            if (/\.\d{3}$/.test(normalized) && dots === 1 && commas === 0)
                normalized = normalized.replace('.', '');
        }
        const v = parseFloat(normalized.replace(/[^\d.]/g, ''));
        return isFinite(v) && v > 0 ? v : null;
    };

    // ── Price ──────────────────────────────────────────────────────────────
    let sale_price = null, base_price = null, discount_percent = null;
    const origEl = document.querySelector(
        's [class*="currency-value"], del [class*="currency-value"],' +
        '[class*="original-price"] [class*="currency-value"],' +
        '[class*="regularPrice"] [class*="currency-value"]'
    );
    const saleEl = document.querySelector(
        '[class*="sale-price"] [class*="currency-value"],' +
        'p [class*="currency-value"]:first-of-type'
    );
    if (origEl && saleEl) {
        base_price = parsePrice(origEl.textContent);
        sale_price = parsePrice(saleEl.textContent);
    } else {
        const priceEls = [...document.querySelectorAll('[class*="currency-value"]')];
        const prices = priceEls.map(e => parsePrice(e.textContent)).filter(Boolean);
        if (prices.length === 1) { sale_price = prices[0]; }
        else if (prices.length >= 2) {
            sale_price = Math.min(...prices);
            base_price = Math.max(...prices);
            if (sale_price === base_price) base_price = null;
        }
    }
    const discEl = document.querySelector('[class*="percent-off"],[class*="sale-percent"],[class*="discount-label"]');
    if (discEl) { const m = discEl.textContent.match(/(\d+)\s*%/); if (m) discount_percent = parseInt(m[1]); }
    if (!discount_percent && base_price && sale_price && base_price > sale_price)
        discount_percent = Math.round((1 - sale_price / base_price) * 100);

    // ── Listing details ────────────────────────────────────────────────────
    const allText = document.body ? document.body.innerText : '';
    const matM    = allText.match(/Materials?:([^\n]{3,120})/i);
    const materials = matM ? clean(matM[1]) : null;

    const hlEls = [...document.querySelectorAll('[class*="highlight"],[class*="item-highlight"]')];
    const highlights = hlEls.map(e => clean(e.textContent)).filter(Boolean).join(' | ') || null;

    const shipM = allText.match(/Ships out(?:\s+in)?\s+([^\n.]{3,60})/i);
    const shipping_status = shipM ? clean(shipM[1]) : null;
    const fromM2 = allText.match(/Ships from:\s*([^\n]{2,60})/i);
    const origin_ship_from = fromM2 ? clean(fromM2[1]) : null;
    const daysM = allText.match(/(\d+)[-–]?(\d*)\s*business days?/i);
    const ship_time_max_days = daysM ? parseInt(daysM[2] || daysM[1]) : null;
    const us_shipping   = /United States/i.test(allText) && !/doesn't ship to/i.test(allText);
    const return_policy = /Returns? & exchanges? accepted/i.test(allText);

    const descEl = document.querySelector('[data-listing-description],[class*="description"]');
    const design = descEl ? clean(descEl.textContent).slice(0, 500) : null;

    const sumEl = document.querySelector('[class*="buyer-highlight"],[class*="ai-summary"],[class*="review-summary"]');
    const ai_summary = sumEl ? clean(sumEl.textContent) : null;

    // ── Reviews ────────────────────────────────────────────────────────────
    const reviewEls = [...document.querySelectorAll('[class*="review-card"],[data-review-id],[class*="ReviewCard"]')];
    const reviews = reviewEls.slice(0, 10).map(el => {
        const starsEl = el.querySelector('[class*="stars"],[aria-label*="star"],[class*="rating"]');
        let stars = null;
        if (starsEl) {
            const aria = starsEl.getAttribute('aria-label') || '';
            const m = aria.match(/(\d)\s*star/) || starsEl.textContent.match(/(\d)/);
            if (m) stars = parseInt(m[1]);
        }
        const dateEl = el.querySelector('time,[class*="date"],[class*="Date"]');
        const review_date = dateEl ? (dateEl.getAttribute('datetime') || dateEl.textContent.trim()) : null;
        const revEl = el.querySelector('[class*="buyer-name"],[class*="username"],[class*="reviewer"]');
        const reviewer = revEl ? clean(revEl.textContent) : null;
        const contEl = el.querySelector('[class*="review-text"],[class*="body"],[class*="content"] p,[class*="ReviewBody"]');
        const content = contEl ? clean(contEl.textContent) : clean(el.textContent).slice(0, 300);
        return { reviewer, review_date, stars, content };
    }).filter(r => r.content);

    // ── Shop info ──────────────────────────────────────────────────────────
    // Pattern in body.innerText:
    // "OwnerName\nShopName\n·\n\nLocation\n\n4.9\n(8.3k)\n·\n65.9k sales\n·\n2.5 years on Etsy"
    const salesIdx = allText.search(/\d[\d.,]*k?\s+sales/i);
    const block    = salesIdx >= 0 ? allText.slice(Math.max(0, salesIdx - 200), salesIdx + 200) : '';

    const fromShopM  = allText.match(/From\s+shop\s+(\S+)/i);
    const shopLineM  = block.match(/([A-Za-z]\S+)\n([A-Za-z]\S+)\s*\n\s*[·\n]/);
    const salesM2    = block.match(/([\d.,]+k?)\s+sales?/i);
    const yearsM     = block.match(/(\d+(?:\.\d+)?)\s+years?\s+on\s+Etsy/i);
    const monthsM    = block.match(/(\d+)\s+months?\s+on\s+Etsy/i);
    const ratingM2   = block.match(/([\d.]+)\s*\n\s*\(([\d.,k]+)\)/);
    const locM       = block.match(/\n\s*·\s*\n\s*([A-Za-z][^·\d\n]{1,50})\n[\s\n]*[\d.]+\s*\n/)
                    || block.match(/\n([A-Za-z][^\n]{1,40})\n\s*[\d.]+\s*\n\s*\([\d.,k]+\)/);

    let join_year = null;
    if (yearsM)       join_year = new Date().getFullYear() - Math.floor(parseFloat(yearsM[1]));
    else if (monthsM) join_year = new Date().getFullYear();

    let total_sales = null;
    if (salesM2) {
        const s = salesM2[1].replace(/,/g, '');
        total_sales = s.toLowerCase().endsWith('k') ? Math.round(parseFloat(s) * 1000) : parseInt(s);
    }

    return {
        sale_price, base_price, discount_percent,
        materials, highlights, shipping_status, origin_ship_from,
        ship_time_max_days, us_shipping, return_policy, design, ai_summary,
        reviews,
        shop: {
            page_shop_name: fromShopM ? fromShopM[1] : (shopLineM ? shopLineM[2] : null),
            owner_name:     shopLineM ? shopLineM[1] : null,
            join_year,
            total_sales,
            shop_rating:    ratingM2 ? parseFloat(ratingM2[1]) : null,
            location:       locM ? clean(locM[1]) : null,
            smooth_shipping: /smooth\s+shipping/i.test(block),
            speedy_replies:  /speedy\s+repl/i.test(block),
            badge:           /star\s+seller/i.test(block) ? 'Star Seller' : null,
        },
    };
}
