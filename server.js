const express = require("express");
const cors    = require("cors");

const app  = express();
const PORT = process.env.PORT || 8080;

const AT_TOKEN = process.env.AT_TOKEN;
const AT_BASE  = process.env.AT_BASE;
const AT_TABLE = "Events";
const AT_URL   = `https://api.airtable.com/v0/${AT_BASE}/${AT_TABLE}`;
const AT_HEADS = { "Authorization": `Bearer ${AT_TOKEN}`, "Content-Type": "application/json" };
const RESEND_KEY   = process.env.RESEND_KEY;
const DIGEST_EMAIL = process.env.DIGEST_EMAIL || "bywilliamcole@gmail.com";
const ADMIN_URL    = "https://comnground.netlify.app/admin.html";

app.use(cors({ origin: "*" }));
app.use(express.json());

// ── DATE UTILS ───────────────────────────────────────────────────────────────
function today() { return new Date().toISOString().split("T")[0]; }
function threeMonthsFromNow() {
  const d = new Date();
  d.setMonth(d.getMonth() + 3);
  return d.toISOString().split("T")[0];
}
// Only pull events from today through 3 months out — keeps the list relevant
function inWindow(d) { return d && d >= today() && d <= threeMonthsFromNow(); }
function inFuture(d) { return d && d >= today(); } // kept for voting/council which may look further

// ── AIRTABLE HELPER ──────────────────────────────────────────────────────────
async function airtable(path, method = "GET", body = null) {
  const opts = { method, headers: AT_HEADS };
  if (body) opts.body = JSON.stringify(body);
  const res  = await fetch(`${AT_URL}${path}`, opts);
  const data = await res.json();
  if (!res.ok) throw new Error(JSON.stringify(data));
  return data;
}

// ── SCRAPER ──────────────────────────────────────────────────────────────────
async function fetchHTML(url) {
  try {
    const res = await fetch(url, {
      headers: { "User-Agent": "COMN-Civic-Bot/1.0 (comnground.netlify.app)" },
      signal: AbortSignal.timeout(12000),
    });
    if (!res.ok) return null;
    return await res.text();
  } catch(e) { console.warn(`Fetch failed: ${url} — ${e.message}`); return null; }
}

function parseDate(str) {
  if (!str) return null;
  try {
    const d = new Date(str);
    if (!isNaN(d)) return d.toISOString().split("T")[0];
  } catch(e) {}
  return null;
}

// Austin City Council meetings
async function scrapeCouncil() {
  const html = await fetchHTML("https://www.austintexas.gov/department/city-council/2026/2026_council_index.htm");
  if (!html) return [];
  const events = [];
  const seen = new Set();
  const re = /(January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},?\s*202[67]/gi;
  let m;
  while ((m = re.exec(html)) !== null) {
    const date = parseDate(m[0]);
    if (!date || !inWindow(date) || seen.has(date)) continue;
    seen.add(date);
    events.push({
      name: "Austin City Council Regular Meeting",
      type: "townhall", date, time: "10:00",
      address: "Austin City Hall, 301 W. Second Street, Austin, TX",
      lat: 30.2636, lng: -97.7466,
      desc: "Public comment open in-person or by phone. Register to speak at austintexas.gov.",
      source: "https://www.austintexas.gov/department/city-council/2026/2026_council_index.htm",
    });
  }
  return events;
}

// Travis County elections / voting
async function scrapeVoting() {
  const html = await fetchHTML("https://votetravis.gov/current-election-information/");
  if (!html) return [];
  const events = [];
  const seen = new Set();
  const re = /(January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},?\s*202[67]/gi;
  let m;
  while ((m = re.exec(html)) !== null) {
    const date = parseDate(m[0]);
    if (!date || !inWindow(date) || seen.has(date)) continue;
    seen.add(date);
    events.push({
      name: "Travis County Election / Voting Date",
      type: "voting", date, time: "07:00",
      address: "Any Travis County Vote Center",
      lat: 30.2672, lng: -97.7431,
      desc: "Vote at any Travis County Vote Center 7 AM – 7 PM. Bring valid TX photo ID.",
      source: "https://votetravis.gov/current-election-information/",
    });
  }
  return events;
}

// Mobilize.us public API — Austin civic events
async function scrapeMobilize() {
  try {
    const res = await fetch(
      "https://api.mobilize.us/v1/events?zipcode=78701&radius=30&timeslot_start=now&per_page=20&visibility=PUBLIC",
      { headers: { "User-Agent": "COMN-Civic-Bot/1.0" }, signal: AbortSignal.timeout(12000) }
    );
    if (!res.ok) return [];
    const data = await res.json();
    return (data.data || [])
      .filter(e => e.timeslots?.length > 0)
      .map(e => {
        const ts = e.timeslots[0];
        const date = ts?.start_date ? new Date(ts.start_date * 1000).toISOString().split("T")[0] : null;
        const time = ts?.start_date ? new Date(ts.start_date * 1000).toTimeString().slice(0,5) : "10:00";
        const isVirtual = e.is_virtual || e.location?.is_virtual;
        return {
          name: (e.title || "Austin Civic Event").slice(0, 100),
          type: isVirtual ? "online" : "nonprofit",
          date, time,
          address: isVirtual ? "Online" : (e.location?.venue || e.location?.address_lines?.[0] || "Austin, TX"),
          lat: isVirtual ? 0 : (parseFloat(e.location?.lat) || 30.2672),
          lng: isVirtual ? 0 : (parseFloat(e.location?.lon) || -97.7431),
          desc: (e.description || "").replace(/<[^>]*>/g,"").slice(0,200),
          source: e.browser_url || "",
        };
      })
      .filter(e => e.date && inWindow(e.date));
  } catch(e) { console.warn("Mobilize scrape failed:", e.message); return []; }
}

// Hands Off Central TX — uses Squarespace JSON API, then JSON-LD, then HTML fallback
async function scrapeHandsOff() {
  const BASE = "https://www.handsoffcentraltx.org";

  // ── METHOD 1: Squarespace JSON API (most reliable, returns structured data) ─
  try {
    const res = await fetch(`${BASE}/events?format=json`, {
      headers: { "User-Agent": "COMN-Civic-Bot/1.0 (comnground.netlify.app)" },
      signal: AbortSignal.timeout(12000),
    });
    if (res.ok) {
      const data = await res.json();
      const items = data.items || data.upcomingEvents || [];
      const events = [];
      for (const item of items) {
        // Squarespace stores dates as milliseconds or ISO string
        const startMs = item.startDate;
        if (!startMs) continue;
        const d = new Date(typeof startMs === "number" ? startMs : startMs);
        const date = d.toISOString().split("T")[0];
        if (!inWindow(date)) continue;
        const time = d.toTimeString().slice(0, 5);
        const loc = item.location?.addressLine1
          || item.location?.mapWidget?.mapAddress
          || item.location?.name
          || "Austin, TX (see source for location)";
        // Strip HTML tags from description/excerpt
        const rawDesc = (item.excerpt || item.body || item.description || "").replace(/<[^>]*>/g,"").trim();
        const desc = rawDesc.slice(0, 250) || "Community civic event from Hands Off Central TX. See source link for full details.";
        events.push({
          name: (item.title || "Hands Off Central TX Event").slice(0, 100),
          type: "protest", date, time: time || "13:00",
          address: loc,
          lat: parseFloat(item.location?.markerLat) || 30.2747,
          lng: parseFloat(item.location?.markerLng) || -97.7403,
          desc,
          source: item.fullUrl ? `${BASE}${item.fullUrl}` : `${BASE}/events`,
        });
      }
      if (events.length > 0) {
        console.log(`HandsOff (Squarespace API): ${events.length} events`);
        return events;
      }
    }
  } catch(e) { console.warn("HandsOff Squarespace API failed:", e.message); }

  // ── METHOD 2: JSON-LD structured data embedded in the HTML page ─────────────
  const html = await fetchHTML(`${BASE}/events`);
  if (!html) return [];
  const events = [];
  const jsonBlocks = [];
  const jsonRe = /<script type="application\/ld\+json">([\s\S]*?)<\/script>/gi;
  let jm;
  while ((jm = jsonRe.exec(html)) !== null) {
    try {
      const parsed = JSON.parse(jm[1]);
      const items = Array.isArray(parsed) ? parsed : [parsed];
      for (const item of items) {
        if (item["@type"] === "Event" || item.startDate) jsonBlocks.push(item);
      }
    } catch(e) {}
  }
  if (jsonBlocks.length > 0) {
    for (const ev of jsonBlocks) {
      const date = parseDate(ev.startDate);
      if (!date || !inWindow(date)) continue;
      const timeStr = ev.startDate?.includes("T") ? ev.startDate.split("T")[1]?.slice(0,5) : "13:00";
      const loc = ev.location?.name || ev.location?.address?.streetAddress || "Austin, TX (see source for location)";
      const rawDesc = (ev.description || "").replace(/<[^>]*>/g,"").trim();
      events.push({
        name: (ev.name || "Hands Off Central TX Event").slice(0, 100),
        type: "protest", date, time: timeStr || "13:00",
        address: loc,
        lat: parseFloat(ev.location?.geo?.latitude) || 30.2747,
        lng: parseFloat(ev.location?.geo?.longitude) || -97.7403,
        desc: rawDesc.slice(0, 250) || "Community civic event from Hands Off Central TX. See source link for full details.",
        source: ev.url || `${BASE}/events`,
      });
    }
    if (events.length > 0) {
      console.log(`HandsOff (JSON-LD): ${events.length} events`);
      return events;
    }
  }

  // ── METHOD 3: HTML fallback — parses event links, skips ALL navigation items ─
  // Known UI strings that are NOT event names (mobile nav buttons, menu toggles, etc.)
  const UI_STRINGS = new Set([
    "open menu", "close menu", "open", "close", "menu", "toggle menu",
    "navigation", "nav", "skip to content", "back to top",
  ]);
  const NAV_SLUGS = new Set([
    "events","about","merch","civics","alfr","mutualaid","bookclub",
    "book-club","donate","contact","home","open-menu","close-menu",
    "openmenu","closemenu","toggle","skip","back",
  ]);
  const linkRe = /href="(\/events\/([a-z0-9][a-z0-9\-]+))"[^>]*>([\s\S]{0,200}?)<\/a>/gi;
  const dateRe = /(January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},?\s*202[67]/gi;
  const seen = new Set();
  const dates = [];
  let dm;
  while ((dm = dateRe.exec(html)) !== null) {
    const date = parseDate(dm[0]);
    if (date && inWindow(date)) dates.push(date);
  }
  let lm, dateIdx = 0;
  while ((lm = linkRe.exec(html)) !== null && dateIdx < dates.length) {
    const slug = lm[2].toLowerCase();
    if (NAV_SLUGS.has(slug) || seen.has(slug)) continue;
    seen.add(slug);
    const rawName = lm[3].replace(/<[^>]*>/g,"").trim();
    if (!rawName || rawName.length < 4) continue;
    // Skip any UI navigation strings masquerading as event names
    if (UI_STRINGS.has(rawName.toLowerCase())) continue;
    events.push({
      name: rawName.replace(/&amp;/g,"&").replace(/&#\d+;/g,"").slice(0,100),
      type: "protest", date: dates[dateIdx++], time: "13:00",
      address: "Austin, TX (see source for venue details)",
      lat: 30.2747, lng: -97.7403,
      desc: "Community civic event from Hands Off Central TX. Visit the source link for venue, time, and full details before approving.",
      source: `${BASE}${lm[1]}`,
    });
  }
  console.log(`HandsOff (HTML fallback): ${events.length} events`);
  return events;
}


// Austin Justice Coalition
async function scrapeAJC() {
  const html = await fetchHTML("https://austinjustice.org/events/");
  if (!html) return [];
  const events = [];
  const dateRe = /(January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},?\s*202[67]/gi;
  const nameRe = /class="[^"]*tribe-event[^"]*"[^>]*>[\s\S]{0,100}?<a[^>]*>([^<]{5,80})<\/a/gi;
  const names = [], dates = [];
  let m;
  while ((m = nameRe.exec(html)) !== null) names.push(m[1].trim());
  while ((m = dateRe.exec(html)) !== null) {
    const date = parseDate(m[0]);
    if (date && inWindow(date)) dates.push(date);
  }
  for (let i = 0; i < Math.min(names.length, dates.length, 8); i++) {
    if (!names[i] || names[i].length < 4) continue;
    events.push({
      name: names[i].replace(/&amp;/g,"&"),
      type: "nonprofit", date: dates[i], time: "18:00",
      address: "Austin, TX (see source for location)",
      lat: 30.2620, lng: -97.7213,
      desc: "Event from Austin Justice Coalition. Verify details at source.",
      source: "https://austinjustice.org/events/",
    });
  }
  return events;
}

// League of Women Voters Austin
async function scrapeLWV() {
  const html = await fetchHTML("https://lwvaustin.org/events/");
  if (!html) return [];
  const events = [];
  const dateRe = /(January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},?\s*202[67]/gi;
  const seen = new Set();
  let m;
  while ((m = dateRe.exec(html)) !== null) {
    const date = parseDate(m[0]);
    if (!date || !inWindow(date) || seen.has(date)) continue;
    seen.add(date);
    events.push({
      name: "League of Women Voters Austin — Event",
      type: "nonprofit", date, time: "10:00",
      address: "Austin, TX (see source for location)",
      lat: 30.2676, lng: -97.7521,
      desc: "Civic event from LWV Austin. May include voter registration, candidate forums, or community education.",
      source: "https://lwvaustin.org/events/",
    });
  }
  return events.slice(0, 5);
}

// ── DEDUPLICATION ────────────────────────────────────────────────────────────
async function getExistingKeys() {
  try {
    const filter = encodeURIComponent(`NOT({Status}='rejected')`);
    const data = await airtable(`?filterByFormula=${filter}&fields[]=Name&fields[]=Date&fields[]=Source`);
    return new Set(
      (data.records || []).map(r =>
        `${(r.fields.Name||"").toLowerCase().trim()}__${r.fields.Date||""}`
      )
    );
  } catch(e) { console.warn("Dedup fetch failed:", e.message); return new Set(); }
}

// ── SAVE BATCH TO AIRTABLE ───────────────────────────────────────────────────
// ⚠️  HUMAN APPROVAL REQUIRED — SAFETY RULE ⚠️
// Events saved here are ALWAYS "pending". They will NEVER go live automatically.
// The ONLY way an event becomes "live" is when a human admin clicks Approve
// in the admin panel at comnground.netlify.app/admin.html
// DO NOT change Status to anything other than "pending" here. Ever.
async function saveEvents(events) {
  const saved = [];
  for (let i = 0; i < events.length; i += 10) {
    const batch = events.slice(i, i + 10);
    try {
      const data = await airtable("", "POST", {
        records: batch.map(e => {
          // SAFETY GUARD: Explicitly enforce pending status — auto-approval is forbidden
          const status = "pending"; // This must never be changed to "live"
          return { fields: {
            Name:        e.name,
            Type:        e.type,
            Date:        e.date,
            Time:        e.time        || "",
            Address:     e.address     || "",
            Latitude:    e.lat         || 30.2672,
            Longitude:   e.lng         || -97.7431,
            Description: e.desc        || "",
            Source:      e.source      || "",
            Status:      status,        // Always "pending" — human must approve
            SubmittedBy: "COMN Auto-Scraper",
          }};
        })
      });
      saved.push(...(data.records || []));
    } catch(e) { console.error("Save batch error:", e.message); }
  }
  return saved;
}

// ── EMAIL DIGEST ─────────────────────────────────────────────────────────────
async function sendDigest(newCount, totalPending) {
  if (!RESEND_KEY) { console.log("No RESEND_KEY — skipping email"); return; }
  const subject = newCount > 0
    ? `COMN Daily Digest — ${newCount} new event${newCount !== 1 ? "s" : ""} pending review`
    : "COMN Daily Digest — No new events found today";
  const text = `${newCount > 0
    ? `${newCount} new civic event${newCount !== 1 ? "s were" : " was"} found and added to your review queue.`
    : "No new events were found today across your sources."
  }\n\nTotal pending review: ${totalPending}\n\nReview and publish at:\n${ADMIN_URL}\n\n— COMN`;
  try {
    const res = await fetch("https://api.resend.com/emails", {
      method: "POST",
      headers: { "Authorization": `Bearer ${RESEND_KEY}`, "Content-Type": "application/json" },
      body: JSON.stringify({ from: "COMN Digest <onboarding@resend.dev>", to: DIGEST_EMAIL, subject, text }),
    });
    const data = await res.json();
    if (!res.ok) throw new Error(JSON.stringify(data));
    console.log(`✉️  Digest sent: "${subject}"`);
  } catch(e) { console.error("Email failed:", e.message); }
}

// ── MAIN SCRAPE FUNCTION ─────────────────────────────────────────────────────
async function runScraper() {
  console.log(`[${new Date().toISOString()}] Starting scrape...`);
  try {
    const [council, voting, mobilize, handsOff, ajc, lwv] = await Promise.all([
      scrapeCouncil(), scrapeVoting(), scrapeMobilize(),
      scrapeHandsOff(), scrapeAJC(), scrapeLWV(),
    ]);
    const allFound = [...council, ...voting, ...mobilize, ...handsOff, ...ajc, ...lwv];
    console.log(`Found ${allFound.length} candidate events`);

    const existingKeys = await getExistingKeys();
    const newEvents = allFound.filter(e => {
      if (!e.date || !e.name) return false;
      const key = `${e.name.toLowerCase().trim()}__${e.date}`;
      return !existingKeys.has(key);
    });
    console.log(`${newEvents.length} new after dedup`);

    const saved = newEvents.length > 0 ? await saveEvents(newEvents) : [];
    console.log(`Saved ${saved.length} to Airtable`);

    // Get total pending count for digest
    let totalPending = 0;
    try {
      const filter = encodeURIComponent(`{Status}='pending'`);
      const d = await airtable(`?filterByFormula=${filter}&fields[]=Name`);
      totalPending = (d.records || []).length;
    } catch(e) {}

    await sendDigest(saved.length, totalPending);
    console.log(`[${new Date().toISOString()}] Scrape complete.`);
    return { found: allFound.length, new: saved.length, totalPending };
  } catch(e) {
    console.error("Scraper error:", e.message);
    return { error: e.message };
  }
}

// ── CRON: 12 PM CT = 18:00 UTC ───────────────────────────────────────────────
// Railway supports cron via the RAILWAY_CRON_SCHEDULE env var, but we use
// a simple setInterval check here for reliability without extra dependencies
let lastScrapeDate = "";
setInterval(() => {
  const now = new Date();
  // Convert to CT (UTC-5 standard, UTC-6 daylight — use UTC-5 to be safe)
  const ctHour = (now.getUTCHours() - 5 + 24) % 24;
  const ctDate = now.toISOString().split("T")[0];
  if (ctHour === 12 && ctDate !== lastScrapeDate) {
    lastScrapeDate = ctDate;
    console.log("⏰ 12 PM CT — running daily scrape");
    runScraper();
  }
}, 60 * 1000); // check every minute

// ── ROUTES ───────────────────────────────────────────────────────────────────

app.get("/", (req, res) => res.json({ status: "COMN server running", nextScrape: "Daily 12 PM CT" }));

app.get("/events", async (req, res) => {
  try {
    const filter = encodeURIComponent(`AND({Status}='live',{Date}>='${today()}')`);
    const data = await airtable(`?filterByFormula=${filter}&sort[0][field]=Date&sort[0][direction]=asc`);
    const events = (data.records || []).map(r => ({
      id:      r.id,
      name:    r.fields.Name        || "",
      type:    (r.fields.Type       || "nonprofit").toLowerCase(),
      date:    r.fields.Date        || "",
      time:    r.fields.Time        || "",
      address: r.fields.Address     || "",
      lat:     parseFloat(r.fields.Latitude)  || 30.2672,
      lng:     parseFloat(r.fields.Longitude) || -97.7431,
      desc:    r.fields.Description || "",
      source:  r.fields.Source      || "",
    }));
    res.json({ events });
  } catch(e) {
    console.error("GET /events:", e.message);
    res.status(500).json({ error: "Failed to fetch events", detail: e.message });
  }
});

app.get("/pending-count", async (req, res) => {
  try {
    const filter = encodeURIComponent(`{Status}='pending'`);
    const data = await airtable(`?filterByFormula=${filter}&fields[]=Name`);
    res.json({ count: (data.records || []).length });
  } catch(e) { res.status(500).json({ count: 0 }); }
});

app.get("/admin/events", async (req, res) => {
  try {
    const status = req.query.status || "pending";
    const filter = encodeURIComponent(`{Status}='${status}'`);
    const data = await airtable(`?filterByFormula=${filter}&sort[0][field]=Date&sort[0][direction]=asc`);
    res.json({ records: data.records || [] });
  } catch(e) {
    console.error("GET /admin/events:", e.message);
    res.status(500).json({ error: "Failed to fetch admin events", detail: e.message });
  }
});

app.post("/submit", async (req, res) => {
  try {
    const f = req.body;
    if (!f.name || !f.type || !f.date || !f.address)
      return res.status(400).json({ error: "Missing required fields" });
    const data = await airtable("", "POST", {
      records: [{ fields: {
        Name: f.name, Type: f.type, Date: f.date, Time: f.time || "",
        Address: f.address,
        Latitude:    parseFloat(f.lat)  || 30.2672,
        Longitude:   parseFloat(f.lng)  || -97.7431,
        Description: f.desc   || "", Source: f.source || "",
        Status: "pending", // Always pending — human must approve before going live
        SubmittedBy: f.submittedBy || "Public",
      }}]
    });
    res.json({ success: true, id: data.records?.[0]?.id });
  } catch(e) {
    console.error("POST /submit:", e.message);
    res.status(500).json({ error: "Failed to submit event" });
  }
});

// ⚠️  This endpoint is the ONLY way an event becomes "live".
// It requires a human to manually click Approve in the admin panel.
// There is no automated path to "live" — all auto-scraper code uses "pending" only.
app.patch("/admin/events/:id", async (req, res) => {
  try {
    const { status } = req.body;
    if (!["live","rejected"].includes(status))
      return res.status(400).json({ error: "Invalid status" });
    // Log every approval so there's a clear audit trail
    console.log(`[HUMAN ACTION] Event ${req.params.id} set to "${status}" via admin panel`);
    await airtable(`/${req.params.id}`, "PATCH", { fields: { Status: status } });
    res.json({ success: true });
  } catch(e) {
    console.error("PATCH:", e.message);
    res.status(500).json({ error: "Failed to update event" });
  }
});

// Manual trigger — runs scrape immediately and returns result
app.post("/admin/scrape-now", async (req, res) => {
  res.json({ message: "Scrape started — check your email in ~60 seconds" });
  runScraper();
});

app.listen(PORT, () => console.log(`COMN server on port ${PORT}`));
