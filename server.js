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
function inFuture(d) { return d && d >= today(); }

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
    if (!date || !inFuture(date) || seen.has(date)) continue;
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
    if (!date || !inFuture(date) || seen.has(date)) continue;
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
      .filter(e => e.date && inFuture(e.date));
  } catch(e) { console.warn("Mobilize scrape failed:", e.message); return []; }
}

// Hands Off Central TX
async function scrapeHandsOff() {
  const html = await fetchHTML("https://www.handsoffcentraltx.org/events");
  if (!html) return [];
  const events = [];
  const dateRe = /(January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},?\s*202[67]/gi;
  const nameRe = /(?:<h[23][^>]*>|class="[^"]*title[^"]*"[^>]*>)([^<]{5,80})<\//gi;
  const names = [], dates = [];
  let m;
  while ((m = nameRe.exec(html)) !== null) names.push(m[1].trim());
  while ((m = dateRe.exec(html)) !== null) {
    const date = parseDate(m[0]);
    if (date && inFuture(date)) dates.push(date);
  }
  for (let i = 0; i < Math.min(names.length, dates.length, 8); i++) {
    if (!names[i] || names[i].length < 4) continue;
    events.push({
      name: names[i].replace(/&amp;/g,"&").replace(/&#\d+;/g,""),
      type: "protest", date: dates[i], time: "10:00",
      address: "Texas State Capitol, 1100 Congress Ave, Austin, TX",
      lat: 30.2747, lng: -97.7403,
      desc: "Event from Hands Off Central TX. Verify details at source.",
      source: "https://www.handsoffcentraltx.org/events",
    });
  }
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
    if (date && inFuture(date)) dates.push(date);
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
    if (!date || !inFuture(date) || seen.has(date)) continue;
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
async function saveEvents(events) {
  const saved = [];
  for (let i = 0; i < events.length; i += 10) {
    const batch = events.slice(i, i + 10);
    try {
      const data = await airtable("", "POST", {
        records: batch.map(e => ({ fields: {
          Name:        e.name,
          Type:        e.type,
          Date:        e.date,
          Time:        e.time        || "",
          Address:     e.address     || "",
          Latitude:    e.lat         || 30.2672,
          Longitude:   e.lng         || -97.7431,
          Description: e.desc        || "",
          Source:      e.source      || "",
          Status:      "pending",
          SubmittedBy: "COMN Auto-Scraper",
        }}))
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
        Status: "pending", SubmittedBy: f.submittedBy || "Public",
      }}]
    });
    res.json({ success: true, id: data.records?.[0]?.id });
  } catch(e) {
    console.error("POST /submit:", e.message);
    res.status(500).json({ error: "Failed to submit event" });
  }
});

app.patch("/admin/events/:id", async (req, res) => {
  try {
    const { status } = req.body;
    if (!["live","rejected"].includes(status))
      return res.status(400).json({ error: "Invalid status" });
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
