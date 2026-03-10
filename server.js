const express = require("express");
const cors    = require("cors");
const bcrypt  = require("bcryptjs");
const jwt     = require("jsonwebtoken");

const app  = express();
const PORT = process.env.PORT || 8080;

const AT_TOKEN = process.env.AT_TOKEN;
const AT_BASE  = process.env.AT_BASE;
const AT_TABLE        = "Events";
const AT_ADMINS_TABLE = "Admins";
const AT_URL        = `https://api.airtable.com/v0/${AT_BASE}/${AT_TABLE}`;
const AT_ADMINS_URL = `https://api.airtable.com/v0/${AT_BASE}/${AT_ADMINS_TABLE}`;
const AT_HEADS = { "Authorization": `Bearer ${AT_TOKEN}`, "Content-Type": "application/json" };

const RESEND_KEY   = process.env.RESEND_KEY;
const DIGEST_EMAIL = process.env.DIGEST_EMAIL || "bywilliamcole@gmail.com";
const ADMIN_URL    = "https://comnground.netlify.app/admin.html";
const JWT_SECRET   = process.env.JWT_SECRET || "comn-dev-jwt-secret-CHANGE-IN-PROD";

function generateToken() {
  return Math.random().toString(36).slice(2) + Math.random().toString(36).slice(2) + Date.now().toString(36);
}

async function getInviteToken() {
  const filter = encodeURIComponent(`{Role}="invite_token"`);
  const data = await airtableAdmins(`?filterByFormula=${filter}&maxRecords=1`);
  if (data.records && data.records.length > 0) return data.records[0].fields.DisplayName || null;
  const token = generateToken();
  await airtableAdmins("", "POST", { records: [{ fields: { Email: "_invite_token", DisplayName: token, Role: "invite_token" } }] });
  return token;
}

app.use(cors({ origin: "*" }));
app.use(express.json());

// ── DATE UTILS ───────────────────────────────────────────────────────────────
function today() { return new Date().toISOString().split("T")[0]; }
function threeMonthsFromNow() {
  const d = new Date();
  d.setMonth(d.getMonth() + 3);
  return d.toISOString().split("T")[0];
}
function inWindow(d) { return d && d >= today() && d <= threeMonthsFromNow(); }
function inFuture(d) { return d && d >= today(); }

// ── AIRTABLE HELPERS ──────────────────────────────────────────────────────────
// NEW optional fields that may not exist in Airtable yet — auto-stripped on error
const OPTIONAL_FIELDS = ["DuplicateOf", "RejectedBy", "Approvers"];

async function airtable(path, method = "GET", body = null, _retry = true) {
  const opts = { method, headers: AT_HEADS };
  if (body) opts.body = JSON.stringify(body);
  const res  = await fetch(`${AT_URL}${path}`, opts);
  const data = await res.json();
  if (!res.ok) {
    // If a new optional field doesn't exist in Airtable yet, retry without it
    if (_retry && data?.error?.type === "UNKNOWN_FIELD_NAME" && body) {
      console.warn(`[Airtable] Field not found — retrying without optional fields`);
      const stripped = JSON.parse(JSON.stringify(body));
      const strip = f => { if (f) OPTIONAL_FIELDS.forEach(k => delete f[k]); };
      if (stripped.records) stripped.records.forEach(r => strip(r.fields));
      else if (stripped.fields) strip(stripped.fields);
      return await airtable(path, method, stripped, false);
    }
    throw new Error(JSON.stringify(data));
  }
  return data;
}

async function airtableAdmins(path, method = "GET", body = null) {
  const opts = { method, headers: AT_HEADS };
  if (body) opts.body = JSON.stringify(body);
  const res  = await fetch(`${AT_ADMINS_URL}${path}`, opts);
  const data = await res.json();
  if (!res.ok) throw new Error(JSON.stringify(data));
  return data;
}

// ── AUTH MIDDLEWARE ───────────────────────────────────────────────────────────
function requireAuth(req, res, next) {
  const token = (req.headers.authorization || "").replace("Bearer ", "").trim();
  if (!token) return res.status(401).json({ error: "Login required" });
  try {
    req.admin = jwt.verify(token, JWT_SECRET);
    next();
  } catch(e) {
    res.status(401).json({ error: "Session expired — please log in again" });
  }
}

// ── SCRAPER HELPERS ───────────────────────────────────────────────────────────
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

// ── SCRAPERS ──────────────────────────────────────────────────────────────────

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
      desc: "Regular public meeting of the Austin City Council. Public comment is open — register to speak in-person or by phone at austintexas.gov before the meeting.",
      source: "https://www.austintexas.gov/department/city-council/2026/2026_council_index.htm",
    });
  }
  return events;
}

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
      desc: "Vote at any Travis County Vote Center 7 AM – 7 PM. Bring a valid Texas photo ID. Find your nearest location at votetravis.gov.",
      source: "https://votetravis.gov/current-election-information/",
    });
  }
  return events;
}

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
          desc: (e.description || "").replace(/<[^>]*>/g,"").slice(0,250),
          source: e.browser_url || "",
        };
      })
      .filter(e => e.date && inWindow(e.date));
  } catch(e) { console.warn("Mobilize scrape failed:", e.message); return []; }
}

async function scrapeHandsOff() {
  const BASE = "https://www.handsoffcentraltx.org";

  // METHOD 1: Squarespace JSON API
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
        const rawDesc = (item.excerpt || item.body || item.description || "").replace(/<[^>]*>/g,"").trim();
        events.push({
          name: (item.title || "Hands Off Central TX Event").slice(0, 100),
          type: "protest", date, time: time || "13:00",
          address: loc,
          lat: parseFloat(item.location?.markerLat) || 30.2747,
          lng: parseFloat(item.location?.markerLng) || -97.7403,
          desc: rawDesc.slice(0, 250) || "Community civic event from Hands Off Central TX. See source link for full details.",
          source: item.fullUrl ? `${BASE}${item.fullUrl}` : `${BASE}/events`,
        });
      }
      if (events.length > 0) {
        console.log(`HandsOff (Squarespace API): ${events.length} events`);
        return events;
      }
    }
  } catch(e) { console.warn("HandsOff Squarespace API failed:", e.message); }

  // METHOD 2: JSON-LD structured data
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
        desc: rawDesc.slice(0, 250) || "Community civic event. See source for full details.",
        source: ev.url || `${BASE}/events`,
      });
    }
    if (events.length > 0) {
      console.log(`HandsOff (JSON-LD): ${events.length} events`);
      return events;
    }
  }

  // METHOD 3: HTML fallback — skips ALL navigation/UI items
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
      desc: "Event from Austin Justice Coalition. Verify exact location and time at the source link before approving.",
      source: "https://austinjustice.org/events/",
    });
  }
  return events;
}

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
      desc: "Civic event from LWV Austin. May include voter registration drives, candidate forums, or civic education sessions. Check the source for full details.",
      source: "https://lwvaustin.org/events/",
    });
  }
  return events.slice(0, 5);
}

// ── DEDUPLICATION ─────────────────────────────────────────────────────────────
function normalizeForDedup(name) {
  return (name || "").toLowerCase().replace(/[^a-z0-9 ]/g, "").replace(/\s+/g, " ").trim();
}

function isSimilarEvent(name1, date1, name2, date2) {
  if (!date1 || !date2 || date1 !== date2) return false;
  const n1 = normalizeForDedup(name1);
  const n2 = normalizeForDedup(name2);
  if (!n1 || !n2) return false;
  if (n1 === n2) return true;
  if (n1.includes(n2) || n2.includes(n1)) return true;
  // Word-overlap: 2+ significant words in common = likely same event
  const w1 = new Set(n1.split(" ").filter(w => w.length > 3));
  const w2 = new Set(n2.split(" ").filter(w => w.length > 3));
  let overlap = 0;
  for (const w of w1) if (w2.has(w)) overlap++;
  return overlap >= 2 && overlap >= Math.min(w1.size, w2.size) * 0.6;
}

async function getExistingEventsForDedup() {
  try {
    const filter = encodeURIComponent(`NOT(OR({Status}='rejected',{Status}='duplicate'))`);
    const data = await airtable(`?filterByFormula=${filter}&fields[]=Name&fields[]=Date`);
    return (data.records || []).map(r => ({
      id:   r.id,
      name: r.fields.Name || "",
      date: r.fields.Date || "",
    }));
  } catch(e) { console.warn("Dedup fetch failed:", e.message); return []; }
}

// ── SAVE EVENTS ───────────────────────────────────────────────────────────────
// ⚠️  HUMAN APPROVAL REQUIRED — SAFETY RULE ⚠️
// Events are saved as "pending" or "duplicate". NEVER "live" automatically.
// The ONLY path to "live" is a human admin approving via the admin panel.
async function saveEvents(events, existingEventsForDedup) {
  const saved = [];
  for (let i = 0; i < events.length; i += 10) {
    const batch = events.slice(i, i + 10);
    try {
      const data = await airtable("", "POST", {
        records: batch.map(e => {
          let status = "pending"; // Default — NEVER change to "live"
          let dupOf  = "";
          if (existingEventsForDedup) {
            const match = existingEventsForDedup.find(ex =>
              isSimilarEvent(ex.name, ex.date, e.name, e.date)
            );
            if (match) {
              status = "duplicate";
              dupOf  = match.id;
              console.log(`  ⚠️ Near-duplicate: "${e.name}" (${e.date}) ~ "${match.name}"`);
            }
          }
          return { fields: {
            Name:        e.name,
            Type:        e.type,
            Date:        e.date,
            Time:        e.time     || "",
            Address:     e.address  || "",
            Latitude:    e.lat      || 30.2672,
            Longitude:   e.lng      || -97.7431,
            Description: e.desc     || "",
            Source:      e.source   || "",
            Status:      status,     // "pending" or "duplicate" — NEVER "live"
            SubmittedBy: "COMN Auto-Scraper",
            DuplicateOf: dupOf,
          }};
        })
      });
      saved.push(...(data.records || []));
    } catch(e) { console.error("Save batch error:", e.message); }
  }
  return saved;
}

// ── EMAIL DIGEST ──────────────────────────────────────────────────────────────
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

// ── MAIN SCRAPE ───────────────────────────────────────────────────────────────
async function runScraper() {
  console.log(`[${new Date().toISOString()}] Starting scrape...`);
  try {
    const [council, voting, mobilize, handsOff, ajc, lwv] = await Promise.all([
      scrapeCouncil(), scrapeVoting(), scrapeMobilize(),
      scrapeHandsOff(), scrapeAJC(), scrapeLWV(),
    ]);
    const allFound = [...council, ...voting, ...mobilize, ...handsOff, ...ajc, ...lwv];
    console.log(`Found ${allFound.length} candidate events`);

    const existingEvents = await getExistingEventsForDedup();
    const existingKeys = new Set(
      existingEvents.map(e => `${normalizeForDedup(e.name)}__${e.date}`)
    );

    // Remove exact matches (already in system)
    const candidates = allFound.filter(e => {
      if (!e.date || !e.name) return false;
      return !existingKeys.has(`${normalizeForDedup(e.name)}__${e.date}`);
    });
    console.log(`${candidates.length} after exact-dedup`);

    // Save — near-dupes automatically get status="duplicate"
    const saved = candidates.length > 0 ? await saveEvents(candidates, existingEvents) : [];
    const newPending = saved.filter(r => r.fields?.Status === "pending").length;
    const newDupes   = saved.filter(r => r.fields?.Status === "duplicate").length;
    console.log(`Saved: ${newPending} pending, ${newDupes} duplicates flagged`);

    let totalPending = 0;
    try {
      const f1 = encodeURIComponent(`{Status}='pending'`);
      const f2 = encodeURIComponent(`{Status}='pending_2nd'`);
      const [d1, d2] = await Promise.all([
        airtable(`?filterByFormula=${f1}&fields[]=Name`),
        airtable(`?filterByFormula=${f2}&fields[]=Name`),
      ]);
      totalPending = (d1.records||[]).length + (d2.records||[]).length;
    } catch(e) {}

    await sendDigest(newPending, totalPending);
    console.log(`[${new Date().toISOString()}] Scrape complete.`);
    return { found: allFound.length, newPending, newDupes, totalPending };
  } catch(e) {
    console.error("Scraper error:", e.message);
    return { error: e.message };
  }
}

// ── CRON: 12 PM CT ───────────────────────────────────────────────────────────
let lastScrapeDate = "";
setInterval(() => {
  const now = new Date();
  const ctHour = (now.getUTCHours() - 5 + 24) % 24;
  const ctDate = now.toISOString().split("T")[0];
  if (ctHour === 12 && ctDate !== lastScrapeDate) {
    lastScrapeDate = ctDate;
    console.log("⏰ 12 PM CT — running daily scrape");
    runScraper();
  }
}, 60 * 1000);

// ══════════════════════════════════════════════════════════════════════════════
// ROUTES
// ══════════════════════════════════════════════════════════════════════════════

app.get("/", (req, res) =>
  res.json({ status: "COMN server running", nextScrape: "Daily 12 PM CT" })
);

// ── PUBLIC EVENTS ─────────────────────────────────────────────────────────────
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
    res.status(500).json({ error: "Failed to fetch events" });
  }
});

app.get("/pending-count", async (req, res) => {
  try {
    const f1 = encodeURIComponent(`{Status}='pending'`);
    const f2 = encodeURIComponent(`{Status}='pending_2nd'`);
    const [d1, d2] = await Promise.all([
      airtable(`?filterByFormula=${f1}&fields[]=Name`),
      airtable(`?filterByFormula=${f2}&fields[]=Name`),
    ]);
    res.json({ count: (d1.records||[]).length + (d2.records||[]).length });
  } catch(e) { res.status(500).json({ count: 0 }); }
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
        Description: f.desc   || "",
        Source:      f.source || "",
        Status: "pending",
        SubmittedBy: f.submittedBy || "Public",
      }}]
    });
    res.json({ success: true, id: data.records?.[0]?.id });
  } catch(e) {
    console.error("POST /submit:", e.message);
    res.status(500).json({ error: "Failed to submit event" });
  }
});

// ── ADMIN AUTH ────────────────────────────────────────────────────────────────

// Check if any real admins exist (excludes invite_token records)
app.get("/admin/auth/status", async (req, res) => {
  try {
    const filter = encodeURIComponent(`NOT({Role}="invite_token")`);
    const data = await airtableAdmins(`?filterByFormula=${filter}&fields[]=Email&maxRecords=1`);
    res.json({ hasAdmins: (data.records || []).length > 0 });
  } catch(e) {
    res.json({ hasAdmins: false });
  }
});

// Register — first account = super_admin (no invite needed); others need valid invite token
app.post("/admin/auth/register", async (req, res) => {
  try {
    const { email, password, name, inviteToken } = req.body;
    if (!email || !password || !name)
      return res.status(400).json({ error: "Name, email and password are required" });
    if (password.length < 8)
      return res.status(400).json({ error: "Password must be at least 8 characters" });

    // Check if any real admins already exist
    const filter = encodeURIComponent(`NOT({Role}="invite_token")`);
    const existing = await airtableAdmins(`?filterByFormula=${filter}&fields[]=Email&maxRecords=1`);
    const isFirstAdmin = (existing.records || []).length === 0;

    if (!isFirstAdmin) {
      if (!inviteToken)
        return res.status(403).json({ error: "An invite link is required to apply as admin" });
      const validToken = await getInviteToken();
      if (inviteToken !== validToken)
        return res.status(403).json({ error: "Invalid or expired invite link — ask the Super Admin for a new one" });
    }

    const emailFilter = encodeURIComponent(`AND({Email}="${email.toLowerCase().trim()}",NOT({Role}="invite_token"))`);
    const emailCheck = await airtableAdmins(`?filterByFormula=${emailFilter}&fields[]=Email`);
    if ((emailCheck.records || []).length > 0)
      return res.status(400).json({ error: "An account with this email already exists" });

    const role = isFirstAdmin ? "super_admin" : "pending_approval";
    const hash = await bcrypt.hash(password, 12);

    const created = await airtableAdmins("", "POST", {
      records: [{ fields: {
        Email:        email.toLowerCase().trim(),
        PasswordHash: hash,
        DisplayName:  name.trim(),
        Role:         role,
        CreatedAt:    today(),
      }}]
    });

    console.log(`[ADMIN CREATED] ${email} as ${role}`);

    if (isFirstAdmin) {
      const record = created.records[0];
      const token = jwt.sign(
        { id: record.id, email: email.toLowerCase().trim(), name: name.trim(), role: "super_admin" },
        JWT_SECRET, { expiresIn: "30d" }
      );
      res.json({ success: true, token, email: email.toLowerCase().trim(), name: name.trim(), role: "super_admin" });
    } else {
      res.json({ success: true, pending: true, message: "Application submitted! The Super Admin will review and approve your account." });
    }
  } catch(e) {
    console.error("Register error:", e.message);
    res.status(500).json({ error: "Failed to create account: " + e.message });
  }
});

// Login
app.post("/admin/auth/login", async (req, res) => {
  try {
    const { email, password } = req.body;
    if (!email || !password)
      return res.status(400).json({ error: "Email and password required" });

    const filter = encodeURIComponent(`{Email}='${email.toLowerCase().trim()}'`);
    const data = await airtableAdmins(`?filterByFormula=${filter}`);
    const record = (data.records || [])[0];

    if (!record)
      return res.status(401).json({ error: "No account found with that email" });

    if (record.fields.Role === "pending_approval")
      return res.status(403).json({ error: "Your account is pending approval from the Super Admin" });

    if (record.fields.Role === "invite_token")
      return res.status(401).json({ error: "No account found with that email" });

    const valid = await bcrypt.compare(password, record.fields.PasswordHash || "");
    if (!valid)
      return res.status(401).json({ error: "Incorrect password" });

    const payload = {
      id:    record.id,
      email: record.fields.Email,
      name:  record.fields.DisplayName || record.fields.Email,
      role:  record.fields.Role || "admin",
    };
    const token = jwt.sign(payload, JWT_SECRET, { expiresIn: "30d" });
    console.log(`[LOGIN] ${payload.email} (${payload.role})`);
    res.json({ token, email: payload.email, name: payload.name, role: payload.role });
  } catch(e) {
    console.error("Login error:", e.message);
    res.status(500).json({ error: "Login failed" });
  }
});

// Get current admin's profile
app.get("/admin/profile", requireAuth, async (req, res) => {
  res.json({ id: req.admin.id, email: req.admin.email, name: req.admin.name, role: req.admin.role });
});

// Update name or password
app.patch("/admin/profile", requireAuth, async (req, res) => {
  try {
    const { name, currentPassword, newPassword } = req.body;
    const updates = {};
    if (name) updates.DisplayName = name.trim();
    if (newPassword) {
      if (!currentPassword)
        return res.status(400).json({ error: "Current password required to set a new one" });
      if (newPassword.length < 8)
        return res.status(400).json({ error: "New password must be at least 8 characters" });
      const rec = await airtableAdmins(`/${req.admin.id}`);
      const valid = await bcrypt.compare(currentPassword, rec.fields?.PasswordHash || "");
      if (!valid) return res.status(401).json({ error: "Current password is incorrect" });
      updates.PasswordHash = await bcrypt.hash(newPassword, 12);
    }
    if (Object.keys(updates).length === 0)
      return res.status(400).json({ error: "Nothing to update" });
    await airtableAdmins(`/${req.admin.id}`, "PATCH", { fields: updates });
    res.json({ success: true });
  } catch(e) {
    console.error("Profile update:", e.message);
    res.status(500).json({ error: "Failed to update profile" });
  }
});

// List active admins (super_admin only — excludes pending and invite_token)
app.get("/admin/team", requireAuth, async (req, res) => {
  if (req.admin.role !== "super_admin")
    return res.status(403).json({ error: "Super admin access required" });
  try {
    const filter = encodeURIComponent(`AND(NOT({Role}="invite_token"),NOT({Role}="pending_approval"))`);
    const data = await airtableAdmins(`?filterByFormula=${filter}&fields[]=Email&fields[]=DisplayName&fields[]=Role&fields[]=CreatedAt`);
    res.json({ admins: (data.records || []).map(r => ({
      id:        r.id,
      email:     r.fields.Email       || "",
      name:      r.fields.DisplayName || "",
      role:      r.fields.Role        || "admin",
      createdAt: r.fields.CreatedAt   || "",
    }))});
  } catch(e) { res.status(500).json({ error: "Failed to fetch team" }); }
});

// Get current invite link (super_admin only)
app.get("/admin/invite-token", requireAuth, async (req, res) => {
  if (req.admin.role !== "super_admin")
    return res.status(403).json({ error: "Super Admin only" });
  try {
    const token = await getInviteToken();
    res.json({ token, link: `https://comnground.netlify.app/admin.html?invite=${token}` });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// Regenerate invite link (super_admin only — old link stops working immediately)
app.post("/admin/invite-token/regenerate", requireAuth, async (req, res) => {
  if (req.admin.role !== "super_admin")
    return res.status(403).json({ error: "Super Admin only" });
  try {
    const filter = encodeURIComponent(`{Role}="invite_token"`);
    const data = await airtableAdmins(`?filterByFormula=${filter}&maxRecords=1`);
    const newToken = generateToken();
    if (data.records && data.records.length > 0) {
      await airtableAdmins(`/${data.records[0].id}`, "PATCH", { fields: { DisplayName: newToken } });
    } else {
      await airtableAdmins("", "POST", { records: [{ fields: { Email: "_invite_token", DisplayName: newToken, Role: "invite_token" } }] });
    }
    res.json({ token: newToken, link: `https://comnground.netlify.app/admin.html?invite=${newToken}` });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// List pending admin applications (super_admin only)
app.get("/admin/pending-admins", requireAuth, async (req, res) => {
  if (req.admin.role !== "super_admin")
    return res.status(403).json({ error: "Super Admin only" });
  try {
    const filter = encodeURIComponent(`{Role}="pending_approval"`);
    const data = await airtableAdmins(`?filterByFormula=${filter}&fields[]=Email&fields[]=DisplayName&fields[]=CreatedAt`);
    res.json({ admins: (data.records || []).map(r => ({ id: r.id, email: r.fields.Email || "", name: r.fields.DisplayName || "", appliedAt: r.fields.CreatedAt || "" })) });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// Approve a pending admin (super_admin only)
app.post("/admin/pending-admins/:id/approve", requireAuth, async (req, res) => {
  if (req.admin.role !== "super_admin")
    return res.status(403).json({ error: "Super Admin only" });
  try {
    await airtableAdmins(`/${req.params.id}`, "PATCH", { fields: { Role: "admin" } });
    console.log(`[ADMIN APPROVED] ${req.params.id} by ${req.admin.email}`);
    res.json({ success: true });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// Reject a pending admin — deletes the record (super_admin only)
app.delete("/admin/pending-admins/:id", requireAuth, async (req, res) => {
  if (req.admin.role !== "super_admin")
    return res.status(403).json({ error: "Super Admin only" });
  try {
    await airtableAdmins(`/${req.params.id}`, "DELETE");
    console.log(`[ADMIN REJECTED] ${req.params.id} by ${req.admin.email}`);
    res.json({ success: true });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// ── ADMIN EVENTS ──────────────────────────────────────────────────────────────

app.get("/admin/events", requireAuth, async (req, res) => {
  try {
    const status = req.query.status || "pending";
    const filter = encodeURIComponent(`{Status}='${status}'`);
    const data = await airtable(`?filterByFormula=${filter}&sort[0][field]=Date&sort[0][direction]=asc`);
    res.json({ records: data.records || [] });
  } catch(e) {
    console.error("GET /admin/events:", e.message);
    res.status(500).json({ error: "Failed to fetch events" });
  }
});

// ⚠️  THE ONLY WAY AN EVENT BECOMES "live" — requires human authentication.
app.patch("/admin/events/:id", requireAuth, async (req, res) => {
  try {
    const { action } = req.body;
    const adminEmail = req.admin.email;
    const adminRole  = req.admin.role;
    const eventId    = req.params.id;

    if (!["approve", "reject"].includes(action))
      return res.status(400).json({ error: "action must be 'approve' or 'reject'" });

    const current = await airtable(`/${eventId}`);
    const currentStatus    = current.fields?.Status    || "";
    const currentApprovers = current.fields?.Approvers || "";

    if (action === "reject") {
      await airtable(`/${eventId}`, "PATCH", {
        fields: { Status: "rejected", RejectedBy: adminEmail, Approvers: "" }
      });
      console.log(`[REJECTED] ${eventId} by ${adminEmail}`);
      return res.json({ success: true, newStatus: "rejected" });
    }

    // APPROVE
    if (adminRole === "super_admin") {
      // Super admin: instant live — they count as the full approval chain
      await airtable(`/${eventId}`, "PATCH", {
        fields: { Status: "live", Approvers: `${adminEmail} (super admin)`, RejectedBy: "" }
      });
      console.log(`[SUPER ADMIN → LIVE] ${eventId} by ${adminEmail}`);
      return res.json({ success: true, newStatus: "live", message: "Event is now live!" });
    }

    // Regular admin — two-step
    if (currentStatus === "pending") {
      await airtable(`/${eventId}`, "PATCH", {
        fields: { Status: "pending_2nd", Approvers: adminEmail }
      });
      console.log(`[1ST APPROVAL] ${eventId} by ${adminEmail} — awaiting 2nd`);
      return res.json({
        success: true, newStatus: "pending_2nd",
        message: "First approval done! A second admin must now approve before it goes live.",
      });
    }

    if (currentStatus === "pending_2nd") {
      if (currentApprovers.includes(adminEmail))
        return res.status(400).json({
          error: "You already gave the first approval. A different admin must give the second.",
        });
      const allApprovers = [currentApprovers, adminEmail].filter(Boolean).join(", ");
      await airtable(`/${eventId}`, "PATCH", {
        fields: { Status: "live", Approvers: allApprovers }
      });
      console.log(`[2ND APPROVAL → LIVE] ${eventId}. Approvers: ${allApprovers}`);
      return res.json({ success: true, newStatus: "live", message: "Event approved and now live!" });
    }

    return res.status(400).json({ error: `Cannot approve event with status: ${currentStatus}` });

  } catch(e) {
    console.error("PATCH /admin/events/:id:", e.message);
    res.status(500).json({ error: "Failed to update event" });
  }
});

// Restore a rejected or duplicate event back to pending
app.post("/admin/events/:id/restore", requireAuth, async (req, res) => {
  try {
    const current = await airtable(`/${req.params.id}`);
    const status = current.fields?.Status || "";
    if (!["rejected", "duplicate"].includes(status))
      return res.status(400).json({ error: "Only rejected or duplicate events can be restored" });
    await airtable(`/${req.params.id}`, "PATCH", {
      fields: { Status: "pending", RejectedBy: "", DuplicateOf: "", Approvers: "" }
    });
    console.log(`[RESTORED] ${req.params.id} to pending by ${req.admin.email}`);
    res.json({ success: true, newStatus: "pending" });
  } catch(e) {
    console.error("Restore error:", e.message);
    res.status(500).json({ error: "Failed to restore event" });
  }
});

// Manual scrape trigger
app.post("/admin/scrape-now", requireAuth, async (req, res) => {
  res.json({ message: "Scrape started — new events will appear in your queue shortly" });
  runScraper();
});

app.listen(PORT, () => console.log(`COMN server on port ${PORT}`));
