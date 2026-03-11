const express = require("express");
const cors    = require("cors");
const bcrypt  = require("bcryptjs");
const jwt     = require("jsonwebtoken");
const crypto  = require("crypto");

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
const JWT_SECRET = process.env.JWT_SECRET || "comn-dev-jwt-secret-CHANGE-IN-PROD";
if (!process.env.JWT_SECRET) console.warn("[WARN] JWT_SECRET env var not set — using insecure default. Set it in Railway before going to production.");

function generateToken() {
  return crypto.randomBytes(32).toString("hex");
}

// ── RATE LIMITING (in-memory, no extra deps) ──────────────────────────────────
const rateLimitStore = new Map();
function rateLimit(key, maxPerMinute) {
  const now = Date.now(), windowMs = 60 * 1000;
  const rec = rateLimitStore.get(key) || { count: 0, resetAt: now + windowMs };
  if (now > rec.resetAt) { rec.count = 0; rec.resetAt = now + windowMs; }
  rec.count++;
  rateLimitStore.set(key, rec);
  return rec.count > maxPerMinute;
}
setInterval(() => { const now = Date.now(); rateLimitStore.forEach((v,k) => { if (now > v.resetAt) rateLimitStore.delete(k); }); }, 5 * 60 * 1000);

// ── LOGIN LOCKOUT ─────────────────────────────────────────────────────────────
const loginAttempts = new Map(); // email -> { count, lockedUntil }
function checkLoginLockout(email) {
  const rec = loginAttempts.get(email);
  if (!rec) return false;
  if (rec.lockedUntil && Date.now() < rec.lockedUntil) return true;
  return false;
}
function recordLoginFailure(email) {
  const rec = loginAttempts.get(email) || { count: 0, lockedUntil: null };
  rec.count++;
  if (rec.count >= 5) rec.lockedUntil = Date.now() + 15 * 60 * 1000; // lock 15 min
  loginAttempts.set(email, rec);
}
function clearLoginFailures(email) { loginAttempts.delete(email); }

async function getInviteToken() {
  const filter = encodeURIComponent(`{Role}="invite_token"`);
  const data = await airtableAdmins(`?filterByFormula=${filter}&maxRecords=1`);
  if (data.records && data.records.length > 0) return data.records[0].fields.DisplayName || null;
  const token = generateToken();
  await airtableAdmins("", "POST", { records: [{ fields: { Email: "_invite_token", DisplayName: token, Role: "invite_token" } }] });
  return token;
}

app.use(cors({ origin: ["https://comnground.netlify.app", "https://comn-server-production.up.railway.app", "http://localhost:3000", "http://localhost:8080"] }));
app.use(express.json());

// ── CONTENT HELPERS ───────────────────────────────────────────────────────────
const ADMIN_NOTE_PHRASES = [
  "before approving", "check source link", "visit the source link",
  "NOTE:", "verify this is not a duplicate", "consider rejecting",
  "location tbd", "see source for venue", "check source for venue",
  "full details before approving"
];
function hasAdminNote(desc) {
  if (!desc) return false;
  const d = desc.toLowerCase();
  return ADMIN_NOTE_PHRASES.some(p => d.includes(p.toLowerCase()));
}
function stripAdminNotes(desc) {
  if (!desc) return desc;
  let s = desc;
  ADMIN_NOTE_PHRASES.forEach(p => {
    const re = new RegExp("[.!]?\\s*" + p.replace(/[.*+?^${}()|[\]\\]/g,"\\$&") + "[^.!]*[.!]?", "gi");
    s = s.replace(re, "");
  });
  // Also strip standalone NOTE: lines
  s = s.replace(/NOTE:[^\n]*/gi, "").replace(/\s{2,}/g, " ").trim();
  return s || desc; // fall back to original if stripped to empty
}
function escHtml(str) {
  return String(str || "").replace(/&/g,"&amp;").replace(/</g,"&lt;").replace(/>/g,"&gt;").replace(/"/g,"&quot;");
}

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
const OPTIONAL_FIELDS = ["DuplicateOf", "RejectedBy", "Approvers", "LastScraped"];

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

// ── EVENT TYPE DETECTION (non-partisan, content-based) ────────────────────────
// Type is determined by CONTENT of the event, never by which source it came from.
function detectEventType(name, desc) {
  const text = ((name || "") + " " + (desc || "")).toLowerCase();
  if (/\b(election day|election|vote|voting|polls open|ballot|runoff|primary election|early voting|voter registration)\b/.test(text)) return "voting";
  if (/\b(city council|town hall|townhall|committee meeting|public hearing|commission meeting|board meeting|city meeting)\b/.test(text)) return "townhall";
  if (/\b(online|virtual|zoom|webinar|livestream|live stream|remote meeting)\b/.test(text)) return "online";
  if (/\b(march|rally|protest|demonstration|strike|walkout|vigil|picket)\b/.test(text)) return "protest";
  return "nonprofit";
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

// Fetch an event's detail page and extract accurate info (deep-link enrichment)
async function fetchEventDetails(url) {
  if (!url || url.endsWith("/events") || url.endsWith("/events/") || url.includes("api.mobilize.us")) return {};
  try {
    const html = await fetchHTML(url);
    if (!html) return {};
    const details = {};

    // 1) Try JSON-LD structured data first (most reliable)
    const jsonRe = /<script type="application\/ld\+json">([\s\S]*?)<\/script>/gi;
    let jm;
    while ((jm = jsonRe.exec(html)) !== null) {
      try {
        const parsed = JSON.parse(jm[1]);
        const items = Array.isArray(parsed) ? parsed : [parsed];
        const ev = items.find(p => p["@type"] === "Event" || p.startDate);
        if (!ev) continue;
        if (ev.startDate) {
          const d = new Date(ev.startDate);
          if (!isNaN(d)) { details.date = d.toISOString().split("T")[0]; details.time = d.toTimeString().slice(0,5); }
        }
        const loc = ev.location?.address?.streetAddress || ev.location?.name;
        if (loc && loc.length > 5 && !loc.toLowerCase().includes("see source")) details.address = loc.slice(0,200);
        if (ev.location?.geo?.latitude)  details.lat = parseFloat(ev.location.geo.latitude);
        if (ev.location?.geo?.longitude) details.lng = parseFloat(ev.location.geo.longitude);
        const rawDesc = (ev.description || "").replace(/<[^>]*>/g,"").replace(/\s+/g," ").trim();
        if (rawDesc.length > 20) details.desc = rawDesc.slice(0,250);
        if (details.date || details.address || details.desc) break;
      } catch(e) {}
    }

    // 2) Try OpenGraph meta tags for description if still missing
    if (!details.desc) {
      const ogM = html.match(/<meta[^>]+property=["']og:description["'][^>]+content=["']([^"']{20,})["']/i)
                || html.match(/<meta[^>]+content=["']([^"']{20,})["'][^>]+property=["']og:description["']/i);
      if (ogM) details.desc = ogM[1].replace(/\s+/g," ").trim().slice(0,250);
    }

    // 3) Try to find a street address if still missing
    if (!details.address) {
      const addrM = html.match(/(\d{3,5}\s+[A-Z][a-z]+(?:\s+[A-Za-z]+){0,3}(?:\s+(?:St|Ave|Blvd|Dr|Rd|Way|Ln|Ct|Pl|Loop|Pkwy|Hwy))[^<"]{0,60})/);
      if (addrM) details.address = addrM[1].trim().slice(0,200);
    }

    return details;
  } catch(e) { return {}; }
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
    const base = (data.data || [])
      .filter(e => e.timeslots?.length > 0)
      .map(e => {
        const ts = e.timeslots[0];
        const date = ts?.start_date ? new Date(ts.start_date * 1000).toISOString().split("T")[0] : null;
        const time = ts?.start_date ? new Date(ts.start_date * 1000).toTimeString().slice(0,5) : "10:00";
        const isVirtual = e.is_virtual || e.location?.is_virtual;
        const rawDesc = (e.description || "").replace(/<[^>]*>/g,"").replace(/\s+/g," ").trim();
        return {
          name: (e.title || "Austin Civic Event").slice(0,100),
          type: "nonprofit", // will be overridden by detectEventType below
          date, time,
          address: isVirtual ? "Online" : (e.location?.venue || e.location?.address_lines?.[0] || "Austin, TX"),
          lat: isVirtual ? 0 : (parseFloat(e.location?.lat) || 30.2672),
          lng: isVirtual ? 0 : (parseFloat(e.location?.lon) || -97.7431),
          desc: rawDesc.slice(0,250),
          source: e.browser_url || "",
        };
      })
      .filter(e => e.date && inWindow(e.date));

    // Deep-link each event for accurate details
    const enriched = [];
    for (const ev of base) {
      if (ev.source) {
        const d = await fetchEventDetails(ev.source);
        if (d.date && inWindow(d.date)) ev.date = d.date;
        if (d.time) ev.time = d.time;
        if (d.address && d.address.length > 5) ev.address = d.address;
        if (d.lat) ev.lat = d.lat;
        if (d.lng) ev.lng = d.lng;
        if (d.desc && d.desc.length > 20) ev.desc = d.desc;
      }
      ev.type = detectEventType(ev.name, ev.desc);
      if (ev.date && inWindow(ev.date)) enriched.push(ev);
    }
    return enriched;
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
          name: (item.title || "Hands Off Central TX Event").slice(0,100),
          type: "nonprofit", date, time: time || "13:00",
          address: loc,
          lat: parseFloat(item.location?.markerLat) || 30.2747,
          lng: parseFloat(item.location?.markerLng) || -97.7403,
          desc: rawDesc.slice(0,250),
          source: item.fullUrl ? `${BASE}${item.fullUrl}` : `${BASE}/events`,
        });
      }
      if (events.length > 0) {
        // Deep-link each event for full details
        const enriched = [];
        for (const ev of events) {
          if (ev.source && !ev.source.endsWith("/events")) {
            const d = await fetchEventDetails(ev.source);
            if (d.date && inWindow(d.date)) ev.date = d.date;
            if (d.time) ev.time = d.time;
            if (d.address && d.address.length > 5) ev.address = d.address;
            if (d.lat) ev.lat = d.lat; if (d.lng) ev.lng = d.lng;
            if (d.desc && d.desc.length > 20) ev.desc = d.desc;
          }
          ev.type = detectEventType(ev.name, ev.desc);
          if (ev.date && inWindow(ev.date)) enriched.push(ev);
        }
        console.log(`HandsOff (Squarespace API): ${enriched.length} events`);
        return enriched;
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
        name: (ev.name || "Hands Off Central TX Event").slice(0,100),
        type: "nonprofit", date, time: timeStr || "13:00",
        address: loc,
        lat: parseFloat(ev.location?.geo?.latitude) || 30.2747,
        lng: parseFloat(ev.location?.geo?.longitude) || -97.7403,
        desc: rawDesc.slice(0,250),
        source: ev.url || `${BASE}/events`,
      });
    }
    if (events.length > 0) {
      const enriched = [];
      for (const ev of events) {
        if (ev.source && !ev.source.endsWith("/events")) {
          const d = await fetchEventDetails(ev.source);
          if (d.date && inWindow(d.date)) ev.date = d.date;
          if (d.time) ev.time = d.time;
          if (d.address && d.address.length > 5) ev.address = d.address;
          if (d.lat) ev.lat = d.lat; if (d.lng) ev.lng = d.lng;
          if (d.desc && d.desc.length > 20) ev.desc = d.desc;
        }
        ev.type = detectEventType(ev.name, ev.desc);
        if (ev.date && inWindow(ev.date)) enriched.push(ev);
      }
      console.log(`HandsOff (JSON-LD): ${enriched.length} events`);
      return enriched;
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
      type: "nonprofit", date: dates[dateIdx++], time: "13:00",
      address: "Austin, TX", lat: 30.2747, lng: -97.7403,
      desc: "",
      source: `${BASE}${lm[1]}`,
    });
  }
  // Deep-link each event for full details
  const enriched = [];
  for (const ev of events) {
    const d = await fetchEventDetails(ev.source);
    if (d.date && inWindow(d.date)) ev.date = d.date;
    if (d.time) ev.time = d.time;
    if (d.address && d.address.length > 5) ev.address = d.address;
    if (d.lat) ev.lat = d.lat; if (d.lng) ev.lng = d.lng;
    if (d.desc && d.desc.length > 20) ev.desc = d.desc;
    ev.type = detectEventType(ev.name, ev.desc);
    if (ev.date && inWindow(ev.date)) enriched.push(ev);
  }
  console.log(`HandsOff (HTML fallback): ${enriched.length} events`);
  return enriched;
}

async function scrapeAJC() {
  const html = await fetchHTML("https://austinjustice.org/events/");
  if (!html) return [];
  const base = [];
  const dateRe = /(January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},?\s*202[67]/gi;
  const nameRe = /class="[^"]*tribe-event[^"]*"[^>]*>[\s\S]{0,100}?<a[^>]+href="([^"]+)"[^>]*>([^<]{5,80})<\/a/gi;
  const linkRe  = /class="[^"]*tribe-event[^"]*"[^>]*>[\s\S]{0,100}?<a[^>]+href="([^"]+)"/gi;
  const names = [], dates = [], links = [];
  let m;
  while ((m = nameRe.exec(html)) !== null) { links.push(m[1]); names.push(m[2].trim()); }
  while ((m = dateRe.exec(html)) !== null) {
    const date = parseDate(m[0]);
    if (date && inWindow(date)) dates.push(date);
  }
  for (let i = 0; i < Math.min(names.length, dates.length, 8); i++) {
    if (!names[i] || names[i].length < 4) continue;
    base.push({
      name: names[i].replace(/&amp;/g,"&"),
      type: "nonprofit", date: dates[i], time: "18:00",
      address: "Austin, TX", lat: 30.2620, lng: -97.7213,
      desc: "",
      source: links[i] || "https://austinjustice.org/events/",
    });
  }
  // Deep-link each event
  const enriched = [];
  for (const ev of base) {
    if (ev.source && ev.source !== "https://austinjustice.org/events/") {
      const d = await fetchEventDetails(ev.source);
      if (d.date && inWindow(d.date)) ev.date = d.date;
      if (d.time) ev.time = d.time;
      if (d.address && d.address.length > 5) ev.address = d.address;
      if (d.lat) ev.lat = d.lat; if (d.lng) ev.lng = d.lng;
      if (d.desc && d.desc.length > 20) ev.desc = d.desc;
    }
    ev.type = detectEventType(ev.name, ev.desc);
    if (ev.date && inWindow(ev.date)) enriched.push(ev);
  }
  return enriched;
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
      name: "League of Women Voters Austin — Civic Event",
      type: "nonprofit", date, time: "10:00",
      address: "Austin, TX — see source for location",
      lat: 30.2676, lng: -97.7521,
      desc: "Civic engagement event hosted by the League of Women Voters Austin chapter. See the source link for full details, location, and registration.",
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
    const data = await airtable(`?filterByFormula=${filter}&fields[]=Name&fields[]=Date&fields[]=Source`);
    return (data.records || []).map(r => ({
      id:     r.id,
      name:   r.fields.Name   || "",
      date:   r.fields.Date   || "",
      source: r.fields.Source || "",
    }));
  } catch(e) { console.warn("Dedup fetch failed:", e.message); return []; }
}

// ── SAVE EVENTS ───────────────────────────────────────────────────────────────
// ⚠️  HUMAN APPROVAL REQUIRED — SAFETY RULE ⚠️
// Events are saved as "pending" or "duplicate". NEVER "live" automatically.
// The ONLY path to "live" is a human admin approving via the admin panel.
async function saveEvents(events, existingEventsForDedup) {
  const saved = [];
  // Build a set of existing source URLs for fast URL-based dedup
  const existingUrls = new Set(
    (existingEventsForDedup || []).map(e => e.source).filter(Boolean)
  );
  for (let i = 0; i < events.length; i += 10) {
    const batch = events.slice(i, i + 10);
    try {
      const data = await airtable("", "POST", {
        records: batch.map(e => {
          let status = "pending"; // Default — NEVER change to "live"
          let dupOf  = "";
          // URL-based dedup (most precise — same source URL = same event)
          if (e.source && existingUrls.has(e.source)) {
            status = "duplicate";
            const urlMatch = (existingEventsForDedup || []).find(ex => ex.source === e.source);
            dupOf = urlMatch?.id || "";
            console.log(`  ⚠️ URL duplicate: "${e.name}" — source URL already in system`);
          } else if (existingEventsForDedup) {
            // Fuzzy name+date dedup as fallback
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
            LastScraped: new Date().toISOString(),
          }};
        })
      });
      saved.push(...(data.records || []));
    } catch(e) { console.error("Save batch error:", e.message); }
  }
  return saved;
}

// ── EMAIL DIGEST ──────────────────────────────────────────────────────────────
async function sendDigest(stats) {
  if (!RESEND_KEY) { console.log("No RESEND_KEY — skipping email"); return; }
  const { newPending, totalPending, found, dupes, perSource, errors, typeCounts, reclassified } = stats;
  const dateStr = new Date().toLocaleDateString("en-US", { month: "long", day: "numeric", year: "numeric" });
  const subject = newPending > 0
    ? `COMN Daily Digest — ${newPending} new event${newPending !== 1 ? "s" : ""} pending review`
    : "COMN Daily Digest — No new events found today";

  // Bias audit — flag if any type is >60% of new events
  const totalTyped = Object.values(typeCounts || {}).reduce((a, b) => a + b, 0);
  const biasLines = totalTyped > 0
    ? Object.entries(typeCounts || {}).map(([t, c]) =>
        `  ${t}: ${c} event${c !== 1 ? "s" : ""} (${Math.round(c / totalTyped * 100)}%)`
      ).join("\n")
    : "  No new events";
  const biasWarnings = [];
  for (const [t, c] of Object.entries(typeCounts || {})) {
    if (totalTyped > 4 && c / totalTyped > 0.6)
      biasWarnings.push(`  ⚠️  "${t}" is ${Math.round(c / totalTyped * 100)}% of new events — review scraper sources`);
  }
  if (reclassified > 0)
    biasWarnings.push(`  ⚠️  ${reclassified} event${reclassified > 1 ? "s" : ""} reclassified by content analysis`);

  // Per-source breakdown
  const sourceLines = Object.entries(perSource || {}).map(([src, count]) =>
    `  ${src}: ${count} event${count !== 1 ? "s" : ""}${count === 0 ? " ⚠️  (0 found — possible scraping failure)" : ""}`
  ).join("\n");
  const errorLines = (errors || []).map(e => `  ❌ ${e}`).join("\n");

  const lines = [
    `COMN Daily Digest — ${dateStr}`,
    "",
    "📊 Scrape Summary",
    `  Sources checked: ${Object.keys(perSource || {}).length}`,
    `  Total candidate events: ${found}`,
    `  Duplicates filtered: ${dupes}`,
    `  New events pending review: ${newPending}`,
    errors?.length > 0 ? `  Scraping errors: ${errors.length}` : null,
    "",
    "📦 Events by Source",
    sourceLines || "  (none)",
  ];
  if (errors?.length > 0) lines.push("", "🚨 Scraping Errors", errorLines);
  lines.push(
    "",
    "🔍 Bias Audit",
    biasLines,
    biasWarnings.length > 0 ? biasWarnings.join("\n") : "  ✅ No bias flags",
    "",
    `📋 Total events currently pending admin review: ${totalPending}`,
    "",
    `🔗 Review now: ${ADMIN_URL}`,
    "",
    "---",
    "COMN is committed to non-partisan, fact-based civic information.",
  );
  const text = lines.filter(l => l !== null).join("\n");
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
  const scrapeErrors = [];
  const perSource = {};
  const typeCounts = {};

  // Wrap each scraper with per-source error tracking
  async function runSource(label, fn) {
    try {
      const results = await fn();
      perSource[label] = results.length;
      for (const e of results) typeCounts[e.type] = (typeCounts[e.type] || 0) + 1;
      return results;
    } catch(e) {
      console.error(`[${label}] scraper error:`, e.message);
      scrapeErrors.push(`${label} — ${e.message.slice(0, 120)}`);
      perSource[label] = 0;
      return [];
    }
  }

  try {
    const [council, voting, mobilize, handsOff, ajc, lwv] = await Promise.all([
      runSource("City Council", scrapeCouncil),
      runSource("Travis County Voting", scrapeVoting),
      runSource("Mobilize", scrapeMobilize),
      runSource("Hands Off Central TX", scrapeHandsOff),
      runSource("Austin Justice Coalition", scrapeAJC),
      runSource("LWV Austin", scrapeLWV),
    ]);
    const allFound = [...council, ...voting, ...mobilize, ...handsOff, ...ajc, ...lwv];
    console.log(`Found ${allFound.length} candidate events`);

    const existingEvents = await getExistingEventsForDedup();
    const existingKeys = new Set(
      existingEvents.map(e => `${normalizeForDedup(e.name)}__${e.date}`)
    );
    const existingUrls = new Set(existingEvents.map(e => e.source).filter(Boolean));

    // Remove exact name+date or URL matches (already in system)
    const candidates = allFound.filter(e => {
      if (!e.date || !e.name) return false;
      if (e.source && existingUrls.has(e.source)) return false; // URL already exists
      return !existingKeys.has(`${normalizeForDedup(e.name)}__${e.date}`);
    });
    console.log(`${candidates.length} after exact-dedup (${allFound.length - candidates.length} filtered)`);

    // Save — near-dupes automatically get status="duplicate"
    const saved = candidates.length > 0 ? await saveEvents(candidates, existingEvents) : [];
    const newPending = saved.filter(r => r.fields?.Status === "pending").length;
    const newDupes   = saved.filter(r => r.fields?.Status === "duplicate").length;
    const dupes      = (allFound.length - candidates.length) + newDupes;
    console.log(`Saved: ${newPending} pending, ${newDupes} near-dupes flagged`);

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

    await sendDigest({ newPending, totalPending, found: allFound.length, dupes, perSource, errors: scrapeErrors, typeCounts, reclassified: 0 });
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
  if (rateLimit(req.ip + "_submit", 10))
    return res.status(429).json({ error: "Too many submissions. Please wait a minute." });
  try {
    const f = req.body;
    if (!f.name || !f.type || !f.date || !f.address)
      return res.status(400).json({ error: "Missing required fields" });
    const validTypes = ["protest","townhall","voting","nonprofit","online"];
    if (!validTypes.includes(f.type))
      return res.status(400).json({ error: "Invalid event type" });
    // Policy 9: Reject social-media-only source URLs — a primary verifiable source is required
    const SOCIAL_DOMAINS = ["twitter.com","x.com","facebook.com","fb.com","instagram.com","tiktok.com","threads.net"];
    const sourceUrlLower = (f.source || "").toLowerCase();
    if (sourceUrlLower && SOCIAL_DOMAINS.some(d => sourceUrlLower.includes(d))) {
      return res.status(400).json({
        error: "Please provide a primary source link (official website or event page) rather than a social media URL. COMN requires a verifiable source beyond social media.",
      });
    }
    const submitterEmail = (f.submitterEmail || "").trim().toLowerCase();
    const data = await airtable("", "POST", {
      records: [{ fields: {
        Name: f.name.trim().slice(0, 100),
        Type: f.type, Date: f.date, Time: f.time || "",
        Address: (f.address||"").trim().slice(0, 200),
        Latitude:    parseFloat(f.lat)  || 30.2672,
        Longitude:   parseFloat(f.lng)  || -97.7431,
        Description: (f.desc || "").trim().slice(0, 500),
        Source:      f.source || "",
        Status: "pending",
        SubmittedBy: f.submittedBy || "Public",
        SubmitterEmail: submitterEmail,
      }}]
    });
    // Notify admin of public submission
    if (RESEND_KEY) {
      const eventName = escHtml(f.name.trim().slice(0, 100));
      sendEmail(DIGEST_EMAIL, `📬 New public event submitted: ${f.name.trim()}`,
        `<p>A new civic event was submitted by a member of the public.</p>
         <p><strong>Name:</strong> ${eventName}<br>
         <strong>Type:</strong> ${escHtml(f.type)}<br>
         <strong>Date:</strong> ${escHtml(f.date)}<br>
         <strong>Address:</strong> ${escHtml(f.address||"")}<br>
         ${submitterEmail ? `<strong>Submitter email:</strong> ${escHtml(submitterEmail)}<br>` : ""}
         </p>
         <p><a href="${ADMIN_URL}">Review in admin panel →</a></p>`
      ).catch(() => {});
      // Confirmation to submitter
      if (submitterEmail && /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(submitterEmail)) {
        sendEmail(submitterEmail, `Your event "${f.name.trim()}" was submitted to COMN`,
          `<p>Thanks for submitting to COMN — Common Ground!</p>
           <p>Your event <strong>${eventName}</strong> is now under review by our team. We'll notify you when it's approved or if we need more information.</p>
           <p>Events are typically reviewed within 24 hours.</p>
           <p style="color:#888;font-size:12px">— The COMN Team | <a href="https://comnground.netlify.app">comnground.netlify.app</a></p>`
        ).catch(() => {});
      }
    }
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
  if (rateLimit(req.ip + "_login", 10))
    return res.status(429).json({ error: "Too many login attempts. Please wait a minute." });
  try {
    const { email, password } = req.body;
    if (!email || !password)
      return res.status(400).json({ error: "Email and password required" });
    const normalEmail = email.toLowerCase().trim();

    // Check account lockout
    if (checkLoginLockout(normalEmail))
      return res.status(429).json({ error: "Account temporarily locked due to too many failed attempts. Try again in 15 minutes." });

    const filter = encodeURIComponent(`{Email}='${normalEmail}'`);
    const data = await airtableAdmins(`?filterByFormula=${filter}`);
    const record = (data.records || [])[0];

    if (!record) {
      recordLoginFailure(normalEmail);
      return res.status(401).json({ error: "No account found with that email" });
    }
    if (record.fields.Role === "pending_approval")
      return res.status(403).json({ error: "Your account is pending approval from the Super Admin" });
    if (record.fields.Role === "invite_token") {
      recordLoginFailure(normalEmail);
      return res.status(401).json({ error: "No account found with that email" });
    }

    const valid = await bcrypt.compare(password, record.fields.PasswordHash || "");
    if (!valid) {
      recordLoginFailure(normalEmail);
      return res.status(401).json({ error: "Incorrect password" });
    }

    clearLoginFailures(normalEmail);
    const payload = {
      id:    record.id,
      email: record.fields.Email,
      name:  record.fields.DisplayName || record.fields.Email,
      role:  record.fields.Role || "admin",
    };
    const token = jwt.sign(payload, JWT_SECRET, { expiresIn: "14d" });
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
      const reason = (req.body.reason || "").trim().slice(0, 300);
      await airtable(`/${eventId}`, "PATCH", {
        fields: { Status: "rejected", RejectedBy: adminEmail, Approvers: "" }
      });
      console.log(`[REJECTED] ${eventId} by ${adminEmail}${reason ? ` — "${reason}"` : ""}`);
      // Email submitter if they left an email
      const submitterEmail = current.fields?.SubmitterEmail || "";
      if (submitterEmail && RESEND_KEY) {
        const evName = current.fields?.Name || "Your event";
        await sendEmail(submitterEmail, `Update on your COMN submission: "${evName}"`,
          `<p>Hi there,</p>
           <p>Thank you for submitting to COMN — Common Ground.</p>
           <p>Unfortunately, our team was unable to approve <strong>${escHtml(evName)}</strong> at this time.</p>
           ${reason ? `<p><strong>Reason:</strong> ${escHtml(reason)}</p>` : ""}
           <p>You're welcome to make changes and resubmit at <a href="https://comnground.netlify.app">comnground.netlify.app</a>.</p>
           <p style="color:#888;font-size:12px">— The COMN Team</p>`
        ).catch(() => {});
      }
      return res.json({ success: true, newStatus: "rejected" });
    }

    // APPROVE — Policy 12: Conflict-of-interest check
    // Admins may not approve events they submitted or events from their own organization
    const submitterEmail = (current.fields?.SubmitterEmail || "").toLowerCase();
    if (submitterEmail && submitterEmail === adminEmail.toLowerCase()) {
      return res.status(403).json({
        error: "Policy 12: You cannot approve an event you submitted yourself. Another admin must review this event.",
      });
    }
    const GENERIC_DOMAINS = ["gmail.com","yahoo.com","hotmail.com","outlook.com","icloud.com","protonmail.com","me.com","aol.com","live.com","msn.com"];
    const adminDomain     = (adminEmail.split("@")[1] || "").toLowerCase();
    const submitterDomain = (submitterEmail.split("@")[1] || "").toLowerCase();
    if (submitterEmail && adminDomain && submitterDomain && adminDomain === submitterDomain && !GENERIC_DOMAINS.includes(adminDomain)) {
      return res.status(403).json({
        error: `Policy 12: You share an organization domain (@${adminDomain}) with the event submitter. Another admin must approve this event to avoid a conflict of interest.`,
      });
    }

    // APPROVE — strip admin-internal notes before going live
    const rawDesc     = current.fields?.Description || "";
    const cleanDesc   = hasAdminNote(rawDesc) ? stripAdminNotes(rawDesc) : rawDesc;
    const descChanged = cleanDesc !== rawDesc;

    // Warn if event date is >90 days out
    const eventDate = current.fields?.Date || "";
    const ninetyDaysOut = new Date(); ninetyDaysOut.setDate(ninetyDaysOut.getDate() + 90);
    const farFuture = eventDate && new Date(eventDate) > ninetyDaysOut;

    if (adminRole === "super_admin") {
      // Super admin: instant live — they count as the full approval chain
      const patchFields = { Status: "live", Approvers: `${adminEmail} (super admin)`, RejectedBy: "" };
      if (descChanged) patchFields.Description = cleanDesc;
      await airtable(`/${eventId}`, "PATCH", { fields: patchFields });
      console.log(`[SUPER ADMIN → LIVE] ${eventId} by ${adminEmail}${descChanged ? " (admin notes stripped)" : ""}`);
      return res.json({
        success: true, newStatus: "live",
        message: "Event is now live!" + (descChanged ? " (Admin notes were stripped from description.)" : ""),
        farFuture,
      });
    }

    // Regular admin — two-step
    if (currentStatus === "pending") {
      const patchFields = { Status: "pending_2nd", Approvers: adminEmail };
      if (descChanged) patchFields.Description = cleanDesc;
      await airtable(`/${eventId}`, "PATCH", { fields: patchFields });
      console.log(`[1ST APPROVAL] ${eventId} by ${adminEmail} — awaiting 2nd`);
      return res.json({
        success: true, newStatus: "pending_2nd",
        message: "First approval done! A second admin must now approve before it goes live.",
        farFuture,
      });
    }

    if (currentStatus === "pending_2nd") {
      if (currentApprovers.includes(adminEmail))
        return res.status(400).json({
          error: "You already gave the first approval. A different admin must give the second.",
        });
      const allApprovers = [currentApprovers, adminEmail].filter(Boolean).join(", ");
      const patchFields = { Status: "live", Approvers: allApprovers };
      if (descChanged) patchFields.Description = cleanDesc;
      await airtable(`/${eventId}`, "PATCH", { fields: patchFields });
      console.log(`[2ND APPROVAL → LIVE] ${eventId}. Approvers: ${allApprovers}${descChanged ? " (admin notes stripped)" : ""}`);
      return res.json({
        success: true, newStatus: "live",
        message: "Event approved and now live!" + (descChanged ? " (Admin notes were stripped from description.)" : ""),
        farFuture,
      });
    }

    return res.status(400).json({ error: `Cannot approve event with status: ${currentStatus}` });

  } catch(e) {
    console.error("PATCH /admin/events/:id:", e.message);
    res.status(500).json({ error: "Failed to update event" });
  }
});

// Edit an event — resets status to pending (unless super_admin uses keepLive)
app.put("/admin/events/:id", requireAuth, async (req, res) => {
  try {
    const { name, type, date, time, address, desc, lat, lng, keepLive } = req.body;
    if (!name || !type || !date || !address)
      return res.status(400).json({ error: "name, type, date and address are required" });
    const validTypes = ["protest", "voting", "townhall", "nonprofit", "online"];
    if (!validTypes.includes(type))
      return res.status(400).json({ error: `type must be one of: ${validTypes.join(", ")}` });

    // Only super_admin may keep an event live while editing
    const usekeepLive = keepLive && req.admin.role === "super_admin";

    const fields = {
      Name:        name.trim().slice(0, 100),
      Type:        type,
      Date:        date,
      Time:        time         || "",
      Address:     address.trim().slice(0, 200),
      Description: (desc || "").trim().slice(0, 500),
      Latitude:    parseFloat(lat)  || 30.2672,
      Longitude:   parseFloat(lng)  || -97.7431,
      Status:      usekeepLive ? "live" : "pending",
      Approvers:   usekeepLive ? `${req.admin.email} (super admin edit)` : "",
      RejectedBy:  "",
      SubmittedBy: `Edited by ${req.admin.name || req.admin.email}`,
    };
    await airtable(`/${req.params.id}`, "PATCH", { fields });
    const newStatus = usekeepLive ? "live" : "pending";
    console.log(`[EDITED] ${req.params.id} by ${req.admin.email} → ${newStatus}`);
    res.json({
      success: true, newStatus,
      message: usekeepLive ? "Event updated and kept live." : "Event updated and sent back for re-review.",
    });
  } catch(e) {
    console.error("PUT /admin/events/:id:", e.message);
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

// Dashboard badge counts
app.get("/admin/stats", requireAuth, async (req, res) => {
  try {
    const statuses = ["pending", "pending_2nd", "live", "rejected", "duplicate"];
    const counts = {};
    await Promise.all(statuses.map(async s => {
      const filter = encodeURIComponent(`{Status}='${s}'`);
      const data = await airtable(`?filterByFormula=${filter}&fields[]=Status`);
      counts[s] = (data.records || []).length;
    }));
    res.json(counts);
  } catch(e) {
    console.error("GET /admin/stats:", e.message);
    res.status(500).json({ error: "Failed to fetch stats" });
  }
});

// Manual scrape trigger
app.post("/admin/scrape-now", requireAuth, async (req, res) => {
  res.json({ message: "Scrape started — new events will appear in your queue shortly" });
  runScraper();
});

// ── BUG REPORTS ───────────────────────────────────────────────────────────────
// Public endpoint — no auth required. Emails William when users report issues.
app.post("/bug-report", async (req, res) => {
  if (rateLimit(req.ip + "_bug", 5))
    return res.status(429).json({ error: "Too many reports. Please wait a minute." });
  try {
    const { what, url, email } = req.body;
    if (!what || what.trim().length < 5)
      return res.status(400).json({ error: "Please describe the issue (at least 5 characters)" });
    if (RESEND_KEY) {
      const subject = "🐛 COMN Bug Report";
      const lines = [
        "A user submitted a bug report via COMN:",
        "",
        `What's wrong: ${what.trim()}`,
        url   ? `Event / URL: ${url.trim()}`      : null,
        email ? `Reporter email: ${email.trim()}` : null,
        "",
        `Submitted: ${new Date().toISOString()}`,
      ].filter(Boolean);
      const emailRes = await fetch("https://api.resend.com/emails", {
        method: "POST",
        headers: { "Authorization": `Bearer ${RESEND_KEY}`, "Content-Type": "application/json" },
        body: JSON.stringify({
          from: "COMN Bugs <onboarding@resend.dev>",
          to:   DIGEST_EMAIL,
          subject,
          text: lines.join("\n"),
        }),
      });
      if (!emailRes.ok) throw new Error("Email send failed");
    }
    console.log(`[BUG REPORT] "${what.trim().slice(0, 80)}"`);
    res.json({ success: true });
  } catch(e) {
    console.error("Bug report error:", e.message);
    res.status(500).json({ error: "Failed to send bug report" });
  }
});

app.listen(PORT, () => console.log(`COMN server on port ${PORT}`));
