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
const AT_SOURCES_URL = `https://api.airtable.com/v0/${AT_BASE}/Sources`;
const AT_HEADS = { "Authorization": `Bearer ${AT_TOKEN}`, "Content-Type": "application/json" };

const RESEND_KEY   = process.env.RESEND_KEY;
const DIGEST_EMAIL = process.env.DIGEST_EMAIL || "bywilliamcole@gmail.com";
const ADMIN_URL    = "https://comnground.net/admin.html";
const JWT_SECRET = process.env.JWT_SECRET || "comn-dev-jwt-secret-CHANGE-IN-PROD";
if (!process.env.JWT_SECRET) console.warn("[WARN] JWT_SECRET env var not set — using insecure default. Set it in Railway before going to production.");

// ── SOURCES REGISTRY ──────────────────────────────────────────────────────────
const SOURCES = [
  { id: "council",    label: "Austin City Council",          url: "https://www.austintexas.gov/fullcalendar",             type: "government", desc: "Austin city government meetings and public hearings" },
  { id: "voting",     label: "Travis County Voting",         url: "https://votetravis.gov",                               type: "government", desc: "Travis County elections and voting information" },
  { id: "mobilize",   label: "Mobilize.us",                  url: "https://api.mobilize.us/v1/events",                    type: "platform",   desc: "Civic engagement events near Austin (50-mile radius)" },
  { id: "handsoff",   label: "Hands Off Central TX",         url: "https://handsoffcentraltx.org/events",                 type: "nonprofit",  desc: "Austin-area advocacy and civic events" },
  { id: "ajc",        label: "Austin Justice Coalition",     url: "https://austinjustice.org/events/",                    type: "nonprofit",  desc: "Criminal justice and equity events" },
  { id: "lwv",        label: "LWV Austin",                   url: "https://lwvaustin.org/content.aspx?page_id=4001&club_id=334869", type: "nonprofit", desc: "League of Women Voters Austin civic education events" },
  { id: "austincal",  label: "City of Austin Open Calendar", url: "https://www.austintexas.gov/department/volunteer-city-austin", type: "government", desc: "All public city events, programs, and government meetings" },
  { id: "chronicle",  label: "Austin Chronicle Events",      url: "https://www.austinchronicle.com/events/",              type: "media",      desc: "Austin Chronicle calendar — civic events only (keyword filtered)" },
  { id: "eventbrite", label: "Eventbrite Austin",            url: "https://www.eventbrite.com/d/tx--austin/community/",   type: "platform",   desc: "Community and civic events on Eventbrite" },
  { id: "traviscc",   label: "Travis County Commissioners",  url: "https://www.traviscountytx.gov/commissioners-court",   type: "government", desc: "Travis County Commissioners Court meetings and hearings" },
  { id: "txleg",      label: "Texas Legislature",            url: "https://capitol.texas.gov/Committees/Committees.aspx", type: "government", desc: "Texas Legislature committee hearings and public sessions" },
  { id: "texastrib",  label: "Texas Tribune Events",         url: "https://www.texastribune.org/events/",                type: "media",      desc: "Texas Tribune civic journalism events, panels, and forums" },
  { id: "aisd",       label: "Austin ISD Board",             url: "https://www.austinisd.org/board/board-meetings-calendar", type: "government", desc: "Austin ISD Board of Trustees public meetings" },
  { id: "indivisible",label: "Indivisible Austin",           url: "https://indivisibleaustin.com/events/",               type: "nonprofit",  desc: "Indivisible Austin civic action and advocacy events" },
  { id: "movetx",     label: "Move TX",                      url: "https://movetx.org/events/",                          type: "nonprofit",  desc: "Move TX civic engagement and voter registration events" },
  { id: "workersdef", label: "Workers Defense Project",      url: "https://workersdefense.org/events/",                  type: "nonprofit",  desc: "Workers Defense Project labor rights events in Austin" },
  { id: "top",        label: "Texas Organizing Project",     url: "https://organizetexas.org/events/",                   type: "nonprofit",  desc: "Texas Organizing Project community organizing events" },
  { id: "tfn",        label: "Texas Freedom Network",        url: "https://tfn.org/events/",                             type: "nonprofit",  desc: "Texas Freedom Network civil liberties and education events" },
  { id: "tcdemocrats",label: "Travis County Democrats",      url: "https://www.mobilize.us/traviscountydems/",            type: "nonprofit",  desc: "Travis County Democratic Party meetings and civic events" },
  { id: "sunrise",    label: "Sunrise Austin",               url: "https://www.sunrisemovement.org/hubs/austin",         type: "nonprofit",  desc: "Sunrise Movement Austin climate action events" },
  { id: "txcivil",    label: "TX Civil Rights Project",      url: "https://texascivilrightsproject.org/events/",         type: "nonprofit",  desc: "Texas Civil Rights Project advocacy events and workshops" },
  { id: "aclutx",     label: "ACLU Texas",                   url: "https://www.aclutx.org/events",                       type: "nonprofit",  desc: "ACLU of Texas civil liberties events and advocacy actions" },
  { id: "do512",      label: "Do512 Civic Calendar",         url: "https://do512.com/events/category/civic",             type: "media",      desc: "Do512 Austin community calendar — civic and community events" },
  { id: "luma",       label: "Luma Austin Events",           url: "https://lu.ma/austin",                                type: "platform",   desc: "Luma platform — Austin civic and community organizing events" },
];
// Runtime tracking: updated after each scrape run
const scrapeResults = {}; // id → { lastRun, found, error, durationMs }

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

app.use(cors({ origin: ["https://comnground.net", "https://comn-server-production.up.railway.app", "http://localhost:3000", "http://localhost:8080"] }));
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
function sixMonthsFromNow() {
  const d = new Date();
  d.setMonth(d.getMonth() + 6);
  return d.toISOString().split("T")[0];
}
function inWindow(d) { return d && d >= today() && d <= sixMonthsFromNow(); }
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

async function airtableSources(path, method = "GET", body = null) {
  const opts = { method, headers: AT_HEADS };
  if (body) opts.body = JSON.stringify(body);
  const res  = await fetch(`${AT_SOURCES_URL}${path}`, opts);
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
      headers: { "User-Agent": "COMN-Civic-Bot/1.0 (comnground.net)" },
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
  const events = [];
  const seen = new Set();

  // METHOD 1: Official 2026 City Council index page (confirmed working)
  try {
    const years = ["2026", "2027"];
    for (const yr of years) {
      const html = await fetchHTML(`https://www.austintexas.gov/department/city-council/${yr}/${yr}_council_index.htm`);
      if (!html) continue;
      // Match dates like "March 12, 2026" or "April 10 2026"
      const re = /(January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},?\s*20(26|27)/gi;
      let m;
      while ((m = re.exec(html)) !== null) {
        const date = parseDate(m[0]);
        if (!date || !inWindow(date) || seen.has(date)) continue;
        seen.add(date);
        // Try to extract meeting type from surrounding context
        const ctx = html.slice(Math.max(0, m.index - 200), m.index + 200);
        const isWork = /work session/i.test(ctx);
        const isSpecial = /special/i.test(ctx);
        const meetingName = isSpecial ? "Austin City Council Special Meeting" :
                            isWork    ? "Austin City Council Work Session" :
                                        "Austin City Council Regular Meeting";
        events.push({
          name: meetingName,
          type: "townhall", date, time: isWork ? "09:00" : "10:00",
          address: "Austin City Hall, 301 W. Second Street, Austin, TX 78701",
          lat: 30.2636, lng: -97.7466,
          desc: "Public meeting of the Austin City Council. Register to speak in-person or by phone at austintexas.gov before the meeting. Meetings are also streamed live.",
          source: `https://www.austintexas.gov/department/city-council/${yr}/${yr}_council_index.htm`,
        });
      }
    }
    if (events.length > 0) {
      console.log(`[City Council] Found ${events.length} meetings from index page`);
      return events;
    }
  } catch(e) { console.warn("[City Council index]", e.message); }

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
    const nowTs = Math.floor(Date.now() / 1000);
    const res = await fetch(
      `https://api.mobilize.us/v1/events?zipcode=78701&radius=50&timeslot_start_min=${nowTs}&per_page=150&visibility=PUBLIC&order_by=timeslot_start`,
      { headers: { "User-Agent": "COMN-Civic-Bot/1.0" }, signal: AbortSignal.timeout(15000) }
    );
    if (!res.ok) return [];
    const data = await res.json();
    // Use API data directly — no per-event deep-linking (was causing 100+ sequential fetches)
    return (data.data || [])
      .filter(e => e.timeslots?.length > 0)
      .map(e => {
        const ts = e.timeslots[0];
        const date = ts?.start_date ? new Date(ts.start_date * 1000).toISOString().split("T")[0] : null;
        const time = ts?.start_date ? new Date(ts.start_date * 1000).toTimeString().slice(0,5) : "10:00";
        const isVirtual = e.is_virtual || e.location?.is_virtual;
        const rawDesc = (e.description || "").replace(/<[^>]*>/g,"").replace(/\s+/g," ").trim();
        const name = (e.title || "Austin Civic Event").slice(0,100);
        return {
          name, type: detectEventType(name, rawDesc),
          date, time,
          address: isVirtual ? "Online" : (e.location?.venue || e.location?.address_lines?.[0] || "Austin, TX"),
          lat: isVirtual ? 0 : (parseFloat(e.location?.lat) || 30.2672),
          lng: isVirtual ? 0 : (parseFloat(e.location?.lon) || -97.7431),
          desc: rawDesc.slice(0,250),
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
      headers: { "User-Agent": "COMN-Civic-Bot/1.0 (comnground.net)" },
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
  for (let i = 0; i < Math.min(names.length, dates.length, 20); i++) {
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
  return events.slice(0, 15);
}

// ── CIVIC KEYWORD FILTER (for broad platform/media sources) ───────────────────
const CIVIC_KW = /\b(town hall|townhall|city council|county|public hearing|community meeting|community convening|community forum|community gathering|community summit|nonprofit|voter|voting|election|runoff|primary|rally|march|protest|vigil|civil rights|civic|neighborhood meeting|neighborhood|district|legislature|legislative|policy|advocacy|social justice|equity|climate|housing|public safety|candidate|political action|community org|volunteer|activism|ballot|commissioner|school board|community|convening|summit|forum|workshop|organizing|mutual aid|coalition|town meeting|open house|town hall meeting|public meeting|community event|gathering|action network)\b/i;

// ── CITY OF AUSTIN OPEN CALENDAR ──────────────────────────────────────────────
async function scrapeAustinCityCalendar() {
  try {
    const res = await fetch("https://www.austintexas.gov/calendar", {
      headers: { "User-Agent": "Mozilla/5.0 (compatible; COMN-Civic-Bot/1.0; +https://comnground.net)" },
      signal: AbortSignal.timeout(15000),
    });
    if (!res.ok) return [];
    const html = await res.text();
    const events = [];

    // Try JSON-LD structured data first (most reliable)
    const jsonLdRe = /<script[^>]+type="application\/ld\+json"[^>]*>([\s\S]*?)<\/script>/gi;
    let m;
    while ((m = jsonLdRe.exec(html)) !== null) {
      try {
        const raw = JSON.parse(m[1]);
        const items = Array.isArray(raw) ? raw : [raw];
        for (const item of items) {
          if (item["@type"] !== "Event") continue;
          const name = (item.name || "").trim();
          const startDate = item.startDate || "";
          const date = startDate.split("T")[0];
          const time = startDate.includes("T") ? startDate.split("T")[1].slice(0, 5) : "09:00";
          const desc = (item.description || "").replace(/<[^>]*>/g, "").slice(0, 250);
          const addr = item.location?.address?.streetAddress || item.location?.name || "Austin, TX";
          const src = item.url || "https://www.austintexas.gov/calendar";
          if (name && date && inWindow(date)) {
            events.push({ name: name.slice(0, 100), type: detectEventType(name, desc), date, time, address: addr, lat: 30.2672, lng: -97.7431, desc, source: src });
          }
        }
      } catch(e) {}
    }
    if (events.length > 0) return events.slice(0, 25);

    // HTML fallback: parse Drupal calendar view
    const eventLinkRe = /href="(https?:\/\/www\.austintexas\.gov\/[^"]*(?:event|calendar|meeting|hearing|program)[^"]*)"[^>]*>([^<]{5,100})</gi;
    const dateNearby = /(\d{4}-\d{2}-\d{2}|\b\w+\s+\d{1,2},?\s+\d{4})/;
    const seen = new Set();
    while ((m = eventLinkRe.exec(html)) !== null) {
      const src2 = m[1], name = m[2].trim();
      if (!name || seen.has(src2)) continue;
      seen.add(src2);
      const surrounding = html.slice(Math.max(0, m.index - 300), m.index + 500);
      const dm = dateNearby.exec(surrounding);
      const date = dm ? parseDate(dm[1]) : null;
      if (!date || !inWindow(date)) continue;
      events.push({ name: name.slice(0, 100), type: detectEventType(name, ""), date, time: "09:00", address: "Austin, TX", lat: 30.2672, lng: -97.7431, desc: "Austin city government event. See source for full details.", source: src2 });
    }
    return events.slice(0, 25);
  } catch(e) { console.error("[Austin City Calendar]", e.message); return []; }
}

// ── AUSTIN CHRONICLE CIVIC EVENTS ─────────────────────────────────────────────
async function scrapeAustinChronicle() {
  try {
    const res = await fetch("https://www.austinchronicle.com/events/", {
      headers: { "User-Agent": "Mozilla/5.0 (compatible; COMN-Civic-Bot/1.0)" },
      signal: AbortSignal.timeout(15000),
    });
    if (!res.ok) return [];
    const html = await res.text();
    const events = [];

    // Try JSON-LD first
    const jsonLdRe = /<script[^>]+type="application\/ld\+json"[^>]*>([\s\S]*?)<\/script>/gi;
    let m;
    while ((m = jsonLdRe.exec(html)) !== null) {
      try {
        const raw = JSON.parse(m[1]);
        const items = Array.isArray(raw) ? raw : [raw];
        for (const item of items) {
          if (item["@type"] !== "Event" && item["@type"] !== "MusicEvent") continue;
          const name = (item.name || "").trim();
          const desc = (item.description || "").replace(/<[^>]*>/g, "").slice(0, 250);
          if (!CIVIC_KW.test(name + " " + desc)) continue;
          const startDate = item.startDate || "";
          const date = startDate.split("T")[0];
          const time = startDate.includes("T") ? startDate.split("T")[1].slice(0, 5) : "19:00";
          const addr = item.location?.address?.streetAddress || item.location?.name || "Austin, TX";
          const src = item.url || "https://www.austinchronicle.com/events/";
          if (name && date && inWindow(date)) {
            events.push({ name: name.slice(0, 100), type: detectEventType(name, desc), date, time, address: addr, lat: 30.2672, lng: -97.7431, desc, source: src });
          }
        }
      } catch(e) {}
    }
    if (events.length > 0) return events.slice(0, 15);

    // HTML fallback: Chronicle event listing links
    const titleLinkRe = /href="(https?:\/\/www\.austinchronicle\.com\/events\/[^"]+)"[^>]*>([^<]{4,100})<\/a>/gi;
    const datePattern = /(\b(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun)[,.]?\s+)?(\w+\s+\d{1,2},?\s+\d{4})/i;
    const seen = new Set();
    while ((m = titleLinkRe.exec(html)) !== null) {
      const src2 = m[1], name = m[2].trim();
      if (!name || seen.has(src2)) continue;
      seen.add(src2);
      const surrounding = html.slice(m.index, m.index + 600);
      if (!CIVIC_KW.test(surrounding)) continue;
      const dm = datePattern.exec(surrounding);
      const date = dm ? parseDate(dm[2] || dm[0]) : null;
      if (!date || !inWindow(date)) continue;
      const descM = /class="[^"]*desc[^"]*"[^>]*>([\s\S]{0,200}?)<\//.exec(surrounding);
      const desc = descM ? descM[1].replace(/<[^>]*>/g, "").trim().slice(0, 250) : "";
      events.push({ name: name.slice(0, 100), type: detectEventType(name, desc), date, time: "19:00", address: "Austin, TX", lat: 30.2672, lng: -97.7431, desc, source: src2 });
    }
    return events.slice(0, 15);
  } catch(e) { console.error("[Austin Chronicle]", e.message); return []; }
}

// ── EVENTBRITE AUSTIN (CIVIC KEYWORD FILTERED) ────────────────────────────────
async function scrapeEventbriteAustin() {
  try {
    const pages = [
      "https://www.eventbrite.com/d/tx--austin/community/",
      "https://www.eventbrite.com/d/tx--austin/nonprofit--charity/",
      "https://www.eventbrite.com/d/tx--austin/government/",
    ];
    const events = [];
    const seenSrc = new Set();

    for (const pageUrl of pages) {
      try {
        const res = await fetch(pageUrl, {
          headers: { "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36" },
          signal: AbortSignal.timeout(15000),
        });
        if (!res.ok) continue;
        const html = await res.text();

        // Extract __SERVER_DATA__ using indexOf to avoid regex length limits
        const sdMarker = html.indexOf("window.__SERVER_DATA__");
        if (sdMarker !== -1) {
          const jsonStart = html.indexOf("{", sdMarker);
          const scriptEnd = html.indexOf("</script>", sdMarker);
          if (jsonStart !== -1 && scriptEnd !== -1 && jsonStart < scriptEnd) {
            // Strip trailing semicolon/whitespace before </script>
            const raw = html.slice(jsonStart, scriptEnd).replace(/;\s*$/, "").trim();
            try {
              const sd = JSON.parse(raw);
              const evList = sd?.search_data?.events?.results || [];
              for (const ev of evList) {
                // Eventbrite community pages are pre-filtered — no CIVIC_KW needed
                const name = (ev.name?.text || ev.name || ev.title || "").trim();
                const desc = (ev.summary || ev.description?.text || "").replace(/<[^>]*>/g, "").slice(0, 250);
                if (!name) continue;
                const startLocal = ev.start?.local || ev.start_date || "";
                const date = startLocal ? startLocal.split("T")[0] : null;
                const time = startLocal?.includes("T") ? startLocal.split("T")[1].slice(0, 5) : "18:00";
                if (!date || !inWindow(date)) continue;
                const isVirtual = ev.is_online_event || /\b(virtual|online|zoom|webinar)\b/i.test(name + " " + desc);
                const venue = ev.primary_venue || ev.venue || {};
                const addr = isVirtual ? "Online"
                  : (venue.address?.localized_address_display || venue.name || "Austin, TX");
                const src = ev.url || pageUrl;
                if (!seenSrc.has(src)) {
                  seenSrc.add(src);
                  events.push({
                    name: name.slice(0, 100), type: detectEventType(name, desc),
                    date, time, address: addr,
                    lat: isVirtual ? 0 : (parseFloat(venue.address?.latitude) || 30.2672),
                    lng: isVirtual ? 0 : (parseFloat(venue.address?.longitude) || -97.7431),
                    desc, source: src,
                  });
                }
              }
              console.log(`[Eventbrite ${pageUrl.split("/").slice(-2,-1)}] ${evList.length} raw → ${events.length} so far`);
            } catch(e) { console.warn("[Eventbrite] JSON parse error:", e.message.slice(0,60)); }
          }
        }
      } catch(e) { console.warn("[Eventbrite page]", e.message); }
    }
    return events.slice(0, 50);
  } catch(e) { console.error("[Eventbrite Austin]", e.message); return []; }
}

// ── TRAVIS COUNTY COMMISSIONERS COURT ─────────────────────────────────────────
async function scrapeTravisCountyCommissioners() {
  const events = [];
  const seen = new Set();

  // METHOD 1: Try CivicClerk portal (their new agenda system)
  try {
    const res = await fetch("https://traviscotx.portal.civicclerk.com/", {
      headers: { "User-Agent": "Mozilla/5.0 (compatible; COMN-Civic-Bot/1.0)" },
      signal: AbortSignal.timeout(12000),
    });
    if (res.ok) {
      const html = await res.text();
      // Look for date patterns in the agenda portal
      const dateRe = /(\d{1,2}\/\d{1,2}\/\d{4}|\d{4}-\d{2}-\d{2}|(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},?\s+\d{4})/gi;
      let m;
      while ((m = dateRe.exec(html)) !== null) {
        const date = parseDate(m[1]);
        if (!date || !inWindow(date) || seen.has(date)) continue;
        const ctx = html.slice(Math.max(0, m.index - 300), m.index + 300);
        if (!/commissioners|court|meeting|agenda/i.test(ctx)) continue;
        seen.add(date);
        events.push({
          name: "Travis County Commissioners Court Meeting",
          type: "townhall", date, time: "09:00",
          address: "700 Lavaca St, Austin, TX 78701",
          lat: 30.2695, lng: -97.7441,
          desc: "Travis County Commissioners Court meeting. Open to the public. Public comment period available.",
          source: "https://traviscotx.portal.civicclerk.com/",
        });
      }
      if (events.length > 0) {
        console.log(`[Travis CC] Found ${events.length} meetings from CivicClerk`);
        return events.slice(0, 26);
      }
    }
  } catch(e) { console.warn("[Travis CC CivicClerk]", e.message); }

  // METHOD 2: Try the main commissioners-court page
  try {
    const html = await fetchHTML("https://www.traviscountytx.gov/commissioners-court");
    if (html) {
      const dateRe = /(\d{1,2}\/\d{1,2}\/\d{4}|\d{4}-\d{2}-\d{2}|(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},?\s+\d{4})/gi;
      let m;
      while ((m = dateRe.exec(html)) !== null) {
        const date = parseDate(m[1]);
        if (!date || !inWindow(date) || seen.has(date)) continue;
        const ctx = html.slice(Math.max(0, m.index - 300), m.index + 300);
        if (!/commissioners|court|meeting|agenda|tuesday/i.test(ctx)) continue;
        seen.add(date);
        events.push({
          name: "Travis County Commissioners Court Meeting",
          type: "townhall", date, time: "09:00",
          address: "700 Lavaca St, Austin, TX 78701",
          lat: 30.2695, lng: -97.7441,
          desc: "Travis County Commissioners Court meeting. Open to the public. Public comment period available.",
          source: "https://www.traviscountytx.gov/commissioners-court",
        });
      }
      if (events.length > 0) return events.slice(0, 26);
    }
  } catch(e) { console.warn("[Travis CC main page]", e.message); }

  return events;
}

// ── WORDPRESS EVENTS CALENDAR HELPER ──────────────────────────────────────────
// Generic helper: tries The Events Calendar REST API, then falls back to JSON-LD scraping.
async function scrapeWordPressEventsCal(siteUrl, label) {
  try {
    const apiUrl = siteUrl.replace(/\/$/, "") + "/wp-json/tribe/events/v1/events?per_page=50&status=publish";
    const res = await fetch(apiUrl, {
      headers: { "User-Agent": "COMN-Civic-Bot/1.0 (comnground.net)" },
      signal: AbortSignal.timeout(10000),
    });
    if (res.ok) {
      const data = await res.json();
      const evts = [];
      for (const ev of (data.events || [])) {
        const date = parseDate((ev.start_date || "").split(" ")[0]);
        if (!date || !inWindow(date)) continue;
        const time = (ev.start_date || "").split(" ")[1]?.slice(0, 5) || "10:00";
        const name = (ev.title || label + " Event")
          .replace(/&#(\d+);/g, (_, c) => String.fromCharCode(c))
          .replace(/&amp;/g, "&").replace(/&nbsp;/g, " ")
          .replace(/<[^>]*>/g, "").trim().slice(0, 100);
        const desc = ((ev.description || ev.excerpt || "")
          .replace(/<[^>]*>/g, "").replace(/\s+/g, " ").trim()).slice(0, 250);
        const venue = ev.venue;
        const addr = venue
          ? `${venue.venue || ""} ${venue.address || ""} ${venue.city || "Austin"}, TX`.trim().replace(/\s+/g, " ")
          : "Austin, TX";
        evts.push({
          name, type: detectEventType(name, desc),
          date, time, address: addr,
          lat: parseFloat(venue?.geo_lat) || 30.2672,
          lng: parseFloat(venue?.geo_lng) || -97.7431,
          desc, source: ev.url || siteUrl,
        });
      }
      if (evts.length > 0) return evts;
    }
  } catch(e) { /* fall through to HTML */ }
  // Fallback: JSON-LD in page HTML
  try {
    const html = await fetchHTML(siteUrl);
    if (!html) return [];
    const evts = [];
    const ldRe = /<script[^>]+type="application\/ld\+json"[^>]*>([\s\S]*?)<\/script>/gi;
    let m;
    while ((m = ldRe.exec(html)) !== null) {
      try {
        const raw = JSON.parse(m[1]);
        const items = Array.isArray(raw) ? raw : (raw["@graph"] ? raw["@graph"] : [raw]);
        for (const item of items) {
          if (item["@type"] !== "Event") continue;
          const name = (item.name || "").replace(/<[^>]*>/g, "").trim().slice(0, 100);
          const desc = (item.description || "").replace(/<[^>]*>/g, "").replace(/\s+/g, " ").trim().slice(0, 250);
          if (!name) continue;
          const start = item.startDate || "";
          const date = parseDate(start.split("T")[0]);
          if (!date || !inWindow(date)) continue;
          const time = start.includes("T") ? start.split("T")[1].slice(0, 5) : "10:00";
          const addr = item.location?.address?.streetAddress || item.location?.name || "Austin, TX";
          evts.push({
            name, type: detectEventType(name, desc),
            date, time, address: addr,
            lat: 30.2672, lng: -97.7431, desc, source: item.url || siteUrl,
          });
        }
      } catch(e2) {}
    }
    return evts;
  } catch(e) { return []; }
}

// ── TEXAS LEGISLATURE COMMITTEE HEARINGS ──────────────────────────────────────
async function scrapeTXLegislature() {
  try {
    const events = [];
    // Try XML hearing notice feed for current session (89th Legislature)
    // Try different session codes for the XML feed
    let xmlText = await fetchHTML("https://capitol.texas.gov/tlodocs/89R/schedulexml/hearing.xml");
    if (!xmlText) xmlText = await fetchHTML("https://capitol.texas.gov/tlodocs/88R/schedulexml/hearing.xml");
    if (!xmlText) xmlText = await fetchHTML("https://capitol.texas.gov/tlodocs/892/schedulexml/hearing.xml");
    if (xmlText) {
      const hearingRe = /<hearing[^>]*>([\s\S]*?)<\/hearing>/gi;
      const seen = new Set();
      let m;
      while ((m = hearingRe.exec(xmlText)) !== null) {
        const chunk = m[1];
        const dateM = chunk.match(/<date[^>]*>(.*?)<\/date>/i);
        const timeM = chunk.match(/<time[^>]*>(.*?)<\/time>/i);
        const commM  = chunk.match(/<committee[^>]*>(.*?)<\/committee>/i);
        const roomM  = chunk.match(/<room[^>]*>(.*?)<\/room>/i);
        if (!dateM) continue;
        const date = parseDate(dateM[1]);
        if (!date || !inWindow(date)) continue;
        const committee = (commM?.[1] || "Committee").trim().replace(/&amp;/g, "&");
        const key = `${committee}__${date}`;
        if (seen.has(key)) continue;
        seen.add(key);
        const room = (roomM?.[1] || "").trim();
        events.push({
          name: `TX Legislature: ${committee}`.slice(0, 100),
          type: "townhall", date,
          time: timeM?.[1]?.slice(0, 5) || "09:00",
          address: room ? `Texas State Capitol, ${room}, Austin, TX` : "Texas State Capitol, 1100 Congress Ave, Austin, TX 78701",
          lat: 30.2747, lng: -97.7404,
          desc: `Texas Legislature committee hearing — ${committee}. Open to public testimony.`,
          source: "https://capitol.texas.gov/Committees/Committees.aspx",
        });
      }
      if (events.length > 0) return events.slice(0, 15);
    }
    // Fallback: scrape the Committees page for date patterns
    const html = await fetchHTML("https://capitol.texas.gov/Committees/Committees.aspx");
    if (!html) return [];
    const dateRe = /(January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},?\s*202[67]/gi;
    const seen2 = new Set();
    let m2;
    while ((m2 = dateRe.exec(html)) !== null) {
      const date = parseDate(m2[0]);
      if (!date || !inWindow(date) || seen2.has(date)) continue;
      const ctx = html.slice(Math.max(0, m2.index - 300), m2.index + 300);
      if (!/committee|hearing|session/i.test(ctx)) continue;
      seen2.add(date);
      events.push({
        name: "Texas Legislature Committee Hearing",
        type: "townhall", date, time: "09:00",
        address: "Texas State Capitol, 1100 Congress Ave, Austin, TX 78701",
        lat: 30.2747, lng: -97.7404,
        desc: "Texas Legislature public committee hearing. Open to the public — Austin, TX.",
        source: "https://capitol.texas.gov/Committees/Committees.aspx",
      });
    }
    return events.slice(0, 10);
  } catch(e) { console.warn("[TX Legislature]", e.message); return []; }
}

// ── TEXAS TRIBUNE EVENTS ───────────────────────────────────────────────────────
async function scrapeTexasTribune() {
  try {
    const html = await fetchHTML("https://www.texastribune.org/events/");
    if (!html) return [];
    const events = [];
    const ldRe = /<script[^>]+type="application\/ld\+json"[^>]*>([\s\S]*?)<\/script>/gi;
    let m;
    while ((m = ldRe.exec(html)) !== null) {
      try {
        const raw = JSON.parse(m[1]);
        const items = Array.isArray(raw) ? raw : (raw["@graph"] ? raw["@graph"] : [raw]);
        for (const item of items) {
          if (item["@type"] !== "Event") continue;
          const name = (item.name || "").replace(/<[^>]*>/g, "").trim().slice(0, 100);
          const desc = (item.description || "").replace(/<[^>]*>/g, "").replace(/\s+/g, " ").trim().slice(0, 250);
          if (!name) continue;
          const start = item.startDate || "";
          const date = parseDate(start.split("T")[0]);
          if (!date || !inWindow(date)) continue;
          const time = start.includes("T") ? start.split("T")[1].slice(0, 5) : "18:00";
          const addr = item.location?.address?.streetAddress || item.location?.name || "Austin, TX";
          events.push({
            name, type: detectEventType(name, desc),
            date, time, address: addr,
            lat: 30.2672, lng: -97.7431, desc,
            source: item.url || "https://www.texastribune.org/events/",
          });
        }
      } catch(e2) {}
    }
    // HTML fallback: look for event links
    if (events.length === 0) {
      const linkRe = /<a[^>]+href="(https:\/\/www\.texastribune\.org\/events\/[^"]+)"[^>]*>([\s\S]*?)<\/a>/gi;
      let m2;
      while ((m2 = linkRe.exec(html)) !== null) {
        const srcUrl = m2[1];
        const rawName = m2[2].replace(/<[^>]*>/g, "").trim().slice(0, 100);
        if (!rawName || rawName.length < 5) continue;
        const d = await fetchEventDetails(srcUrl);
        if (!d.date || !inWindow(d.date)) continue;
        events.push({
          name: rawName, type: detectEventType(rawName, d.desc || ""),
          date: d.date, time: d.time || "18:00",
          address: d.address || "Austin, TX",
          lat: d.lat || 30.2672, lng: d.lng || -97.7431,
          desc: d.desc || "", source: srcUrl,
        });
        if (events.length >= 8) break;
      }
    }
    return events.slice(0, 10);
  } catch(e) { console.warn("[Texas Tribune]", e.message); return []; }
}

// ── AUSTIN ISD BOARD OF TRUSTEES ──────────────────────────────────────────────
async function scrapeAISDBoard() {
  const events = [];
  const seen = new Set();

  // METHOD 1: Try several possible AISD board calendar URLs
  const aisdUrls = [
    "https://www.austinisd.org/board",
    "https://www.austinisd.org/board-of-trustees",
    "https://www.austinisd.org/about/leadership/board-of-trustees",
  ];
  for (const url of aisdUrls) {
    try {
      const html = await fetchHTML(url);
      if (!html) continue;
      const dateRe = /(January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},?\s*202[67]/gi;
      let m;
      while ((m = dateRe.exec(html)) !== null) {
        const date = parseDate(m[0]);
        if (!date || !inWindow(date) || seen.has(date)) continue;
        const ctx = html.slice(Math.max(0, m.index - 300), m.index + 300);
        if (!/board|trustee|meeting|agenda/i.test(ctx)) continue;
        seen.add(date);
        events.push({
          name: "Austin ISD Board of Trustees Meeting",
          type: "townhall", date, time: "17:30",
          address: "Austin ISD, 1111 West 6th Street, Austin, TX 78703",
          lat: 30.2710, lng: -97.7577,
          desc: "Regular public meeting of the Austin ISD Board of Trustees. Public comment available. Check austinisd.org for agenda.",
          source: url,
        });
      }
      if (events.length > 0) {
        console.log(`[AISD Board] Found ${events.length} meetings from ${url}`);
        return events.slice(0, 10);
      }
    } catch(e) { /* try next */ }
  }

  return events;
}

// ── AUSTIN PLANNING COMMISSION ─────────────────────────────────────────────────
async function scrapeAustinPlanningCommission() {
  const events = [];
  const seen = new Set();

  // Try Austin Planning Commission pages
  const urls = [
    "https://www.austintexas.gov/department/planning-commission",
    "https://www.austintexas.gov/cityclerk/boards_commissions/meetingschedule/120.htm",
    "https://austintexas.gov/planningcommission",
  ];
  for (const url of urls) {
    try {
      const html = await fetchHTML(url);
      if (!html) continue;
      const dateRe = /(January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},?\s*202[67]/gi;
      let m;
      while ((m = dateRe.exec(html)) !== null) {
        const date = parseDate(m[0]);
        if (!date || !inWindow(date) || seen.has(date)) continue;
        const ctx = html.slice(Math.max(0, m.index - 300), m.index + 300);
        if (!/planning|commission|meeting|agenda/i.test(ctx)) continue;
        seen.add(date);
        events.push({
          name: "Austin Planning Commission Meeting",
          type: "townhall", date, time: "18:00",
          address: "Austin City Hall, 301 W. Second Street, Austin, TX 78701",
          lat: 30.2636, lng: -97.7466,
          desc: "Austin Planning Commission meeting. Reviews land use, zoning, and development applications. Public comment welcome.",
          source: url,
        });
      }
      if (events.length > 0) return events.slice(0, 12);
    } catch(e) { /* try next */ }
  }

  return events;
}



// ── INDIVISIBLE AUSTIN ─────────────────────────────────────────────────────────
async function scrapeIndivisibleAustin() {
  return scrapeWordPressEventsCal("https://indivisibleaustin.com/events/", "Indivisible Austin");
}

// ── MOVE TX ────────────────────────────────────────────────────────────────────
async function scrapeMoveTX() {
  return scrapeWordPressEventsCal("https://movetx.org/events/", "Move TX");
}

// ── WORKERS DEFENSE PROJECT ────────────────────────────────────────────────────
async function scrapeWorkersDefense() {
  return scrapeWordPressEventsCal("https://workersdefense.org/events/", "Workers Defense Project");
}

// ── TEXAS ORGANIZING PROJECT ───────────────────────────────────────────────────
async function scrapeTexasOrganizing() {
  return scrapeWordPressEventsCal("https://organizetexas.org/events/", "Texas Organizing Project");
}

// ── TEXAS FREEDOM NETWORK ──────────────────────────────────────────────────────
async function scrapeTexasFreedomNetwork() {
  return scrapeWordPressEventsCal("https://tfn.org/events/", "Texas Freedom Network");
}

// ── TRAVIS COUNTY DEMOCRATS ────────────────────────────────────────────────────
async function scrapeTravisCountyDems() {
  try {
    // travisdemocrats.org domain is down — use Mobilize org API (org ID 903046)
    const nowTs = Math.floor(Date.now() / 1000);
    const res = await fetch(
      `https://api.mobilize.us/v1/organizations/903046/events?timeslot_start_min=${nowTs}&per_page=50&visibility=PUBLIC`,
      { headers: { "User-Agent": "COMN-Civic-Bot/1.0" }, signal: AbortSignal.timeout(10000) }
    );
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const data = await res.json();
    return (data.data || [])
      .filter(e => e.timeslots?.length > 0)
      .map(e => {
        const ts = e.timeslots[0];
        const date = ts?.start_date ? new Date(ts.start_date * 1000).toISOString().split("T")[0] : null;
        const time = ts?.start_date ? new Date(ts.start_date * 1000).toTimeString().slice(0,5) : "18:00";
        const rawDesc = (e.description || "").replace(/<[^>]*>/g,"").replace(/\s+/g," ").trim();
        const name = (e.title || "Travis County Democrats Event").slice(0,100);
        return {
          name, type: detectEventType(name, rawDesc),
          date, time,
          address: e.location?.venue || e.location?.address_lines?.[0] || "Austin, TX",
          lat: parseFloat(e.location?.lat) || 30.2672,
          lng: parseFloat(e.location?.lon) || -97.7431,
          desc: rawDesc.slice(0,250),
          source: e.browser_url || "https://www.mobilize.us/traviscountydems/",
        };
      })
      .filter(e => e.date && inWindow(e.date));
  } catch(e) {
    console.warn("[Travis County Dems]", e.message);
    return [];
  }
}

// ── SUNRISE AUSTIN ─────────────────────────────────────────────────────────────
async function scrapeSunriseAustin() {
  // Sunrise chapters may host events on sunrisemovement.org or their own ActionNetwork page
  const evts = await scrapeWordPressEventsCal("https://www.sunrisemovement.org/hubs/austin", "Sunrise Austin");
  if (evts.length > 0) return evts;
  return scrapeWordPressEventsCal("https://austintx.sunrisemovement.org", "Sunrise Austin");
}

// ── TEXAS CIVIL RIGHTS PROJECT ────────────────────────────────────────────────
async function scrapeTexasCivilRights() {
  return scrapeWordPressEventsCal("https://texascivilrightsproject.org/events/", "TX Civil Rights Project");
}

// ── ACLU OF TEXAS ─────────────────────────────────────────────────────────────
async function scrapeACLUTexas() {
  try {
    const html = await fetchHTML("https://www.aclutx.org/events");
    if (!html) return [];
    const events = [];
    const ldRe = /<script[^>]+type="application\/ld\+json"[^>]*>([\s\S]*?)<\/script>/gi;
    let m;
    while ((m = ldRe.exec(html)) !== null) {
      try {
        const raw = JSON.parse(m[1]);
        const items = Array.isArray(raw) ? raw : (raw["@graph"] ? raw["@graph"] : [raw]);
        for (const item of items) {
          if (item["@type"] !== "Event") continue;
          const name = (item.name || "").trim().slice(0, 100);
          const desc = (item.description || "").replace(/<[^>]*>/g, "").trim().slice(0, 250);
          if (!name) continue;
          const start = item.startDate || "";
          const date = parseDate(start.split("T")[0]);
          if (!date || !inWindow(date)) continue;
          const time = start.includes("T") ? start.split("T")[1].slice(0, 5) : "18:00";
          const addr = item.location?.address?.streetAddress || item.location?.name || "Austin, TX";
          events.push({
            name, type: detectEventType(name, desc),
            date, time, address: addr,
            lat: 30.2672, lng: -97.7431, desc,
            source: item.url || "https://www.aclutx.org/events",
          });
        }
      } catch(e2) {}
    }
    if (events.length > 0) return events.slice(0, 10);
    return scrapeWordPressEventsCal("https://www.aclutx.org", "ACLU Texas");
  } catch(e) { console.warn("[ACLU Texas]", e.message); return []; }
}

// ── DO512 CIVIC CALENDAR ──────────────────────────────────────────────────────
async function scrapeDo512() {
  try {
    const html = await fetchHTML("https://do512.com/events/category/civic");
    if (!html) return [];
    const events = [];
    const ldRe = /<script[^>]+type="application\/ld\+json"[^>]*>([\s\S]*?)<\/script>/gi;
    let m;
    while ((m = ldRe.exec(html)) !== null) {
      try {
        const raw = JSON.parse(m[1]);
        const items = Array.isArray(raw) ? raw : (raw["@graph"] ? raw["@graph"] : [raw]);
        for (const item of items) {
          if (item["@type"] !== "Event") continue;
          const name = (item.name || "").replace(/<[^>]*>/g, "").trim().slice(0, 100);
          const desc = (item.description || "").replace(/<[^>]*>/g, "").replace(/\s+/g, " ").trim().slice(0, 250);
          if (!name || !CIVIC_KW.test(name + " " + desc)) continue;
          const start = item.startDate || "";
          const date = parseDate(start.split("T")[0]);
          if (!date || !inWindow(date)) continue;
          const time = start.includes("T") ? start.split("T")[1].slice(0, 5) : "19:00";
          const addr = item.location?.address?.streetAddress || item.location?.name || "Austin, TX";
          events.push({
            name, type: detectEventType(name, desc),
            date, time, address: addr,
            lat: 30.2672, lng: -97.7431, desc,
            source: item.url || "https://do512.com/events/category/civic",
          });
        }
      } catch(e2) {}
    }
    return events.slice(0, 10);
  } catch(e) { console.warn("[Do512]", e.message); return []; }
}

// ── LUMA AUSTIN CIVIC EVENTS ──────────────────────────────────────────────────
async function scrapeLumaAustin() {
  try {
    // Luma embeds event data in __NEXT_DATA__ or JSON-LD
    const html = await fetchHTML("https://lu.ma/austin");
    if (!html) return [];
    const events = [];
    // Try JSON-LD first
    const ldRe = /<script[^>]+type="application\/ld\+json"[^>]*>([\s\S]*?)<\/script>/gi;
    let m;
    while ((m = ldRe.exec(html)) !== null) {
      try {
        const raw = JSON.parse(m[1]);
        const items = Array.isArray(raw) ? raw : (raw["@graph"] ? raw["@graph"] : [raw]);
        for (const item of items) {
          if (item["@type"] !== "Event") continue;
          const name = (item.name || "").replace(/<[^>]*>/g, "").trim().slice(0, 100);
          const desc = (item.description || "").replace(/<[^>]*>/g, "").replace(/\s+/g, " ").trim().slice(0, 250);
          if (!name || !CIVIC_KW.test(name + " " + desc)) continue;
          const start = item.startDate || "";
          const date = parseDate(start.split("T")[0]);
          if (!date || !inWindow(date)) continue;
          const time = start.includes("T") ? start.split("T")[1].slice(0, 5) : "18:00";
          events.push({
            name, type: detectEventType(name, desc),
            date, time,
            address: item.location?.address?.streetAddress || item.location?.name || "Austin, TX",
            lat: 30.2672, lng: -97.7431, desc,
            source: item.url || "https://lu.ma/austin",
          });
        }
      } catch(e2) {}
    }
    // Try __NEXT_DATA__ embedded JSON
    if (events.length === 0) {
      const nd = html.match(/<script id="__NEXT_DATA__"[^>]*>([\s\S]*?)<\/script>/i);
      if (nd) {
        try {
          const data = JSON.parse(nd[1]);
          const evList = data?.props?.pageProps?.events || data?.props?.pageProps?.initialEvents || [];
          for (const ev of evList) {
            const name = (ev.name || ev.title || "").trim().slice(0, 100);
            const desc = (ev.description || "").replace(/<[^>]*>/g, "").replace(/\s+/g, " ").trim().slice(0, 250);
            if (!name || !CIVIC_KW.test(name + " " + desc)) continue;
            const date = parseDate(ev.start_at || ev.startDate || "");
            if (!date || !inWindow(date)) continue;
            const time = (ev.start_at || "").includes("T") ? ev.start_at.split("T")[1].slice(0, 5) : "18:00";
            events.push({
              name, type: detectEventType(name, desc),
              date, time,
              address: ev.geo_address_info?.full_address || ev.location || "Austin, TX",
              lat: parseFloat(ev.lat) || 30.2672,
              lng: parseFloat(ev.lng) || -97.7431,
              desc, source: ev.url ? `https://lu.ma/${ev.url}` : "https://lu.ma/austin",
            });
          }
        } catch(e2) {}
      }
    }
    return events.slice(0, 10);
  } catch(e) { console.warn("[Luma Austin]", e.message); return []; }
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

// ── PUSH SCRAPE RESULTS TO AIRTABLE SOURCES TABLE ────────────────────────────
// Hardcoded source-id → Airtable record-id map (avoids fragile URL matching)
const SOURCE_RECORD_MAP = {
  council:    "recOXeJLmJ1HBryOi",
  voting:     "recyanqHetlni9sFI",
  mobilize:   "recSkk3f47j2ZoHQZ",
  handsoff:   "rec8mZ5fSIdoaK4lw",
  ajc:        "recAGWJ8wyAcZHQVP",
  lwv:        "reczgm6pO2eNKVom3",
  austincal:  "recbUzCtB90LNk65N",
  chronicle:  "recGgy1uN3Yluu50X",
  eventbrite: "recIUzhudwtJ7NGrg",
  traviscc:   "recDwNPrueswMTVrq",
  txleg:      "rec4jgqEHRgnWP2ee",
  texastrib:  "recCLCEufN7wNZ5XS",
  aisd:       "recbUB6jQVc7Cggtw",
  planningcomm: null, // not yet in Airtable Sources table
  indivisible:"recRkWeNkwRnpd4pc",
  movetx:     "recpLbLmItIyz5KPL",
  workersdef: "recy1F4sv18fDmFgr",
  top:        "recYwvS5QrIUeCNJx",
  tfn:        "recZdczNabHsoQSED",
  tcdemocrats:"rec7iJqQjOsnRSsL1",
  sunrise:    "recCjMA9MYEnfDGeE",
  txcivil:    "recpugTKJNUscynzI",
  aclutx:     "recpqr6BLTUAnqaOM",
  do512:      "reckFuPzVm7PiKrdx",
  luma:       "recuG8dlZ13mYikQ0",
};

async function updateSourcesAirtable() {
  try {
    const updates = [];
    for (const [id, recordId] of Object.entries(SOURCE_RECORD_MAP)) {
      if (!recordId) continue;
      const result = scrapeResults[id];
      if (!result) continue;
      updates.push({
        id: recordId,
        fields: {
          "Last Run":     result.lastRun,
          "Events Found": result.found,
          "Last Error":   result.error || null,
        }
      });
    }
    // Airtable PATCH accepts up to 10 records per request
    for (let i = 0; i < updates.length; i += 10) {
      await airtableSources("", "PATCH", { records: updates.slice(i, i + 10) });
    }
    console.log(`✅ Updated ${updates.length} Sources records in Airtable`);
  } catch(e) {
    console.error("Failed to update Sources in Airtable:", e.message);
  }
}

// ── MAIN SCRAPE ───────────────────────────────────────────────────────────────
async function runScraper() {
  console.log(`[${new Date().toISOString()}] Starting scrape...`);
  const scrapeErrors = [];
  const perSource = {};
  const typeCounts = {};

  // Wrap each scraper with per-source error tracking
  async function runSource(id, label, fn) {
    const t = Date.now();
    try {
      const results = await fn();
      perSource[label] = results.length;
      for (const e of results) typeCounts[e.type] = (typeCounts[e.type] || 0) + 1;
      scrapeResults[id] = { lastRun: new Date().toISOString(), found: results.length, error: null, durationMs: Date.now() - t };
      return results;
    } catch(e) {
      console.error(`[${label}] scraper error:`, e.message);
      scrapeErrors.push(`${label} — ${e.message.slice(0, 120)}`);
      perSource[label] = 0;
      scrapeResults[id] = { lastRun: new Date().toISOString(), found: 0, error: e.message.slice(0, 120), durationMs: Date.now() - t };
      return [];
    }
  }

  try {
    const t0 = Date.now();
    const [council, voting, mobilize, handsOff, ajc, lwv, austinCal, chronicle, eventbrite, travisCC,
           txLeg, texasTrib, aisd, planningComm, indivisible, moveTX, workersDef, texasOrg, tfn, tcDems, sunrise, txCivil, acluTX, do512, luma] = await Promise.all([
      runSource("council",      "City Council",                   scrapeCouncil),
      runSource("voting",       "Travis County Voting",           scrapeVoting),
      runSource("mobilize",     "Mobilize",                       scrapeMobilize),
      runSource("handsoff",     "Hands Off Central TX",           scrapeHandsOff),
      runSource("ajc",          "Austin Justice Coalition",       scrapeAJC),
      runSource("lwv",          "LWV Austin",                     scrapeLWV),
      runSource("austincal",    "City of Austin Calendar",        scrapeAustinCityCalendar),
      runSource("chronicle",    "Austin Chronicle",               scrapeAustinChronicle),
      runSource("eventbrite",   "Eventbrite Austin",              scrapeEventbriteAustin),
      runSource("traviscc",     "Travis County Commissioners",    scrapeTravisCountyCommissioners),
      runSource("txleg",        "Texas Legislature",              scrapeTXLegislature),
      runSource("texastrib",    "Texas Tribune Events",           scrapeTexasTribune),
      runSource("aisd",         "Austin ISD Board",               scrapeAISDBoard),
      runSource("planningcomm", "Austin Planning Commission",     scrapeAustinPlanningCommission),
      runSource("indivisible",  "Indivisible Austin",             scrapeIndivisibleAustin),
      runSource("movetx",       "Move TX",                        scrapeMoveTX),
      runSource("workersdef",   "Workers Defense Project",        scrapeWorkersDefense),
      runSource("top",          "Texas Organizing Project",       scrapeTexasOrganizing),
      runSource("tfn",          "Texas Freedom Network",          scrapeTexasFreedomNetwork),
      runSource("tcdemocrats",  "Travis County Democrats",        scrapeTravisCountyDems),
      runSource("sunrise",      "Sunrise Austin",                 scrapeSunriseAustin),
      runSource("txcivil",      "TX Civil Rights Project",        scrapeTexasCivilRights),
      runSource("aclutx",       "ACLU Texas",                     scrapeACLUTexas),
      runSource("do512",        "Do512 Civic Calendar",           scrapeDo512),
      runSource("luma",         "Luma Austin Events",             scrapeLumaAustin),
    ]);
    const allFound = [...council, ...voting, ...mobilize, ...handsOff, ...ajc, ...lwv, ...austinCal, ...chronicle, ...eventbrite, ...travisCC,
                      ...txLeg, ...texasTrib, ...aisd, ...planningComm, ...indivisible, ...moveTX, ...workersDef, ...texasOrg, ...tfn, ...tcDems, ...sunrise, ...txCivil, ...acluTX, ...do512, ...luma];
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
    await updateSourcesAirtable();
    console.log(`[${new Date().toISOString()}] Scrape complete.`);
    return { found: allFound.length, newPending, newDupes, totalPending };
  } catch(e) {
    console.error("Scraper error:", e.message);
    return { error: e.message };
  }
}

// ── CRON: EVERY HOUR ─────────────────────────────────────────────────────────
let lastScrapeHour = "";
setInterval(() => {
  const now = new Date();
  const hourKey = now.toISOString().slice(0, 13); // "2026-03-12T15" — unique per hour
  if (hourKey !== lastScrapeHour) {
    lastScrapeHour = hourKey;
    const ctHour = (now.getUTCHours() - 5 + 24) % 24;
    console.log(`⏰ Hourly scrape — ${now.toISOString()} (${ctHour}:00 CT)`);
    runScraper();
  }
}, 60 * 1000);

// ══════════════════════════════════════════════════════════════════════════════
// ROUTES
// ══════════════════════════════════════════════════════════════════════════════

app.get("/", (req, res) =>
  res.json({ status: "COMN server running", nextScrape: "Hourly" })
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
           <p style="color:#888;font-size:12px">— The COMN Team | <a href="https://comnground.net">comnground.net</a></p>`
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
    res.json({ token, link: `https://comnground.net/admin.html?invite=${token}` });
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
    res.json({ token: newToken, link: `https://comnground.net/admin.html?invite=${newToken}` });
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
           <p>You're welcome to make changes and resubmit at <a href="https://comnground.net">comnground.net</a>.</p>
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

// ── SOURCES STATUS ────────────────────────────────────────────────────────────
app.get("/admin/sources", requireAuth, (req, res) => {
  const data = SOURCES.map(s => ({
    ...s,
    ...(scrapeResults[s.id] || { lastRun: null, found: null, error: null, durationMs: null }),
    status: !scrapeResults[s.id] ? "never_run"
          : scrapeResults[s.id].error ? "error"
          : "ok",
  }));
  res.json(data);
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
