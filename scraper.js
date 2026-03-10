// ─────────────────────────────────────────────────────────────────────────────
// COMN Daily Scraper — runs every day at 12 PM CT
// Checks sources for new events, adds to Airtable as pending, emails digest
// ─────────────────────────────────────────────────────────────────────────────

const { Resend } = require("resend");

const AT_TOKEN  = process.env.AT_TOKEN;
const AT_BASE   = process.env.AT_BASE;
const AT_TABLE  = "Events";
const AT_URL    = `https://api.airtable.com/v0/${AT_BASE}/${AT_TABLE}`;
const AT_HEADS  = { "Authorization": `Bearer ${AT_TOKEN}`, "Content-Type": "application/json" };

const DIGEST_TO = process.env.DIGEST_EMAIL || "bywilliamcole@gmail.com";
const ADMIN_URL = "https://comnground.netlify.app/admin.html";

const resend = new Resend(process.env.RESEND_KEY);

// ── SOURCES TO SCRAPE ────────────────────────────────────────────────────────
// Each source has a URL and a parse function that returns an array of event objects.
// We only scrape sources with clean, structured HTML or JSON data.

async function fetchHTML(url) {
  try {
    const res = await fetch(url, {
      headers: { "User-Agent": "COMN-Civic-Bot/1.0 (comnground.netlify.app)" },
      signal: AbortSignal.timeout(10000),
    });
    if (!res.ok) return null;
    return await res.text();
  } catch (e) {
    console.warn(`Failed to fetch ${url}:`, e.message);
    return null;
  }
}

// Parse dates like "March 12, 2026" or "2026-03-12"
function parseDate(str) {
  if (!str) return null;
  const d = new Date(str);
  if (!isNaN(d)) return d.toISOString().split("T")[0];
  return null;
}

// ── SOURCE SCRAPERS ──────────────────────────────────────────────────────────

// Austin City Council — meetings page
async function scrapeCouncil() {
  const html = await fetchHTML("https://www.austintexas.gov/department/city-council/2026/2026_council_index.htm");
  if (!html) return [];
  const events = [];
  // Match patterns like "March 12, 2026" followed by meeting type
  const datePattern = /(\w+ \d{1,2},\s*202[56])[^<]*?(Regular Meeting|Special Meeting|Work Session)/gi;
  let match;
  while ((match = datePattern.exec(html)) !== null) {
    const date = parseDate(match[1]);
    if (!date) continue;
    // Skip if in the past
    if (new Date(date) < new Date()) continue;
    events.push({
      name: `Austin City Council — ${match[2]}`,
      type: "townhall",
      date,
      time: "10:00",
      address: "Austin City Hall, 301 W 2nd St, Austin",
      lat: 30.2636, lng: -97.7466,
      desc: `${match[2]} of the Austin City Council. Public comment open to all residents.`,
      source: "https://www.austintexas.gov/department/city-council/2026/2026_council_index.htm",
    });
  }
  return events;
}

// Vote Travis — upcoming elections
async function scrapeVoting() {
  const html = await fetchHTML("https://votetravis.gov/current-election-information/current-election/");
  if (!html) return [];
  const events = [];
  // Look for election date patterns
  const datePattern = /(\w+ \d{1,2},\s*202[56])/gi;
  const seen = new Set();
  let match;
  while ((match = datePattern.exec(html)) !== null) {
    const date = parseDate(match[1]);
    if (!date || seen.has(date)) continue;
    if (new Date(date) < new Date()) continue;
    seen.add(date);
    events.push({
      name: "Travis County Election Day",
      type: "voting",
      date,
      time: "07:00",
      address: "Any Travis County Vote Center",
      lat: 30.2672, lng: -97.7431,
      desc: "Vote at any Travis County Vote Center 7 AM – 7 PM. Bring valid TX photo ID.",
      source: "https://votetravis.gov/current-election-information/current-election/",
    });
  }
  return events;
}

// Austin Monitor — civic news events
async function scrapeMonitor() {
  const html = await fetchHTML("https://austinmonitor.com/category/news/city-council/");
  if (!html) return [];
  const events = [];
  // Look for council meeting references with dates
  const pattern = /council.{0,30}(\w+ \d{1,2})/gi;
  let match;
  const seen = new Set();
  while ((match = pattern.exec(html)) !== null) {
    const raw = match[1] + ", 2026";
    const date = parseDate(raw);
    if (!date || seen.has(date)) continue;
    if (new Date(date) < new Date()) continue;
    seen.add(date);
    events.push({
      name: "Austin City Council Coverage — Austin Monitor",
      type: "townhall",
      date,
      time: "10:00",
      address: "Austin City Hall, 301 W 2nd St, Austin",
      lat: 30.2636, lng: -97.7466,
      desc: "Upcoming council meeting referenced in Austin Monitor coverage.",
      source: "https://austinmonitor.com/category/news/city-council/",
    });
  }
  return events;
}

// Hands Off Central TX — protests and rallies
async function scrapeHandsOff() {
  const html = await fetchHTML("https://www.handsoffcentraltx.org/events");
  if (!html) return [];
  const events = [];
  // Look for event title + date blocks
  const pattern = /(?:class="[^"]*(?:title|event-name)[^"]*"[^>]*>|<h[23][^>]*>)([^<]{5,80})<\/(?:h[23]|[a-z]+)>[\s\S]{0,300}?(\w+ \d{1,2},?\s*202[56])/gi;
  let match;
  while ((match = pattern.exec(html)) !== null) {
    const name = match[1].trim().replace(/&amp;/g, "&").replace(/&#\d+;/g, "");
    const date = parseDate(match[2]);
    if (!date || !name || name.length < 5) continue;
    if (new Date(date) < new Date()) continue;
    events.push({
      name,
      type: "protest",
      date,
      time: "10:00",
      address: "Texas State Capitol, 1100 Congress Ave, Austin",
      lat: 30.2747, lng: -97.7403,
      desc: `Event from Hands Off Central TX. Verify details at source.`,
      source: "https://www.handsoffcentraltx.org/events",
    });
  }
  return events.slice(0, 5); // cap at 5 per source
}

// Austin Justice Coalition
async function scrapeAJC() {
  const html = await fetchHTML("https://austinjustice.org/events/");
  if (!html) return [];
  const events = [];
  const pattern = /(?:class="[^"]*(?:tribe-event-url|tribe-event-name|entry-title)[^"]*"[^>]*>|<h[23][^>]*>)([^<]{5,80})<[\s\S]{0,500}?(\w+ \d{1,2},?\s*202[56])/gi;
  let match;
  while ((match = pattern.exec(html)) !== null) {
    const name = match[1].trim().replace(/&amp;/g, "&");
    const date = parseDate(match[2]);
    if (!date || !name || name.length < 5) continue;
    if (new Date(date) < new Date()) continue;
    events.push({
      name,
      type: "nonprofit",
      date,
      time: "18:00",
      address: "Huston-Tillotson University, 900 Chicon St, Austin",
      lat: 30.2620, lng: -97.7213,
      desc: "Event from Austin Justice Coalition. Verify details at source.",
      source: "https://austinjustice.org/events/",
    });
  }
  return events.slice(0, 5);
}

// Mobilize.us — Austin civic actions
async function scrapeMobilize() {
  try {
    // Mobilize has a public API
    const res = await fetch(
      "https://api.mobilize.us/v1/events?zipcode=78701&radius=25&timeslot_start=now&per_page=10",
      { headers: { "User-Agent": "COMN-Civic-Bot/1.0" }, signal: AbortSignal.timeout(10000) }
    );
    if (!res.ok) return [];
    const data = await res.json();
    return (data.data || []).slice(0, 8).map(e => ({
      name: e.title || "Austin Civic Event",
      type: "nonprofit",
      date: e.timeslots?.[0]?.start_date
        ? new Date(e.timeslots[0].start_date * 1000).toISOString().split("T")[0]
        : null,
      time: e.timeslots?.[0]?.start_date
        ? new Date(e.timeslots[0].start_date * 1000).toTimeString().slice(0, 5)
        : "10:00",
      address: e.location?.venue || "Austin, TX",
      lat: parseFloat(e.location?.lat) || 30.2672,
      lng: parseFloat(e.location?.lon) || -97.7431,
      desc: (e.description || "").replace(/<[^>]*>/g, "").slice(0, 200),
      source: e.browser_url || "https://mobilize.us",
    })).filter(e => e.date && new Date(e.date) >= new Date());
  } catch (err) {
    console.warn("Mobilize scrape failed:", err.message);
    return [];
  }
}

// ── DEDUPLICATION ────────────────────────────────────────────────────────────

async function getExistingEvents() {
  try {
    const params = new URLSearchParams({
      fields: JSON.stringify(["Name", "Date", "Source"]),
      filterByFormula: "NOT({Status}='rejected')",
    });
    const res  = await fetch(`${AT_URL}?${params}`, { headers: AT_HEADS });
    const data = await res.json();
    return (data.records || []).map(r => ({
      name:   (r.fields.Name   || "").toLowerCase().trim(),
      date:    r.fields.Date   || "",
      source:  r.fields.Source || "",
    }));
  } catch (e) {
    console.warn("Could not fetch existing events for dedup:", e.message);
    return [];
  }
}

function isDuplicate(ev, existing) {
  const nameLower = ev.name.toLowerCase().trim();
  return existing.some(ex =>
    (ex.date === ev.date && ex.name === nameLower) ||
    (ex.source === ev.source && ex.date === ev.date)
  );
}

// ── SAVE TO AIRTABLE ─────────────────────────────────────────────────────────

async function saveEvents(events) {
  const saved = [];
  // Airtable allows max 10 records per request
  for (let i = 0; i < events.length; i += 10) {
    const batch = events.slice(i, i + 10);
    const body  = {
      records: batch.map(e => ({
        fields: {
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
        },
      })),
    };
    try {
      const res  = await fetch(AT_URL, { method: "POST", headers: AT_HEADS, body: JSON.stringify(body) });
      const data = await res.json();
      saved.push(...(data.records || []));
    } catch (e) {
      console.error("Failed to save batch to Airtable:", e.message);
    }
  }
  return saved;
}

// ── SEND EMAIL DIGEST ────────────────────────────────────────────────────────

async function sendDigest(newCount, totalPending) {
  const subject = newCount > 0
    ? `COMN Daily Digest — ${newCount} new event${newCount !== 1 ? "s" : ""} pending review`
    : "COMN Daily Digest — No new events found today";

  const text = newCount > 0
    ? `${newCount} new civic event${newCount !== 1 ? "s were" : " was"} found today and added to your review queue.\n\nTotal pending review: ${totalPending}\n\nReview and publish at:\n${ADMIN_URL}\n\n— COMN`
    : `No new events were found today across your 25 sources.\n\nTotal pending review: ${totalPending}\n\nAdmin panel:\n${ADMIN_URL}\n\n— COMN`;

  try {
    await resend.emails.send({
      from:    "COMN Digest <digest@comnground.netlify.app>",
      to:      DIGEST_TO,
      subject,
      text,
    });
    console.log(`✉️  Digest sent to ${DIGEST_TO}: "${subject}"`);
  } catch (e) {
    console.error("Failed to send email:", e.message);
  }
}

// ── MAIN ─────────────────────────────────────────────────────────────────────

async function runScraper() {
  console.log(`[${new Date().toISOString()}] COMN daily scrape starting…`);

  // 1. Fetch all sources in parallel
  const [council, voting, monitor, handsOff, ajc, mobilize] = await Promise.all([
    scrapeCouncil(),
    scrapeVoting(),
    scrapeMonitor(),
    scrapeHandsOff(),
    scrapeAJC(),
    scrapeMobilize(),
  ]);

  const allFound = [...council, ...voting, ...monitor, ...handsOff, ...ajc, ...mobilize];
  console.log(`Found ${allFound.length} candidate events across all sources`);

  // 2. Dedup against existing Airtable records
  const existing = await getExistingEvents();
  const newEvents = allFound.filter(e => e.date && !isDuplicate(e, existing));
  console.log(`${newEvents.length} new events after deduplication`);

  // 3. Save new events to Airtable as pending
  let saved = [];
  if (newEvents.length > 0) {
    saved = await saveEvents(newEvents);
    console.log(`Saved ${saved.length} new events to Airtable`);
  }

  // 4. Get total pending count for digest
  let totalPending = 0;
  try {
    const params = new URLSearchParams({ filterByFormula: "{Status}='pending'", fields: ["Name"] });
    const res  = await fetch(`${AT_URL}?${params}`, { headers: AT_HEADS });
    const data = await res.json();
    totalPending = (data.records || []).length;
  } catch (e) {}

  // 5. Send email digest
  await sendDigest(saved.length, totalPending);

  console.log(`[${new Date().toISOString()}] Scrape complete.`);
}

// Export for use as a scheduled job or direct call
module.exports = { runScraper };

// If called directly (e.g. node scraper.js)
if (require.main === module) runScraper();
