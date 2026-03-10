const express    = require("express");
const cors       = require("cors");
const cron       = require("node-cron");
const { runScraper } = require("./scraper");

const app  = express();
const PORT = process.env.PORT || 3000;

const AT_TOKEN = process.env.AT_TOKEN;
const AT_BASE  = process.env.AT_BASE;
const AT_TABLE = "Events";
const AT_URL   = `https://api.airtable.com/v0/${AT_BASE}/${AT_TABLE}`;
const AT_HEADS = { "Authorization": `Bearer ${AT_TOKEN}`, "Content-Type": "application/json" };

app.use(cors({ origin: "*" }));
app.use(express.json());

// ── CRON: Daily scrape at 12:00 PM Central Time ───────────────────────────────
cron.schedule("0 18 * * *", () => {
  console.log("Running daily scrape at 12 PM CT…");
  runScraper().catch(e => console.error("Scraper error:", e));
}, { timezone: "America/Chicago" });

// ── HELPER ────────────────────────────────────────────────────────────────────
async function airtable(path, method = "GET", body = null) {
  const opts = { method, headers: AT_HEADS, signal: AbortSignal.timeout(10000) };
  if (body) opts.body = JSON.stringify(body);
  const res  = await fetch(`${AT_URL}${path}`, opts);
  const data = await res.json();
  if (!res.ok) throw { status: res.status, error: data };
  return data;
}

// ── GET live events (public map) ──────────────────────────────────────────────
app.get("/events", async (req, res) => {
  try {
    const params = new URLSearchParams({
      filterByFormula: "{Status}='live'",
      sort: JSON.stringify([{ field: "Date", direction: "asc" }]),
    });
    const data   = await airtable(`?${params}`);
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
  } catch (e) {
    res.status(500).json({ error: "Failed to fetch events" });
  }
});

// ── GET pending count ─────────────────────────────────────────────────────────
app.get("/pending-count", async (req, res) => {
  try {
    const params = new URLSearchParams({ filterByFormula: "{Status}='pending'", fields: JSON.stringify(["Name"]) });
    const data   = await airtable(`?${params}`);
    res.json({ count: (data.records || []).length });
  } catch (e) {
    res.status(500).json({ count: 0 });
  }
});

// ── GET admin events ──────────────────────────────────────────────────────────
app.get("/admin/events", async (req, res) => {
  try {
    const status = req.query.status || "pending";
    const params = new URLSearchParams({
      filterByFormula: `{Status}='${status}'`,
      sort: JSON.stringify([{ field: "Date", direction: "asc" }]),
    });
    const data = await airtable(`?${params}`);
    res.json({ records: data.records || [] });
  } catch (e) {
    res.status(500).json({ error: "Failed to fetch admin events" });
  }
});

// ── POST submit event ─────────────────────────────────────────────────────────
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
        Status:      "pending",
        SubmittedBy: f.submittedBy || "Public",
      }}],
    });
    res.json({ success: true, id: data.records?.[0]?.id });
  } catch (e) {
    res.status(500).json({ error: "Failed to submit event" });
  }
});

// ── PATCH event status ────────────────────────────────────────────────────────
app.patch("/admin/events/:id", async (req, res) => {
  try {
    const { status } = req.body;
    if (!["live", "rejected"].includes(status))
      return res.status(400).json({ error: "Invalid status" });
    await airtable(`/${req.params.id}`, "PATCH", { fields: { Status: status } });
    res.json({ success: true });
  } catch (e) {
    res.status(500).json({ error: "Failed to update event" });
  }
});

// ── Manual scrape trigger ─────────────────────────────────────────────────────
app.post("/admin/scrape-now", async (req, res) => {
  res.json({ message: "Scrape started — check your email in ~30 seconds" });
  runScraper().catch(e => console.error("Manual scrape error:", e));
});

// ── Health check ──────────────────────────────────────────────────────────────
app.get("/", (req, res) => res.json({ status: "COMN server running", nextScrape: "Daily 12 PM CT" }));

app.listen(PORT, () => console.log(`COMN server on port ${PORT}`));
