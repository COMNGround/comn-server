const express = require("express");
const cors    = require("cors");

const app  = express();
const PORT = process.env.PORT || 8080;

const AT_TOKEN = process.env.AT_TOKEN;
const AT_BASE  = process.env.AT_BASE;
const AT_TABLE = "Events";
const AT_URL   = `https://api.airtable.com/v0/${AT_BASE}/${AT_TABLE}`;
const AT_HEADS = {
  "Authorization": `Bearer ${AT_TOKEN}`,
  "Content-Type":  "application/json"
};

app.use(cors({ origin: "*" }));
app.use(express.json());

// Today's date in YYYY-MM-DD for filtering upcoming events
function today() {
  return new Date().toISOString().split("T")[0];
}

async function airtable(path, method = "GET", body = null) {
  const opts = { method, headers: AT_HEADS };
  if (body) opts.body = JSON.stringify(body);
  const res  = await fetch(`${AT_URL}${path}`, opts);
  const data = await res.json();
  if (!res.ok) throw new Error(JSON.stringify(data));
  return data;
}

// Health check
app.get("/", (req, res) => {
  res.json({ status: "COMN server running", nextScrape: "Daily 12 PM CT" });
});

// GET upcoming live events for public map (today and future only)
app.get("/events", async (req, res) => {
  try {
    const filter = encodeURIComponent(`AND({Status}='live', {Date}>='${today()}')`);
    const data = await airtable(
      `?filterByFormula=${filter}&sort[0][field]=Date&sort[0][direction]=asc`
    );
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
    console.error("GET /events error:", e.message);
    res.status(500).json({ error: "Failed to fetch events", detail: e.message });
  }
});

// GET pending count for admin dot
app.get("/pending-count", async (req, res) => {
  try {
    const filter = encodeURIComponent(`{Status}='pending'`);
    const data = await airtable(`?filterByFormula=${filter}`);
    res.json({ count: (data.records || []).length });
  } catch (e) {
    res.status(500).json({ count: 0 });
  }
});

// GET admin events by status (all dates for admin)
app.get("/admin/events", async (req, res) => {
  try {
    const status = req.query.status || "pending";
    const filter = encodeURIComponent(`{Status}='${status}'`);
    const data = await airtable(
      `?filterByFormula=${filter}&sort[0][field]=Date&sort[0][direction]=asc`
    );
    res.json({ records: data.records || [] });
  } catch (e) {
    console.error("GET /admin/events error:", e.message);
    res.status(500).json({ error: "Failed to fetch admin events", detail: e.message });
  }
});

// POST submit new event
app.post("/submit", async (req, res) => {
  try {
    const f = req.body;
    if (!f.name || !f.type || !f.date || !f.address)
      return res.status(400).json({ error: "Missing required fields" });
    const data = await airtable("", "POST", {
      records: [{ fields: {
        Name:        f.name,
        Type:        f.type,
        Date:        f.date,
        Time:        f.time        || "",
        Address:     f.address,
        Latitude:    parseFloat(f.lat)  || 30.2672,
        Longitude:   parseFloat(f.lng)  || -97.7431,
        Description: f.desc        || "",
        Source:      f.source      || "",
        Status:      "pending",
        SubmittedBy: f.submittedBy || "Public",
      }}]
    });
    res.json({ success: true, id: data.records?.[0]?.id });
  } catch (e) {
    console.error("POST /submit error:", e.message);
    res.status(500).json({ error: "Failed to submit event" });
  }
});

// PATCH event status
app.patch("/admin/events/:id", async (req, res) => {
  try {
    const { status } = req.body;
    if (!["live", "rejected"].includes(status))
      return res.status(400).json({ error: "Invalid status" });
    await airtable(`/${req.params.id}`, "PATCH", { fields: { Status: status } });
    res.json({ success: true });
  } catch (e) {
    console.error("PATCH error:", e.message);
    res.status(500).json({ error: "Failed to update event" });
  }
});

app.listen(PORT, () => console.log(`COMN server on port ${PORT}`));
