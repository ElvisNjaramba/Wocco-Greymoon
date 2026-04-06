export default async function handler(req, res) {
  const path = (req.query.path || []).join("/");
  const url = `http://38.247.148.50:10001/api/${path}/`;

  try {
    const response = await fetch(url, {
      method: req.method,
      headers: { "Content-Type": "application/json" },
      body: ["GET", "HEAD"].includes(req.method) ? undefined : JSON.stringify(req.body),
    });

    const data = await response.json();
    res.status(response.status).json(data);
  } catch (err) {
    res.status(502).json({ error: "Backend unreachable", detail: err.message });
  }
}