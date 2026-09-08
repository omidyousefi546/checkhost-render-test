import express from "express";

const app = express();

app.get("/", (req, res) => {
  res.send("Render test is running");
});

app.get("/test-checkhost", async (req, res) => {
  try {
    const response = await fetch("https://check-host.net/nodes/hosts", {
      headers: {
        "Accept": "application/json",
        "User-Agent": "TelegramProxyAdmin/1.1"
      }
    });

    const body = await response.text();

    res.status(200).json({
      checkHostStatus: response.status,
      checkHostStatusText: response.statusText,
      headers: Object.fromEntries(response.headers.entries()),
      body: body.slice(0, 3000)
    });
  } catch (error) {
    res.status(500).json({
      error: error.message
    });
  }
});

const PORT = process.env.PORT || 10000;

app.listen(PORT, "0.0.0.0", () => {
  console.log(`Server running on port ${PORT}`);
});
