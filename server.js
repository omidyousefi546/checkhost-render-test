import express from "express";

const app = express();

app.get("/", (req, res) => {
  res.send("Render test is running");
});

app.get("/test-checkhost/:requestId", async (req, res) => {
  try {
    const { requestId } = req.params;

    const slaves = [
      "ir1.node.check-host.net",
      "ir2.node.check-host.net",
      "ir3.node.check-host.net",
      "ir4.node.check-host.net",
      "ir5.node.check-host.net",
      "ir6.node.check-host.net",
      "ir7.node.check-host.net"
    ];

    const body = new URLSearchParams();

    for (const slave of slaves) {
      body.append("slaves[]", slave);
    }

    const response = await fetch(
      `https://check-host.net/check_result/${encodeURIComponent(requestId)}`,
      {
        method: "POST",
        headers: {
          "accept": "*/*",
          "accept-language": "en-GB,en-US;q=0.9,en;q=0.8,fa;q=0.7",
          "content-type": "application/x-www-form-urlencoded; charset=UTF-8",
          "origin": "https://check-host.net",
          "referer": "https://check-host.net/",
          "user-agent":
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/152.0.0.0 Safari/537.36",

          // این مقدار معمولاً موقت است؛ پایین توضیح دادم.
          "cookie":
            "csrf_token=cd8521a0a46768fb0355cfe1b9cbc20bdaf242fe"
        },
        body
      }
    );

    const responseText = await response.text();

    res.status(200).json({
      checkHostStatus: response.status,
      checkHostStatusText: response.statusText,

      sentNodes: slaves,

      sentBody: body.toString(),

      response: responseText
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
