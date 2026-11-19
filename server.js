// ai-denwa: 電話 -> Twilio ConversationRelay -> Gemini (無料枠) -> 音声で返答
// Twilio が音声<->文字の変換を担当し、このサーバーは会話の頭脳だけを持つ。
import express from "express";
import { WebSocketServer } from "ws";
import http from "http";

const PORT = process.env.PORT || 8080;
const GEMINI_API_KEY = process.env.GEMINI_API_KEY || "";
const MODEL = process.env.GEMINI_MODEL || "gemini-2.5-flash";
const SYSTEM_PROMPT =
  process.env.SYSTEM_PROMPT ||
  "あなたは電話で話す、明るくて親切な日本語のAIアシスタントです。" +
    "返事は短く、1〜2文で、話し言葉で答えてください。" +
    "記号や箇条書きは使わず、声で聞いて分かる言い方にしてください。";

const app = express();
app.use(express.urlencoded({ extended: false }));
app.use(express.json());

app.get("/", (_req, res) => res.send("ai-denwa is running"));

// Twilio が着信時に呼ぶ Webhook。ConversationRelay へつなぐ TwiML を返す。
app.post("/voice", (req, res) => {
  const host = req.headers.host;
  const wsUrl = `wss://${host}/relay`;
  const twiml = `<?xml version="1.0" encoding="UTF-8"?>
<Response>
  <Connect>
    <ConversationRelay url="${wsUrl}" language="ja-JP"
      welcomeGreeting="こんにちは。何でも聞いてください。" />
  </Connect>
</Response>`;
  res.type("text/xml").send(twiml);
});

const server = http.createServer(app);
const wss = new WebSocketServer({ server, path: "/relay" });

// 通話ごとの会話履歴
const sessions = new Map();

// history: [{role:"user"|"model", text}] を Gemini の contents 形式で送る
async function askAI(history) {
  const url =
    `https://generativelanguage.googleapis.com/v1beta/models/${MODEL}` +
    `:streamGenerateContent?alt=sse`;
  const resp = await fetch(url, {
    method: "POST",
    headers: {
      "x-goog-api-key": GEMINI_API_KEY,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      systemInstruction: { parts: [{ text: SYSTEM_PROMPT }] },
      contents: history.map((m) => ({
        role: m.role === "assistant" ? "model" : "user",
        parts: [{ text: m.text }],
      })),
      generationConfig: { maxOutputTokens: 300 },
    }),
  });
  if (!resp.ok || !resp.body) {
    throw new Error(`Gemini API error: ${resp.status}`);
  }
  return resp.body; // ReadableStream of SSE chunks
}

wss.on("connection", (ws) => {
  let callSid = null;
  let buffer = "";

  ws.on("message", async (data) => {
    let msg;
    try {
      msg = JSON.parse(data.toString());
    } catch {
      return;
    }

    if (msg.type === "setup") {
      callSid = msg.callSid;
      sessions.set(callSid, []);
      console.log(`[setup] call ${callSid} from ${msg.from}`);
      return;
    }

    if (msg.type === "prompt") {
      // 相手の発話（確定テキスト）が届く
      const userText = (msg.voicePrompt || "").trim();
      if (!userText || !callSid) return;
      const history = sessions.get(callSid) || [];
      history.push({ role: "user", text: userText });
      console.log(`[user] ${userText}`);

      let fullReply = "";
      try {
        const stream = await askAI(history);
        const decoder = new TextDecoder();
        for await (const chunk of stream) {
          buffer += decoder.decode(chunk, { stream: true });
          const lines = buffer.split("\n");
          buffer = lines.pop() || "";
          for (const line of lines) {
            const t = line.trim();
            if (!t.startsWith("data:")) continue;
            const payload = t.slice(5).trim();
            if (payload === "[DONE]") continue;
            try {
              const json = JSON.parse(payload);
              const parts = json.candidates?.[0]?.content?.parts || [];
              const token = parts.map((p) => p.text || "").join("");
              if (token) {
                fullReply += token;
                ws.send(
                  JSON.stringify({ type: "text", token, last: false })
                );
              }
            } catch {
              /* 部分JSONは無視 */
            }
          }
        }
      } catch (e) {
        console.error("[ai error]", e.message);
        fullReply = "すみません、うまく聞き取れませんでした。もう一度お願いします。";
        ws.send(JSON.stringify({ type: "text", token: fullReply, last: false }));
      }
      ws.send(JSON.stringify({ type: "text", token: "", last: true }));
      history.push({ role: "assistant", text: fullReply });
      sessions.set(callSid, history);
      console.log(`[ai] ${fullReply}`);
      return;
    }

    if (msg.type === "interrupt") {
      console.log("[interrupt] 相手が割り込みました");
      return;
    }

    if (msg.type === "error") {
      console.error("[relay error]", msg.description || msg);
    }
  });

  ws.on("close", () => {
    if (callSid) sessions.delete(callSid);
    console.log(`[close] call ${callSid}`);
  });
});

server.listen(PORT, () => {
  console.log(`ai-denwa listening on :${PORT}`);
  if (!GEMINI_API_KEY) {
    console.warn("警告: GEMINI_API_KEY が未設定です");
  }
});
