import express from "express";
import cors from "cors";

import Queue from "./queue.js";

const app = express();
const port = 3000;

app.use(express.json());
app.use(
  cors({
    origin: "*",
    methods: ["GET", "POST"],
    allowedHeaders: ["Content-Type"],
  })
);

const queue = new Queue();

// registers subscribers
app.get("/events", (req, res) => {
  console.log({ req, res });
  let callback;
  try {
    const { topic_id, idx } = req.query;

    if (typeof topic_id !== "string" ) {
      return res.status(400).send("topic_id is invalid");
    }

    let isIdxValid = true;
    if (idx && isNaN(+idx)) {
      isIdxValid = false;
      return res.status(400).send("idx is not a number");
    }

    res.set({
      "Content-Type": "text/event-stream",
      "Cache-Control": "no-cache",
      Connection: "keep-alive",
    });
    res.flushHeaders();

    // Send an initial message
    res.write(`data: ACK from server\n\n`);

    callback = (message) => {
      console.log(
        `Sending message to topic ${topic_id}, idx ${idx}: ${message}`
      );
      res.write(`data: ${message}\n\n`);
    };

    queue.register(topic_id, isIdxValid ? +idx : Infinity, callback);

    // Clean up when client disconnects
    req.on("close", () => {
      queue.unregister(topic_id, callback);
    });
  } catch (error) {
    if (callback) {
      queue.unregister(topic_id, callback);
    }
    console.error("Error in /events:", error);
    res.status(500).send("Internal Server Error");
  }
});

app.post("/publish", (req, res) => {
  try {
    const { topicId, message } = req?.body ?? {};

    if (typeof topicId != "string") {
      res.status(400).send("topicId is not string");
      return;
    }

    if (typeof message != "string") {
      res.status(400).send("topicId is not string");
      return;
    }

    console.log(`Received message for topic ${topicId}: ${message}`);

    queue.publish(topicId, message);

    res.status(200).send("Message received");
  } catch (error) {
    console.error("Error in /publish:", error);
    res.status(500).send("Internal Server Error");
  }
});

app.listen(port, () => {
  console.log(`App listening on port ${port}`);
});
