// server.js
const express = require("express");
const { MongoClient, GridFSBucket, ObjectId } = require("mongodb");
const cors = require("cors");
require("dotenv").config();

const app = express();
app.use(cors());
app.use(express.json());

const MONGO_URI = process.env.MONGO_URI;
const DB_NAME = process.env.DB_NAME || "suorya_db";
const BUCKET_NAME = "images";

let db, bucket;

async function start() {
  const client = new MongoClient(MONGO_URI, { useUnifiedTopology: true });
  await client.connect();
  db = client.db(DB_NAME);
  bucket = new GridFSBucket(db, { bucketName: BUCKET_NAME });
  console.log("Connected to MongoDB", DB_NAME);
}
start().catch(err => { console.error(err); process.exit(1); });

// GET metadata: /api/metadata?category=Ribbons&subcategory=Velvet%20Ribbons&subsubcategory=Cotton
app.get("/api/metadata", async (req, res) => {
  const q = {};
  const { category, subcategory, subsubcategory, limit } = req.query;
  if (category) q.category = category;
  if (subcategory) q.subcategory = subcategory;
  if (subsubcategory) q.subsubcategory = subsubcategory;
  const cursor = db.collection("imagesMeta").find(q).sort({ createdAt: -1 });
  if (limit) cursor.limit(parseInt(limit, 10));
  const results = await cursor.toArray();
  // convert ObjectIds to strings for frontend
  const out = results.map(r => {
    return {
      ...r,
      _id: r._id.toString(),
      variants: {
        thumbnailId: r.variants.thumbnailId.toString(),
        fullId: r.variants.fullId.toString()
      }
    };
  });
  res.json(out);
});

// stream file by GridFS file id
// GET /api/file/:fileId
app.get("/api/file/:fileId", async (req, res) => {
  const { fileId } = req.params;
  const { as } = req.query; // optional query param for forced content type or other usage
  let _id;
  try { _id = new ObjectId(fileId); } catch (e) { return res.status(400).send("bad id"); }

  const filesColl = db.collection(`${BUCKET_NAME}.files`);
  const fileDoc = await filesColl.findOne({ _id });
  if (!fileDoc) return res.status(404).send("file not found");

  const contentType = fileDoc.contentType || "image/webp";
  res.setHeader("Content-Type", contentType);
  // long cache for static images
  res.setHeader("Cache-Control", "public, max-age=31536000, immutable");

  const downloadStream = bucket.openDownloadStream(_id);
  downloadStream.on("error", (err) => {
    console.error("GridFS stream error", err);
    res.sendStatus(500);
  });
  downloadStream.pipe(res);
});

const PORT = process.env.PORT || 5001;
app.listen(PORT, () => console.log("Server listening on", PORT));
