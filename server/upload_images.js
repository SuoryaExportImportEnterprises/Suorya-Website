// upload_images.js - FINAL PIPELINE FIX
const { MongoClient, GridFSBucket, ObjectId } = require("mongodb");
const path = require("path");
const fs = require("fs");
const sharp = require("sharp");
const stream = require("stream");
// Import the stream promises API for reliable pipeline execution
const { pipeline } = require('stream/promises');

require("dotenv").config();

const MONGO_URI = process.env.MONGO_URI;
if (!MONGO_URI) throw new Error("Set MONGO_URI in .env");

const BASE_IMAGES_DIR = path.resolve(process.env.BASE_IMAGES_DIR || "./src/assets/SUORYA");
const DB_NAME = process.env.DB_NAME || "suorya_db";
const BUCKET_NAME = "images";

function bufferToStream(buffer) {
  const readable = new stream.PassThrough();
  readable.end(buffer);
  return readable;
}

/**
 * Uploads a buffer to GridFS using stream.pipeline for reliable error handling.
 * Returns the file object containing the _id.
 */
async function uploadBufferToGridFS(bucket, buffer, filename, metadata = {}, contentType = "image/webp") {
    const uploadStream = bucket.openUploadStream(filename, {
        metadata,
        contentType,
    });

    // Capture the file object from the stream's 'finish' event
    let fileMetadata;
    uploadStream.once('finish', (file) => {
        fileMetadata = file;
    });

    // Use stream.promises.pipeline for guaranteed error propagation and cleanup
    await pipeline(
        bufferToStream(buffer),
        uploadStream
    );

    // fileMetadata should be set if pipeline completes successfully
    if (!fileMetadata) {
        throw new Error(`GridFS upload finished without returning file metadata for ${filename}.`);
    }
    
    return fileMetadata;
}

/**
 * Processes a single image file, creates variants, uploads them, and inserts metadata.
 * NOTE: The try/catch is removed for sharp processing errors, but kept for read errors.
 */
async function processFile(db, bucket, filePath, category, subcategory, subsubcategory) {
  const filename = path.basename(filePath);
  
  // 1. READ ORIGINAL FILE (Critical Check)
  let originalBuffer;
  try {
    originalBuffer = fs.readFileSync(filePath);
  } catch (readErr) {
    console.error(`\n❌ FAILED: Could not read file ${filename}. Skipping.`, readErr.message);
    return null; 
  }

  // 2. SHARP PROCESSING (Allowed to throw up to walkAndUpload catch)
  const sharpMeta = await sharp(originalBuffer).metadata();

  const thumbBuffer = await sharp(originalBuffer)
    .resize({ width: 600, withoutEnlargement: true })
    .webp({ quality: 75 })
    .toBuffer();

  const fullBuffer = await sharp(originalBuffer)
    .resize({ width: 1600, withoutEnlargement: true })
    .webp({ quality: 82 })
    .toBuffer();

  const lqipBuffer = await sharp(originalBuffer)
    .resize({ width: 20 })
    .blur(1)
    .webp({ quality: 30 })
    .toBuffer();
  const lqipDataUri = `data:image/webp;base64,${lqipBuffer.toString("base64")}`;

  // 3. UPLOAD BUFFERS TO GRIDFS 
  // If this throws, walkAndUpload().catch() will catch it with the raw error.
  const thumbFile = await uploadBufferToGridFS(bucket, thumbBuffer, `${filename}-thumb.webp`, {
    category, subcategory, subsubcategory, variant: "thumbnail"
  }, "image/webp");

  const fullFile = await uploadBufferToGridFS(bucket, fullBuffer, `${filename}-full.webp`, {
    category, subcategory, subsubcategory, variant: "full"
  }, "image/webp");

  // 4. METADATA INSERTION
  
  const metaDoc = {
    filename,
    alt: filename.replace(/\.(jpg|jpeg|png|webp)$/i, ""),
    category,
    subcategory,
    subsubcategory: subsubcategory || null,
    variants: {
      thumbnailId: thumbFile._id, 
      fullId: fullFile._id,
    },
    lqip: lqipDataUri,
    original: {
      width: sharpMeta.width || null,
      height: sharpMeta.height || null,
      format: sharpMeta.format || null,
    },
    createdAt: new Date(),
  };

  const insertRes = await db.collection("imagesMeta").insertOne(metaDoc);
  console.log(`✅ Success: ${filename} inserted.`);
  return insertRes.insertedId;
}

async function walkAndUpload() {
  const client = new MongoClient(MONGO_URI, {
      // Keep the explicit timeout settings
      serverSelectionTimeoutMS: 5000, 
      connectTimeoutMS: 10000,       
      socketTimeoutMS: 45000         
  }); 
  
  try {
    await client.connect();
  } catch (err) {
    console.error("\n*** FATAL CONNECTION ERROR ***");
    console.error("Could not connect to MongoDB. Check your MONGO_URI and network access (e.g., Atlas IP Whitelist).");
    console.error("Raw Error:", err.message);
    await client.close();
    process.exit(1);
  }
  
  console.log("Successfully connected to MongoDB.");
  const db = client.db(DB_NAME);
  const bucket = new GridFSBucket(db, { bucketName: BUCKET_NAME });

  const categories = fs.readdirSync(BASE_IMAGES_DIR).filter(f => fs.statSync(path.join(BASE_IMAGES_DIR, f)).isDirectory());

  for (const category of categories) {
    const categoryPath = path.join(BASE_IMAGES_DIR, category);
    const subcategories = fs.readdirSync(categoryPath).filter(f => fs.statSync(path.join(categoryPath, f)).isDirectory());

    if (subcategories.length === 0) {
      const files = fs.readdirSync(categoryPath).filter(f => /\.(jpg|jpeg|png|webp)$/i.test(f));
      for (const f of files) {
        console.log(`\nProcessing ${f} -> ${category} -> (none)`);
        await processFile(db, bucket, path.join(categoryPath, f), category, null, null);
      }
    } else {
      for (const subcat of subcategories) {
        const subcatPath = path.join(categoryPath, subcat);
        const subsubdirs = fs.readdirSync(subcatPath).filter(f => fs.statSync(path.join(subcatPath, f)).isDirectory());
        
        if (subsubdirs.length > 0) {
          for (const subsub of subsubdirs) {
            const subsubPath = path.join(subcatPath, subsub);
            const files = fs.readdirSync(subsubPath).filter(f => /\.(jpg|jpeg|png|webp)$/i.test(f));
            for (const f of files) {
              console.log(`\nProcessing ${f} -> ${category} -> ${subcat} -> ${subsub}`);
              await processFile(db, bucket, path.join(subsubPath, f), category, subcat, subsub);
            }
          }
        } else {
          const files = fs.readdirSync(subcatPath).filter(f => /\.(jpg|jpeg|png|webp)$/i.test(f));
          for (const f of files) {
            console.log(`\nProcessing ${f} -> ${category} -> ${subcat}`);
            await processFile(db, bucket, path.join(subcatPath, f), category, subcat, null);
          }
        }
      }
    }
  }

  console.log("\n*** COMPLETE: All images processed. ***");
  await client.close();
}

walkAndUpload().catch(err => { 
    console.error("\nFATAL SCRIPT ERROR (Raw Message Below):"); 
    console.error(err); 
    process.exit(1); 
});