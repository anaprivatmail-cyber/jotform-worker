import fetch from "node-fetch";
import { createClient } from "@supabase/supabase-js";

// ENV VARIABLES
const SUPABASE_URL = process.env.SUPABASE_URL;
const SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
const JOTFORM_API_KEY = process.env.JOTFORM_API_KEY;
const BUCKET = "offer-images";

if (!SUPABASE_URL || !SERVICE_ROLE_KEY || !JOTFORM_API_KEY) {
  console.error("Missing environment variables!");
  process.exit(1);
}

// Supabase client
const supabase = createClient(SUPABASE_URL, SERVICE_ROLE_KEY);

// ----------------------------------------------
// FETCH ALL SUBMISSIONS THAT CONTAIN file_uris
// ----------------------------------------------
async function getPending() {
  const { data, error } = await supabase
    .from("provider_submissions_api")
    .select("submission_id, file_uris");

  if (error) throw error;

  // Filter only submissions with file URLs
  return data.filter(
    (s) => Array.isArray(s.file_uris) && s.file_uris.length > 0
  );
}

// ----------------------------------------------
// DOWNLOAD AN IMAGE FROM JOTFORM FILE API
// ----------------------------------------------
// IMPORTANT: fileIndex is ALWAYS 0 (first file), 
// because JotForm FILE API does NOT use fieldId.
// It uses INDEX of the uploaded file in submission.
async function downloadImage(submissionId, fileIndex, filename) {
  const url = `https://eu-api.jotform.com/file/${submissionId}/${fileIndex}?apiKey=${JOTFORM_API_KEY}`;

  const res = await fetch(url);

  if (!res.ok) {
    console.log(`❌ Download failed (${res.status}): ${url}`);
    return null;
  }

  const buffer = Buffer.from(await res.arrayBuffer());
  const contentType = res.headers.get("content-type") || "image/jpeg";

  return { buffer, contentType };
}

// ----------------------------------------------
// UPLOAD IMAGE TO SUPABASE STORAGE
// ----------------------------------------------
async function uploadToStorage(path, buffer, contentType) {
  const { error } = await supabase.storage
    .from(BUCKET)
    .upload(path, buffer, {
      upsert: true,
      contentType,
    });

  if (error) {
    console.log("❌ Upload error:", error.message);
    return null;
  }

  return true;
}

// ----------------------------------------------
// SAVE IMAGE RECORD IN provider_images TABLE
// ----------------------------------------------
async function saveRecord(submissionId, fieldId, originalUrl, storedPath) {
  const { error } = await supabase.from("provider_images").insert({
    submission_id: submissionId,
    field_id: fieldId,
    original_url: originalUrl,
    stored_path: storedPath,
  });

  if (error) {
    console.log("❌ DB insert error:", error.message);
  }
}

// ----------------------------------------------
// MAIN WORKER FUNCTION
// ----------------------------------------------
async function run() {
  console.log("🚀 Worker started…");

  const submissions = await getPending();

  console.log(`📌 Found ${submissions.length} submissions with images`);

  for (const sub of submissions) {
    const submissionId = sub.submission_id;
    const fileList = sub.file_uris;

    console.log(`\n📁 Processing submission ${submissionId}`);
    
    let fileIndex = 0; // IMPORTANT: JotForm uses INDEX, not fieldId

    for (const file of fileList) {
      const originalUrl = file.originalUrl;
      const fieldId = file.fieldId;
      const filename = originalUrl.split("/").pop();

      console.log(`🔎 Attempting download: fileIndex=${fileIndex} → ${originalUrl}`);

      // Step 1: Download
      const dl = await downloadImage(submissionId, fileIndex, filename);
      if (!dl) {
        fileIndex++;
        continue;
      }

      // Step 2: Upload to Supabase
      const path = `${submissionId}/${fieldId}/${filename}`;
      const uploaded = await uploadToStorage(path, dl.buffer, dl.contentType);
      if (!uploaded) {
        fileIndex++;
        continue;
      }

      // Step 3: Save DB record
      await saveRecord(submissionId, fieldId, originalUrl, path);

      console.log(`✅ Stored image: ${path}`);

      fileIndex++;
    }
  }

  console.log("✨ Worker finished.");
}

// Run
run().catch((err) => console.error("FATAL ERROR:", err));
