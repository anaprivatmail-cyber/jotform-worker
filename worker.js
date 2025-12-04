import fetch from "node-fetch";
import { createClient } from "@supabase/supabase-js";

// ENV VARIABLES
const SUPABASE_URL = process.env.SUPABASE_URL;
const SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
const BUCKET = "offer-images";

if (!SUPABASE_URL || !SERVICE_ROLE_KEY) {
  console.error("Missing environment variables!");
  process.exit(1);
}

// Supabase client
const supabase = createClient(SUPABASE_URL, SERVICE_ROLE_KEY);

// GET SUBMISSIONS THAT CONTAIN file_uris
async function getPending() {
  const { data, error } = await supabase
    .from("provider_submissions_api")
    .select("submission_id, file_uris");

  if (error) throw error;
  return data.filter(s => Array.isArray(s.file_uris) && s.file_uris.length > 0);
}

// DOWNLOAD IMAGE DIRECTLY FROM ORIGINAL UPLOAD URL
async function downloadOriginal(originalUrl) {
  const res = await fetch(originalUrl, {
    headers: {
      "User-Agent": "Mozilla/5.0",
      "Referer": "https://eu.jotform.com",
      "Origin": "https://eu.jotform.com",
    }
  });

  if (!res.ok) {
    console.log(`❌ Download failed (${res.status}): ${originalUrl}`);
    return null;
  }

  const buffer = Buffer.from(await res.arrayBuffer());
  const contentType = res.headers.get("content-type") || "image/jpeg";
  return { buffer, contentType };
}

// UPLOAD INTO SUPABASE STORAGE
async function uploadToStorage(path, buffer, contentType) {
  const { error } = await supabase.storage
    .from(BUCKET)
    .upload(path, buffer, { upsert: true, contentType });

  if (error) {
    console.log("❌ Upload error:", error.message);
    return null;
  }

  return true;
}

// RECORD INTO provider_images TABLE
async function saveRecord(submissionId, fieldId, originalUrl, storedPath) {
  await supabase.from("provider_images").insert({
    submission_id: submissionId,
    field_id: fieldId,
    original_url: originalUrl,
    stored_path: storedPath,
  });
}

// MAIN WORKER
async function run() {
  console.log("🚀 Worker started…");

  const submissions = await getPending();
  console.log(`📌 Found ${submissions.length} submissions with images`);

  for (const sub of submissions) {
    const submissionId = sub.submission_id;
    const files = sub.file_uris;

    console.log(`\n📁 Processing submission ${submissionId}`);

    for (const file of files) {
      const originalUrl = file.originalUrl;
      const fieldId = file.fieldId;
      const filename = originalUrl.split("/").pop();

      console.log(`🔎 Downloading: ${originalUrl}`);

      const dl = await downloadOriginal(originalUrl);
      if (!dl) continue;

      const path = `${submissionId}/${fieldId}/${filename}`;
      const uploaded = await uploadToStorage(path, dl.buffer, dl.contentType);
      if (!uploaded) continue;

      await saveRecord(submissionId, fieldId, originalUrl, path);

      console.log(`✅ Stored image: ${path}`);
    }
  }

  console.log("✨ Worker finished.");
}

run().catch(err => console.error("FATAL:", err));
