import fetch from "node-fetch";
import { createClient } from "@supabase/supabase-js";

// ENV VARIABLES
const SUPABASE_URL = process.env.SUPABASE_URL;
const SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
const BUCKET = "offer-images";

// Build public Supabase Storage URL
const STORAGE_PUBLIC_URL = `${SUPABASE_URL}/storage/v1/object/public/${BUCKET}`;

if (!SUPABASE_URL || !SERVICE_ROLE_KEY) {
  console.error("Missing environment variables!");
  process.exit(1);
}

// Supabase client
const supabase = createClient(SUPABASE_URL, SERVICE_ROLE_KEY);

// ------------------------------------------------------------
// FETCH SUBMISSIONS THAT CONTAIN image metadata
// ------------------------------------------------------------
async function getPending() {
  const { data, error } = await supabase
    .from("provider_submissions_api")
    .select("submission_id, file_uris");

  if (error) throw error;

  return data.filter(
    (s) => Array.isArray(s.file_uris) && s.file_uris.length > 0
  );
}

// ------------------------------------------------------------
// DOWNLOAD IMAGE FROM ORIGINAL JOTFORM URL
// ------------------------------------------------------------
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

// ------------------------------------------------------------
// UPLOAD IMAGE TO SUPABASE STORAGE
// ------------------------------------------------------------
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

  return `${STORAGE_PUBLIC_URL}/${path}`;
}

// ------------------------------------------------------------
// INSERT IMAGE RECORD INTO provider_images
// ------------------------------------------------------------
async function saveImageRecord(submissionId, fieldId, originalUrl, storedPath) {
  const { error } = await supabase.from("provider_images").insert({
    submission_id: submissionId,
    field_id: fieldId,
    original_url: originalUrl,
    stored_path: storedPath,
  });

  if (error) {
    console.log("❌ provider_images insert error:", error.message);
  }
}

// ------------------------------------------------------------
// UPDATE provider_profiles WITH FINAL SUPABASE URL
// ------------------------------------------------------------
async function updateProfileImages(submissionId, newImage) {
  // Step 1: Fetch current profile row
  const { data, error } = await supabase
    .from("provider_profiles")
    .select("images")
    .eq("submission_id", submissionId)
    .single();

  if (error) {
    console.log("❌ Could not load provider_profiles:", error.message);
    return;
  }

  // Step 2: Merge new image with existing images
  const updatedImages = Array.isArray(data.images) ? [...data.images] : [];
  updatedImages.push(newImage);

  // Step 3: Update images field
  const { error: updateError } = await supabase
    .from("provider_profiles")
    .update({ images: updatedImages, updated_at: new Date().toISOString() })
    .eq("submission_id", submissionId);

  if (updateError) {
    console.log("❌ provider_profiles update error:", updateError.message);
  } else {
    console.log(`✅ Updated provider_profiles for ${submissionId}`);
  }
}

// ------------------------------------------------------------
// MAIN WORKER
// ------------------------------------------------------------
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

      console.log(`🔎 Downloading original: ${originalUrl}`);

      const dl = await downloadOriginal(originalUrl);
      if (!dl) continue;

      const storagePath = `${submissionId}/${fieldId}/${filename}`;

      console.log(`⬆ Uploading to Supabase: ${storagePath}`);
      const publicUrl = await uploadToStorage(storagePath, dl.buffer, dl.contentType);
      if (!publicUrl) continue;

      // Save in provider_images
      await saveImageRecord(submissionId, fieldId, originalUrl, storagePath);

      // Add to provider_profiles
      await updateProfileImages(submissionId, {
        stored_path: storagePath,
        url: publicUrl,
        fieldId,
        originalUrl
      });

      console.log(`✅ Stored image: ${publicUrl}`);
    }
  }

  console.log("✨ Worker finished.");
}

run().catch((err) => console.error("FATAL:", err));
