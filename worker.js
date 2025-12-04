import fetch from "node-fetch";
import { createClient } from "@supabase/supabase-js";

const SUPABASE_URL = https://jxsqmbistfnkeawfwuis.supabase.co;
const SERVICE_ROLE_KEY = eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Imp4c3FtYmlzdGZua2Vhd2Z3dWlzIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc2MzgzODA5MCwiZXhwIjoyMDc5NDE0MDkwfQ.aGSYpkzLtWXepupnSuL70EfEgGEQNiYNccdmFbpnoQM;
const JOTFORM_API_KEY = 0a4b9a4f7f1f00ee5d400a2fc0c03f6a;
const BUCKET = "offer-images";

if (!SUPABASE_URL || !SERVICE_ROLE_KEY || !JOTFORM_API_KEY) {
  console.error("Missing environment variables!");
  process.exit(1);
}

const supabase = createClient(SUPABASE_URL, SERVICE_ROLE_KEY);

async function getPending() {
  const { data, error } = await supabase
    .from("provider_submissions_api")
    .select("submission_id, raw_payload");

  if (error) throw error;
  return data;
}

async function downloadImage(submissionId, fieldId, filename) {
  const url = `https://eu-api.jotform.com/file/${submissionId}/${fieldId}?apiKey=${JOTFORM_API_KEY}`;
  const res = await fetch(url);

  if (!res.ok) {
    console.log("Download failed:", res.status, url);
    return null;
  }

  const buffer = Buffer.from(await res.arrayBuffer());
  const contentType = res.headers.get("content-type") || "image/jpeg";

  return { buffer, contentType };
}

async function uploadToStorage(path, buffer, contentType) {
  const { error } = await supabase.storage
    .from(BUCKET)
    .upload(path, buffer, {
      upsert: true,
      contentType,
    });

  if (error) {
    console.log("Upload error:", error);
    return null;
  }
  return true;
}

async function saveRecord(submissionId, fieldId, originalUrl, storedPath) {
  await supabase.from("provider_images").insert({
    submission_id: submissionId,
    field_id: fieldId,
    original_url: originalUrl,
    stored_path: storedPath,
  });
}

async function run() {
  console.log("Worker started…");

  const submissions = await getPending();

  for (const sub of submissions) {
    const submissionId = sub.submission_id;
    const answers = sub.raw_payload?.answers || {};

    for (const fieldId of Object.keys(answers)) {
      const ans = answers[fieldId];

      if (ans?.type === "control_fileupload") {
        const urls = Array.isArray(ans.answer) ? ans.answer : [ans.answer];

        for (const url of urls) {
          const filename = url.split("/").pop();
          const dl = await downloadImage(submissionId, fieldId, filename);

          if (!dl) continue;

          const path = `${submissionId}/${fieldId}/${filename}`;
          const uploaded = await uploadToStorage(path, dl.buffer, dl.contentType);
          if (!uploaded) continue;

          await saveRecord(submissionId, fieldId, url, path);
          console.log("Stored:", path);
        }
      }
    }
  }

  console.log("Worker finished.");
}

run();
