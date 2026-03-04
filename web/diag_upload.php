<?php
// diag_upload.php (example)
// Upload endpoint for Apify diagnostics HTML.
// Deploy to your GoDaddy cPanel folder (e.g., /jobsearch/diag_upload.php).
//
// SECURITY NOTE:
// - Set a strong token here.
// - Keep the token OUT of GitHub and store it as an Apify env var (DIAG_UPLOAD_TOKEN).

$EXPECTED_TOKEN = 'REPLACE_ME_WITH_A_SECRET_TOKEN';

if ($_SERVER['REQUEST_METHOD'] !== 'POST') {
  http_response_code(405);
  echo "Method Not Allowed";
  exit;
}

$token = isset($_POST['token']) ? $_POST['token'] : '';
if ($token !== $EXPECTED_TOKEN) {
  http_response_code(403);
  echo "Forbidden";
  exit;
}

$content = isset($_POST['content']) ? $_POST['content'] : '';
if ($content === '') {
  http_response_code(400);
  echo "Missing content";
  exit;
}

// Write diagnostics page
$target = __DIR__ . '/diagnostics.html';
file_put_contents($target, $content);

header('Content-Type: application/json');
echo json_encode([
  'ok' => true,
  'written' => basename($target),
  'bytes' => strlen($content),
]);
