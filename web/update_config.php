<?php
// update_config.php
// Allows Claude Code to automatically update rubric.md and config.json via POST.
//
// Deploy to: http://forgaard.com/jobsearch/update_config.php
//
// Called by Claude with:
//   curl -s -X POST https://forgaard.com/jobsearch/update_config.php \
//     -H "Content-Type: application/json" \
//     -d '{"key":"...","file":"rubric.md","content":"..."}'
//
// Returns JSON: {"ok":true,"file":"rubric.md","bytes":1234,"updatedAt":"..."}

// ---- Configuration ----
define('API_KEY', 'gvxCbedqoUdg21d52KIiqXBpj4Tei00v-N8o_gQgcsk');
define('ALLOWED_FILES', ['rubric.md', 'config.json']);
define('FILE_DIR', __DIR__);  // Writes files into the same directory as this script

// ---- Main ----
header('Content-Type: application/json');

if ($_SERVER['REQUEST_METHOD'] !== 'POST') {
    http_response_code(405);
    echo json_encode(['ok' => false, 'error' => 'Method not allowed']);
    exit;
}

$raw  = file_get_contents('php://input');
$data = json_decode($raw, true);

if (!is_array($data)) {
    http_response_code(400);
    echo json_encode(['ok' => false, 'error' => 'Invalid JSON body']);
    exit;
}

// Validate API key (constant-time compare to prevent timing attacks)
$provided = $data['key'] ?? '';
if (!hash_equals(API_KEY, $provided)) {
    http_response_code(403);
    echo json_encode(['ok' => false, 'error' => 'Forbidden']);
    exit;
}

// Validate file name against whitelist
$file = $data['file'] ?? '';
if (!in_array($file, ALLOWED_FILES, true)) {
    http_response_code(400);
    echo json_encode(['ok' => false, 'error' => 'Invalid file. Allowed: ' . implode(', ', ALLOWED_FILES)]);
    exit;
}

// Validate content
if (!isset($data['content']) || !is_string($data['content'])) {
    http_response_code(400);
    echo json_encode(['ok' => false, 'error' => 'Missing or invalid content']);
    exit;
}

// Write file (atomic via temp file + rename)
$path    = FILE_DIR . DIRECTORY_SEPARATOR . $file;
$tmpPath = $path . '.tmp.' . getmypid();
$written = file_put_contents($tmpPath, $data['content']);

if ($written === false) {
    http_response_code(500);
    echo json_encode(['ok' => false, 'error' => 'Failed to write temp file (check directory permissions)']);
    exit;
}

if (!rename($tmpPath, $path)) {
    @unlink($tmpPath);
    http_response_code(500);
    echo json_encode(['ok' => false, 'error' => 'Failed to move temp file to destination']);
    exit;
}

echo json_encode([
    'ok'        => true,
    'file'      => $file,
    'bytes'     => $written,
    'updatedAt' => gmdate('c'),
]);
