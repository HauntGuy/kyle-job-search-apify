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

// Repair double-encoded UTF-8 (Windows cp1252 issue).
// When Python on Windows pipes Unicode through cp1252 stdout, em-dashes (E2 80 94)
// become C3 A2 C2 80 C2 94 (each byte re-encoded as UTF-8). Detect and fix this.
// Signature: C3 A2 followed by C2 80..C2 BF is a double-encoded sequence.
$content = $data['content'];
$repaired = false;
if (preg_match('/\xC3\xA2[\xC2\xC3][\x80-\xBF]/', $content)) {
    // Attempt repair: decode UTF-8 bytes as Latin-1 to undo the double-encoding
    $candidate = mb_convert_encoding($content, 'ISO-8859-1', 'UTF-8');
    // Verify the result is valid UTF-8
    if (mb_check_encoding($candidate, 'UTF-8')) {
        $content = $candidate;
        $repaired = true;
    }
}

// Write file (atomic via temp file + rename)
$path    = FILE_DIR . DIRECTORY_SEPARATOR . $file;
$tmpPath = $path . '.tmp.' . getmypid();
$written = file_put_contents($tmpPath, $content);

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

$response = [
    'ok'        => true,
    'file'      => $file,
    'bytes'     => $written,
    'updatedAt' => gmdate('c'),
];
if ($repaired) {
    $response['warning'] = 'Double-encoded UTF-8 detected and repaired (Windows cp1252 issue)';
}
echo json_encode($response);
