const http = require('node:http');
const fs = require('node:fs');
const path = require('node:path');
const { Readable } = require('node:stream');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 7000;
const ROOT_DIR = process.cwd(); // Current working directory as root

// Published builds live in git history, not on disk: the checkout only keeps
// the newest index.html. Older ones are read straight from the data-store repo
// on GitHub, so hosting every version costs no extra storage here.
const REPO = process.env.DATA_STORE_REPO_SLUG || 'Breakmine-Team/data-store';
const RAW_BASE = process.env.DATA_STORE_RAW_BASE || `https://raw.githubusercontent.com/${REPO}`;
const BUILD_MANIFEST = path.join(ROOT_DIR, 'builds.json');

// Common MIME types for file serving
const MIME_TYPES = {
  '.html': 'text/html',
  '.css': 'text/css',
  '.js': 'text/javascript',
  '.json': 'application/json',
  '.png': 'image/png',
  '.jpg': 'image/jpeg',
  '.gif': 'image/gif',
  '.svg': 'image/svg+xml',
  '.ico': 'image/x-icon',
  '.txt': 'text/plain'
};

const SHA_PATTERN = /^[0-9a-f]{7,40}$/i;

let manifestCache = { mtimeMs: -1, versions: {} };

// builds.json is written by scripts/publish-single.js:
//   { "versions": { "1.6.9b": { "sha": "<commit>", "published": "<iso date>" } } }
function readBuildManifest() {
  let stats;
  try {
    stats = fs.statSync(BUILD_MANIFEST);
  } catch {
    return {};
  }

  if (stats.mtimeMs !== manifestCache.mtimeMs) {
    try {
      const parsed = JSON.parse(fs.readFileSync(BUILD_MANIFEST, 'utf8'));
      manifestCache = {
        mtimeMs: stats.mtimeMs,
        versions: (parsed && parsed.versions) || {}
      };
    } catch (err) {
      console.warn('[datastore] could not read builds.json:', err.message);
      return {};
    }
  }

  return manifestCache.versions;
}

// Routes that are answered from git history:
//   /build/<commit>/index.html   exact published commit
//   /<version>/index.html        release version, via builds.json
//   /<version>/                  same, directory style
function lookupArchivedBuild(pathname) {
  const bySha = pathname.match(/^\/build\/([0-9a-f]{7,40})(?:\/index\.html)?\/?$/i);
  if (bySha) {
    return { sha: bySha[1] };
  }

  const byVersion = pathname.match(/^\/([^/]+?)(?:\/index\.html)?\/?$/);
  if (byVersion) {
    const entry = readBuildManifest()[byVersion[1]];
    if (entry && typeof entry.sha === 'string' && SHA_PATTERN.test(entry.sha)) {
      return { sha: entry.sha };
    }
  }

  return null;
}

// The file GitHub serves is text/plain, which a browser would show as source.
// Fetch it here and hand it back as text/html so a version is playable directly.
async function sendArchivedBuild(sha, req, res) {
  const url = `${RAW_BASE}/${sha}/index.html`;

  try {
    const upstream = await fetch(url, { method: req.method === 'HEAD' ? 'HEAD' : 'GET' });
    if (!upstream.ok) {
      res.writeHead(404, { 'Content-Type': 'text/plain' });
      res.end(`404 Not Found (build ${sha} has no index.html)`);
      return;
    }

    // No Content-Length: fetch() transparently decompresses the gzipped raw
    // file, so the upstream length is the compressed size and would truncate
    // the page. Chunked is correct and the browser reads to the end.
    res.writeHead(200, {
      'Content-Type': 'text/html; charset=utf-8',
      'Cache-Control': 'public, max-age=3600',
      'Access-Control-Allow-Origin': '*'
    });
    if (req.method === 'HEAD' || !upstream.body) {
      res.end();
      return;
    }

    const stream = Readable.fromWeb(upstream.body);
    stream.on('error', () => res.destroy());
    stream.pipe(res);
  } catch (err) {
    console.warn(`[datastore] could not fetch ${url}:`, err.message);
    res.writeHead(502, { 'Content-Type': 'text/plain' });
    res.end('502 Bad Gateway');
  }
}

// 1. Create HTTP Server for standard static file delivery
const server = http.createServer((req, res) => {
  let pathname;
  try {
    pathname = decodeURIComponent(new URL(req.url, 'http://localhost').pathname);
  } catch {
    res.writeHead(400, { 'Content-Type': 'text/plain' });
    res.end('400 Bad Request');
    return;
  }

  // Control characters would be rejected by path handling further down.
  if (pathname.indexOf('\0') !== -1) {
    res.writeHead(400, { 'Content-Type': 'text/plain' });
    res.end('400 Bad Request');
    return;
  }

  let safePath = path.normalize(pathname).replace(/^(\.\.[\/\\])+/, '');
  let filePath = path.join(ROOT_DIR, safePath === '/' || safePath === '.' ? 'index.html' : safePath);

  if (!filePath.startsWith(ROOT_DIR)) {
    res.writeHead(403, { 'Content-Type': 'text/plain' });
    res.end('403 Forbidden');
    return;
  }

  fs.stat(filePath, (err, stats) => {
    if (!err) {
      if (stats.isDirectory()) {
        filePath = path.join(filePath, 'index.html');
        fs.stat(filePath, (err2, stats2) => {
          if (err2 || !stats2.isFile()) {
            res.writeHead(404, { 'Content-Type': 'text/plain' });
            res.end('404 Not Found');
            return;
          }
          sendFile(filePath, req, res);
        });
        return;
      }

      if (!stats.isFile()) {
        res.writeHead(404, { 'Content-Type': 'text/plain' });
        res.end('404 Not Found');
        return;
      }

      sendFile(filePath, req, res);
      return;
    }

    const archived = lookupArchivedBuild(pathname);
    if (archived) {
      sendArchivedBuild(archived.sha, req, res);
      return;
    }

    res.writeHead(404, { 'Content-Type': 'text/plain' });
    res.end('404 Not Found');
  });
});

function sendFile(filePath, req, res) {
  const ext = path.extname(filePath).toLowerCase();
  const contentType = MIME_TYPES[ext] || 'application/octet-stream';

  res.writeHead(200, { 'Content-Type': contentType });
  if (req.method === 'HEAD') {
    res.end();
    return;
  }
  fs.createReadStream(filePath).pipe(res);
}

// 2. Attach WebSocket Server to serve files and handle messages
const wss = new WebSocketServer({ server });

wss.on('connection', (ws) => {
  console.log('Client connected via WebSocket');

  ws.on('message', (rawMessage) => {
    let request;

    // Parse incoming message (expecting JSON or plain string path)
    try {
      request = JSON.parse(rawMessage);
    } catch {
      request = { path: rawMessage.toString().trim() };
    }

    // If the client requests a file via WebSocket
    if (request.action === 'getFile' || request.path) {
      const requestedPath = request.path || request.file;
      let safePath = path.normalize(requestedPath).replace(/^(\.\.[\/\\])+/, '');
      let filePath = path.join(ROOT_DIR, safePath);

      // Security check: ensure path stays within root directory
      if (!filePath.startsWith(ROOT_DIR)) {
        ws.send(JSON.stringify({ error: '403 Forbidden', path: requestedPath }));
        return;
      }

      fs.readFile(filePath, (err, data) => {
        if (err) {
          ws.send(JSON.stringify({ error: '404 Not Found', path: requestedPath }));
          return;
        }

        const ext = path.extname(filePath).toLowerCase();
        const contentType = MIME_TYPES[ext] || 'application/octet-stream';

        // Send back file metadata and content (as base64 or text depending on type)
        const isBinary = ['.png', '.jpg', '.gif', '.ico', '.svg'].includes(ext);
        const payload = {
          path: requestedPath,
          contentType: contentType,
          encoding: isBinary ? 'base64' : 'utf8',
          content: isBinary ? data.toString('base64') : data.toString('utf8')
        };

        ws.send(JSON.stringify(payload));
      });
    } else {
      // Fallback regular message handling
      ws.send(JSON.stringify({ message: `Received: ${rawMessage}` }));
    }
  });

  ws.on('close', () => {
    console.log('Client disconnected');
  });
});

// 3. Start listening on 0.0.0.0
server.listen(PORT, '0.0.0.0', () => {
  console.log(`Server running at http://0.0.0.0:${PORT}`);
});
