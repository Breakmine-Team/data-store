const http = require('node:http');
const fs = require('node:fs');
const path = require('node:path');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 7000;
const ROOT_DIR = process.cwd(); // Current working directory as root

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

// 1. Create HTTP Server for standard static file delivery
const server = http.createServer((req, res) => {
  let safePath = path.normalize(req.url).replace(/^(\.\.[\/\\])+/, '');
  let filePath = path.join(ROOT_DIR, safePath === '/' ? 'index.html' : safePath);

  if (!filePath.startsWith(ROOT_DIR)) {
    res.writeHead(403, { 'Content-Type': 'text/plain' });
    res.end('403 Forbidden');
    return;
  }

  fs.stat(filePath, (err, stats) => {
    if (err) {
      res.writeHead(404, { 'Content-Type': 'text/plain' });
      res.end('404 Not Found');
      return;
    }

    if (stats.isDirectory()) {
      filePath = path.join(filePath, 'index.html');
      fs.stat(filePath, (err2, stats2) => {
        if (err2 || !stats2.isFile()) {
          res.writeHead(404, { 'Content-Type': 'text/plain' });
          res.end('404 Not Found');
          return;
        }
        sendFile(filePath, res);
      });
      return;
    }

    sendFile(filePath, res);
  });
});

function sendFile(filePath, res) {
  const ext = path.extname(filePath).toLowerCase();
  const contentType = MIME_TYPES[ext] || 'application/octet-stream';
  
  res.writeHead(200, { 'Content-Type': contentType });
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
