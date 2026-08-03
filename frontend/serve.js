const http = require('http');
const fs = require('fs');
const path = require('path');

// Remove proxy env vars that would route backend requests through system proxy
delete process.env.HTTP_PROXY;
delete process.env.http_proxy;
delete process.env.HTTPS_PROXY;
delete process.env.https_proxy;
delete process.env.ALL_PROXY;
delete process.env.all_proxy;

const dir = path.join(__dirname, 'public');
const mime = {
    '.html': 'text/html; charset=utf-8',
    '.js': 'application/javascript; charset=utf-8',
    '.css': 'text/css',
    '.json': 'application/json',
    '.png': 'image/png',
    '.svg': 'image/svg+xml'
};

// API proxy: forward /api/* requests to backend
const BACKEND = {
    host: process.env.BACKEND_HOST || 'localhost',
    port: parseInt(process.env.BACKEND_PORT || '8080', 10)
};
const PORT = parseInt(process.env.PORT || '3000', 10);
function proxyApi(req, res, url) {
  const opts = {
    hostname: BACKEND.host,
    port: BACKEND.port,
    path: url,
    method: req.method,
    headers: Object.assign({}, req.headers, { host: BACKEND.host + ':' + BACKEND.port })
  };
  const proxyReq = http.request(opts, function(proxyRes) {
    res.writeHead(proxyRes.statusCode, proxyRes.headers);
    proxyRes.pipe(res);
  });
  proxyReq.on('error', function(err) {
    res.writeHead(502, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ error: 'Backend unreachable: ' + err.message }));
  });
  req.pipe(proxyReq);
}

http.createServer((req, res) => {
    // Proxy API requests to backend
    if (req.url && req.url.startsWith('/api/')) {
      proxyApi(req, res, req.url);
      return;
    }
    let file = req.url === '/' ? '/index.html' : req.url;
    file = path.join(dir, file);
    fs.readFile(file, (err, data) => {
        if (err) {
            res.writeHead(404);
            res.end('Not Found');
            return;
        }
        const ext = path.extname(file);
        res.writeHead(200, {
            'Content-Type': mime[ext] || 'text/plain',
            'Access-Control-Allow-Origin': '*',
            'Cache-Control': 'no-store, no-cache, must-revalidate',
            'Pragma': 'no-cache'
        });
        res.end(data);
    });
}).listen(PORT, '0.0.0.0', () => console.log('Frontend running on http://localhost:' + PORT));
