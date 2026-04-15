package com.lsmtree.server;

import com.lsmtree.store.LSMStore;
import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;
import java.util.SortedMap;
import java.util.concurrent.Executors;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Minimal HTTP REST front-end for {@link LSMStore}.
 *
 * <pre>
 * GET    /get?key=foo          → {"value":"bar"}  or 404
 * POST   /put                  body: {"key":"foo","value":"bar"}  → 200
 * DELETE /delete?key=foo       → 200
 * GET    /scan?prefix=foo      → [{"key":"k","value":"v"}, ...]
 * POST   /flush                → {"ok":true}
 * POST   /compact              → {"ok":true}
 * GET    /debug                → structured JSON snapshot
 * </pre>
 *
 * <p>All responses include CORS headers so a browser can call the API directly.
 * Start with {@code main()} or pass a data-directory path as the first argument.
 */
public class RestServer {

    private static final int PORT = 8080;

    public static void main(String[] args) throws IOException {
        Path dataDir = args.length > 0 ? Path.of(args[0]) : Path.of("data");
        Files.createDirectories(dataDir);

        LSMStore store = new LSMStore(dataDir);

        HttpServer server = HttpServer.create(new InetSocketAddress(PORT), /*backlog*/ 32);
        server.createContext("/get",     new GetHandler(store));
        server.createContext("/put",     new PutHandler(store));
        server.createContext("/delete",  new DeleteHandler(store));
        server.createContext("/scan",    new ScanHandler(store));
        server.createContext("/flush",   new FlushHandler(store));
        server.createContext("/compact", new CompactHandler(store));
        server.createContext("/debug",   new DebugHandler(store));
        // Serve index.html at / — must be registered last so specific paths take priority
        server.createContext("/",        new DashboardHandler());
        server.setExecutor(Executors.newFixedThreadPool(8));
        server.start();

        System.out.printf("LSM-Tree REST server on http://localhost:%d%n", PORT);
        System.out.println("  GET    /get?key=<k>");
        System.out.println("  POST   /put          {\"key\":\"k\",\"value\":\"v\"}");
        System.out.println("  DELETE /delete?key=<k>");
        System.out.println("  GET    /scan?prefix=<p>");
        System.out.println("  POST   /flush");
        System.out.println("  POST   /compact");
        System.out.println("  GET    /debug");
        System.out.println("\nPress Ctrl+C to stop.");

        // Flush + close on shutdown
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("\nShutting down…");
            server.stop(1);
            try { store.close(); } catch (IOException e) { e.printStackTrace(); }
        }, "shutdown"));
    }

    // =========================================================================
    // Base handler — CORS, routing helpers, JSON utilities
    // =========================================================================

    abstract static class BaseHandler implements HttpHandler {

        protected final LSMStore store;

        BaseHandler(LSMStore store) { this.store = store; }

        @Override
        public final void handle(HttpExchange ex) throws IOException {
            addCors(ex.getResponseHeaders());

            // Respond to CORS preflight immediately
            if ("OPTIONS".equalsIgnoreCase(ex.getRequestMethod())) {
                ex.sendResponseHeaders(204, -1);
                ex.close();
                return;
            }

            try {
                dispatch(ex);
            } catch (IllegalArgumentException e) {
                sendJson(ex, 400, error(e.getMessage()));
            } catch (Exception e) {
                sendJson(ex, 500, error(e.getClass().getSimpleName() + ": " + e.getMessage()));
            }
        }

        abstract void dispatch(HttpExchange ex) throws Exception;

        // ── HTTP helpers ──────────────────────────────────────────────────────

        void sendJson(HttpExchange ex, int status, String body) throws IOException {
            byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
            ex.getResponseHeaders().set("Content-Type", "application/json; charset=utf-8");
            ex.sendResponseHeaders(status, bytes.length);
            try (OutputStream os = ex.getResponseBody()) {
                os.write(bytes);
            }
        }

        /** Assert the request uses the expected HTTP method; send 405 otherwise. */
        boolean requireMethod(HttpExchange ex, String method) throws IOException {
            if (method.equalsIgnoreCase(ex.getRequestMethod())) return true;
            ex.getResponseHeaders().set("Allow", method);
            sendJson(ex, 405, error("Method not allowed — use " + method));
            return false;
        }

        String readBody(HttpExchange ex) throws IOException {
            return new String(ex.getRequestBody().readAllBytes(), StandardCharsets.UTF_8).trim();
        }

        /** Extract a query-string parameter, URL-decoded. Returns {@code null} if absent. */
        String queryParam(HttpExchange ex, String name) {
            String query = ex.getRequestURI().getRawQuery();
            if (query == null) return null;
            for (String pair : query.split("&")) {
                int eq = pair.indexOf('=');
                if (eq < 0) continue;
                String k = URLDecoder.decode(pair.substring(0, eq), StandardCharsets.UTF_8);
                if (k.equals(name)) {
                    return URLDecoder.decode(pair.substring(eq + 1), StandardCharsets.UTF_8);
                }
            }
            return null;
        }

        // ── JSON builders ─────────────────────────────────────────────────────

        /** Escape and quote a string for embedding in JSON. {@code null} → {@code "null"}. */
        static String js(String s) {
            return LSMStore.jsonStr(s);
        }

        /** Wrap a message in a JSON error object. */
        static String error(String msg) {
            return "{\"error\":" + js(msg) + "}";
        }

        // ── CORS ──────────────────────────────────────────────────────────────

        private static void addCors(Headers h) {
            h.set("Access-Control-Allow-Origin",  "*");
            h.set("Access-Control-Allow-Methods", "GET, POST, DELETE, OPTIONS");
            h.set("Access-Control-Allow-Headers", "Content-Type");
        }
    }

    // =========================================================================
    // Individual handlers
    // =========================================================================

    // GET /  →  serves index.html from the classpath
    static class DashboardHandler implements HttpHandler {

        // Read once at startup as a UTF-8 string, then cache the encoded bytes.
        private final byte[] html;

        DashboardHandler() throws IOException {
            try (InputStream is = getClass().getResourceAsStream("/index.html")) {
                if (is == null) throw new IOException("index.html not found on classpath");
                String content = new String(is.readAllBytes(), StandardCharsets.UTF_8);
                html = content.getBytes(StandardCharsets.UTF_8);
            }
        }

        @Override
        public void handle(HttpExchange ex) throws IOException {
            // Only serve on GET / or GET /index.html; return 404 for unknown paths
            String path = ex.getRequestURI().getPath();
            if (!"GET".equalsIgnoreCase(ex.getRequestMethod())
                    || (!path.equals("/") && !path.equals("/index.html"))) {
                ex.sendResponseHeaders(404, -1);
                ex.close();
                return;
            }
            ex.getResponseHeaders().set("Content-Type", "text/html; charset=utf-8");
            ex.sendResponseHeaders(200, html.length);
            try (OutputStream os = ex.getResponseBody()) {
                os.write(html);
            }
        }
    }

    // GET /get?key=foo
    static class GetHandler extends BaseHandler {
        GetHandler(LSMStore store) { super(store); }

        @Override
        void dispatch(HttpExchange ex) throws Exception {
            if (!requireMethod(ex, "GET")) return;

            String key = queryParam(ex, "key");
            if (key == null || key.isBlank()) {
                sendJson(ex, 400, error("Missing query parameter: key"));
                return;
            }

            Optional<String> value = store.get(key);
            if (value.isEmpty()) {
                sendJson(ex, 404, "{\"error\":\"key not found\"}");
            } else {
                sendJson(ex, 200, "{\"key\":" + js(key) + ",\"value\":" + js(value.get()) + "}");
            }
        }
    }

    // POST /put   body: {"key":"foo","value":"bar"}
    static class PutHandler extends BaseHandler {
        PutHandler(LSMStore store) { super(store); }

        @Override
        void dispatch(HttpExchange ex) throws Exception {
            if (!requireMethod(ex, "POST")) return;

            String body  = readBody(ex);
            String key   = jsonField(body, "key");
            String value = jsonField(body, "value");

            if (key == null)   { sendJson(ex, 400, error("JSON body must contain \"key\"")); return; }
            if (value == null) { sendJson(ex, 400, error("JSON body must contain \"value\"")); return; }

            store.put(key, value);
            sendJson(ex, 200, "{\"ok\":true}");
        }
    }

    // DELETE /delete?key=foo
    static class DeleteHandler extends BaseHandler {
        DeleteHandler(LSMStore store) { super(store); }

        @Override
        void dispatch(HttpExchange ex) throws Exception {
            if (!requireMethod(ex, "DELETE")) return;

            String key = queryParam(ex, "key");
            if (key == null || key.isBlank()) {
                sendJson(ex, 400, error("Missing query parameter: key"));
                return;
            }

            store.delete(key);
            sendJson(ex, 200, "{\"ok\":true}");
        }
    }

    // GET /scan?prefix=foo
    static class ScanHandler extends BaseHandler {
        ScanHandler(LSMStore store) { super(store); }

        @Override
        void dispatch(HttpExchange ex) throws Exception {
            if (!requireMethod(ex, "GET")) return;

            String prefix = queryParam(ex, "prefix");
            // null prefix → scan everything
            SortedMap<String, String> results = store.scan(prefix);

            StringBuilder sb = new StringBuilder("[");
            boolean first = true;
            for (Map.Entry<String, String> e : results.entrySet()) {
                if (!first) sb.append(",");
                first = false;
                sb.append("{\"key\":").append(js(e.getKey()))
                  .append(",\"value\":").append(js(e.getValue()))
                  .append("}");
            }
            sb.append("]");
            sendJson(ex, 200, sb.toString());
        }
    }

    // POST /flush
    static class FlushHandler extends BaseHandler {
        FlushHandler(LSMStore store) { super(store); }

        @Override
        void dispatch(HttpExchange ex) throws Exception {
            if (!requireMethod(ex, "POST")) return;
            store.flush();
            sendJson(ex, 200, "{\"ok\":true,\"sstables\":" + store.ssTableCount() + "}");
        }
    }

    // POST /compact
    static class CompactHandler extends BaseHandler {
        CompactHandler(LSMStore store) { super(store); }

        @Override
        void dispatch(HttpExchange ex) throws Exception {
            if (!requireMethod(ex, "POST")) return;
            int before = store.ssTableCount();
            store.compact();
            int after = store.ssTableCount();
            sendJson(ex, 200,
                    "{\"ok\":true,\"before\":" + before + ",\"after\":" + after + "}");
        }
    }

    // GET /debug
    static class DebugHandler extends BaseHandler {
        DebugHandler(LSMStore store) { super(store); }

        @Override
        void dispatch(HttpExchange ex) throws Exception {
            if (!requireMethod(ex, "GET")) return;
            sendJson(ex, 200, store.debugJson());
        }
    }

    // =========================================================================
    // Minimal JSON field extractor (no external dependencies)
    // =========================================================================

    // Matches:  "fieldName"  :  "string value with \" or \\ escapes"
    private static final Pattern JSON_STRING_FIELD =
            Pattern.compile("\"([^\"]+)\"\\s*:\\s*\"((?:[^\"\\\\]|\\\\.)*)\"");

    /**
     * Extract the string value of {@code field} from a flat JSON object body.
     * Handles {@code \"} and {@code \\} escape sequences only — sufficient for
     * the simple {@code {"key":"…","value":"…"}} payloads this server accepts.
     *
     * @return the unescaped value, or {@code null} if the field is absent.
     */
    static String jsonField(String body, String field) {
        Matcher m = JSON_STRING_FIELD.matcher(body);
        while (m.find()) {
            if (m.group(1).equals(field)) {
                return m.group(2)
                        .replace("\\\"", "\"")
                        .replace("\\\\", "\\")
                        .replace("\\n",  "\n")
                        .replace("\\r",  "\r")
                        .replace("\\t",  "\t");
            }
        }
        return null;
    }
}
