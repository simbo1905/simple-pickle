import com.sun.net.httpserver.*;
import java.io.*;
import java.net.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.*;
import java.util.function.*;

public class Server {
  
  record ServerInfo(String server, String version, java.time.Instant startTime, Path dataDir, Path assetsDir) {
    String uptime() {
      return java.time.Duration.between(startTime, java.time.Instant.now()).toString();
    }
  }
  
  @FunctionalInterface
  interface EndpointHandler {
    void handle(HttpExchange exchange, ServerInfo serverInfo) throws IOException;
  }
  
  record EndpointSpec(String path, String method, String description, EndpointHandler handler) {}
  
  static final List<EndpointSpec> ENDPOINTS = List.of(
    new EndpointSpec("/", "GET", "Serves static files (index.html, CSS, JS). Main UI interface.", Server::handleStatic),
    new EndpointSpec("/api/jmh-results", "GET", "Returns latest JMH results with filename", Server::handleJMHResults),
    new EndpointSpec("/api/sizes", "GET", "Returns latest sizes data", Server::handleSizes),
    new EndpointSpec("/api/search", "GET", "Searches JMH files. Query param: ?type=jmh&q=searchterm. Returns array of matching filenames.", Server::handleSearch),
    new EndpointSpec("/api/jmh-file", "GET", "Loads specific JMH file. Query param: ?name=filename. Returns file data.", Server::handleJMHFile),
    new EndpointSpec("/info", "GET", "Returns server configuration, status, and API documentation", Server::handleInfo),
    // Legacy endpoints for backward compatibility
    new EndpointSpec("/api/convert", "POST", "Converts NJSON format to visualization format. POST body: raw NJSON content.", Server::handleConvert),
    new EndpointSpec("/api/results", "GET", "Returns latest benchmark results with filename in visualization format", Server::handleResults),
    new EndpointSpec("/api/file", "GET", "Loads specific result file. Query param: ?name=filename. Returns file data in visualization format.", Server::handleFile)
  );

  public static void main(String[] args) throws Exception {
    var server = HttpServer.create(new InetSocketAddress(8080), 0);
    var serverInfo = new ServerInfo("NFP Benchmark Visualizer", "2.0", java.time.Instant.now(), 
                                   args.length > 0 ? Path.of(args[0]) : Path.of("."), Path.of("assets"));

    // Register all endpoints using the same list that /info uses
    for (var endpoint : ENDPOINTS) {
      server.createContext(endpoint.path(), exchange -> {
        try {
          endpoint.handler().handle(exchange, serverInfo);
        } catch (IOException e) {
          e.printStackTrace();
          exchange.sendResponseHeaders(500, 0);
          exchange.getResponseBody().close();
        }
      });
    }

    server.setExecutor(Executors.newVirtualThreadPerTaskExecutor());
    server.start();
    System.out.println("Server running at http://localhost:8080");
    System.out.println("Assets directory: " + serverInfo.assetsDir());
    System.out.println("Data directory: " + serverInfo.dataDir());
    System.out.println("Press Ctrl+C to stop");
  }

  static void handleStatic(HttpExchange exchange, ServerInfo serverInfo) throws IOException {
    var path = exchange.getRequestURI().getPath();
    if (path.equals("/")) path = "/index.html";

    var file = serverInfo.assetsDir().resolve(path.substring(1));
    if (Files.exists(file) && !Files.isDirectory(file)) {
      var contentType = getContentType(file.getFileName().toString());
      exchange.getResponseHeaders().set("Content-Type", contentType);
      exchange.sendResponseHeaders(200, Files.size(file));
      try (var os = exchange.getResponseBody()) {
        Files.copy(file, os);
      }
    } else {
      exchange.sendResponseHeaders(404, 0);
      exchange.getResponseBody().close();
    }
  }

  static void handleConvert(HttpExchange exchange, ServerInfo serverInfo) throws IOException {
    if (!"POST".equals(exchange.getRequestMethod())) {
      exchange.sendResponseHeaders(405, 0);
      exchange.getResponseBody().close();
      return;
    }

    var input = new String(exchange.getRequestBody().readAllBytes());
    var converted = convertNJsonToExpectedFormat(input);

    exchange.getResponseHeaders().set("Content-Type", "application/json");
    exchange.getResponseHeaders().set("Access-Control-Allow-Origin", "*");
    var response = converted.getBytes();
    exchange.sendResponseHeaders(200, response.length);
    try (var os = exchange.getResponseBody()) {
      os.write(response);
    }
  }

  static void handleResults(HttpExchange exchange, ServerInfo serverInfo) throws IOException {
    var results = loadLatestResults(serverInfo.dataDir());
    var filename = getLatestFilename(serverInfo.dataDir());
    var responseJson = String.format("""
      {
        "filename": "%s",
        "data": %s
      }
      """, filename, results);
    
    exchange.getResponseHeaders().set("Content-Type", "application/json");
    exchange.getResponseHeaders().set("Access-Control-Allow-Origin", "*");
    var response = responseJson.getBytes();
    exchange.sendResponseHeaders(200, response.length);
    try (var os = exchange.getResponseBody()) {
      os.write(response);
    }
  }

  static void handleSearch(HttpExchange exchange, ServerInfo serverInfo) throws IOException {
    var query = "";
    var type = "";
    if ("GET".equals(exchange.getRequestMethod())) {
      var uri = exchange.getRequestURI().toString();
      if (uri.contains("?")) {
        var queryString = uri.substring(uri.indexOf("?") + 1);
        var params = queryString.split("&");
        for (var param : params) {
          var kv = param.split("=", 2);
          if (kv.length == 2) {
            var key = java.net.URLDecoder.decode(kv[0], "UTF-8");
            var value = java.net.URLDecoder.decode(kv[1], "UTF-8");
            if ("q".equals(key)) query = value;
            if ("type".equals(key)) type = value;
          }
        }
      }
    }
    
    String searchResults;
    if ("jmh".equals(type)) {
      searchResults = searchJMHFiles(serverInfo.dataDir(), query);
    } else {
      searchResults = searchResultFiles(serverInfo.dataDir(), query);
    }
    
    exchange.getResponseHeaders().set("Content-Type", "application/json");
    exchange.getResponseHeaders().set("Access-Control-Allow-Origin", "*");
    var response = searchResults.getBytes();
    exchange.sendResponseHeaders(200, response.length);
    try (var os = exchange.getResponseBody()) {
      os.write(response);
    }
  }

  static void handleFile(HttpExchange exchange, ServerInfo serverInfo) throws IOException {
    var filename = "";
    if ("GET".equals(exchange.getRequestMethod())) {
      var uri = exchange.getRequestURI().toString();
      if (uri.contains("?name=")) {
        filename = uri.substring(uri.indexOf("?name=") + 6);
        filename = java.net.URLDecoder.decode(filename, "UTF-8");
      }
    }
    
    var results = loadSpecificFile(serverInfo.dataDir(), filename);
    var responseJson = String.format("""
      {
        "filename": "%s",
        "data": %s
      }
      """, filename, results);
    
    exchange.getResponseHeaders().set("Content-Type", "application/json");
    exchange.getResponseHeaders().set("Access-Control-Allow-Origin", "*");
    var response = responseJson.getBytes();
    exchange.sendResponseHeaders(200, response.length);
    try (var os = exchange.getResponseBody()) {
      os.write(response);
    }
  }

  static void handleJMHResults(HttpExchange exchange, ServerInfo serverInfo) throws IOException {
    var results = loadLatestJMHResults(serverInfo.dataDir());
    var filename = getLatestJMHFilename(serverInfo.dataDir());
    var responseJson = String.format("""
      {
        "filename": "%s",
        "data": %s
      }
      """, filename, results);
    
    exchange.getResponseHeaders().set("Content-Type", "application/json");
    exchange.getResponseHeaders().set("Access-Control-Allow-Origin", "*");
    var response = responseJson.getBytes();
    exchange.sendResponseHeaders(200, response.length);
    try (var os = exchange.getResponseBody()) {
      os.write(response);
    }
  }

  static void handleSizes(HttpExchange exchange, ServerInfo serverInfo) throws IOException {
    var sizes = loadLatestSizes(serverInfo.dataDir());
    var filename = getLatestSizesFilename(serverInfo.dataDir());
    var responseJson = String.format("""
      {
        "filename": "%s",
        "data": %s
      }
      """, filename, sizes);
    
    exchange.getResponseHeaders().set("Content-Type", "application/json");
    exchange.getResponseHeaders().set("Access-Control-Allow-Origin", "*");
    var response = responseJson.getBytes();
    exchange.sendResponseHeaders(200, response.length);
    try (var os = exchange.getResponseBody()) {
      os.write(response);
    }
  }

  static void handleJMHFile(HttpExchange exchange, ServerInfo serverInfo) throws IOException {
    var filename = "";
    if ("GET".equals(exchange.getRequestMethod())) {
      var uri = exchange.getRequestURI().toString();
      if (uri.contains("?name=")) {
        filename = uri.substring(uri.indexOf("?name=") + 6);
        filename = java.net.URLDecoder.decode(filename, "UTF-8");
      }
    }
    
    var results = loadSpecificJMHFile(serverInfo.dataDir(), filename);
    var responseJson = String.format("""
      {
        "filename": "%s",
        "data": %s
      }
      """, filename, results);
    
    exchange.getResponseHeaders().set("Content-Type", "application/json");
    exchange.getResponseHeaders().set("Access-Control-Allow-Origin", "*");
    var response = responseJson.getBytes();
    exchange.sendResponseHeaders(200, response.length);
    try (var os = exchange.getResponseBody()) {
      os.write(response);
    }
  }

  static void handleInfo(HttpExchange exchange, ServerInfo serverInfo) throws IOException {
    var info = new StringBuilder();
    info.append("Server Info:\n");
    info.append(serverInfo.toString()).append("\n");
    info.append("Uptime: ").append(serverInfo.uptime()).append("\n\n");
    info.append("Endpoints:\n");
    for (var endpoint : ENDPOINTS) {
      info.append(endpoint.path()).append(" ").append(endpoint.method()).append(" - ").append(endpoint.description()).append("\n");
    }
    
    exchange.getResponseHeaders().set("Content-Type", "text/plain");
    exchange.getResponseHeaders().set("Access-Control-Allow-Origin", "*");
    var response = info.toString().getBytes();
    exchange.sendResponseHeaders(200, response.length);
    try (var os = exchange.getResponseBody()) {
      os.write(response);
    }
  }

  static String getContentType(String fileName) {
    if (fileName.endsWith(".html")) return "text/html";
    if (fileName.endsWith(".js")) return "application/javascript";
    if (fileName.endsWith(".css")) return "text/css";
    if (fileName.endsWith(".json")) return "application/json";
    if (fileName.endsWith(".png")) return "image/png";
    return "application/octet-stream";
  }

  static String convertNJsonToExpectedFormat(String njsonContent) {
    var lines = njsonContent.lines()
        .filter(line -> !line.trim().isEmpty())
        .toList();

    var results = new ArrayList<Map<String, Object>>();

    for (var line : lines) {
      try {
        // Parse each line as JSON
        var fields = parseSimpleJson(line);

        var converted = new HashMap<String, Object>();
        converted.put("benchmark", fields.get("benchmark"));
        converted.put("ts", fields.get("ts"));
        converted.put("size", Integer.parseInt(fields.get("size")));
        converted.put("src", fields.get("src"));
        converted.put("comment", fields.get("comment"));

        var primaryMetric = new HashMap<String, Object>();
        primaryMetric.put("score", Double.parseDouble(fields.get("score")));
        primaryMetric.put("scoreError", fields.get("error").equals("NaN") ? 0.0 : Double.parseDouble(fields.get("error")));
        primaryMetric.put("scoreUnit", fields.get("units"));
        converted.put("primaryMetric", primaryMetric);

        results.add(converted);
      } catch (Exception e) {
        System.err.println("Error parsing line: " + line + " - " + e.getMessage());
      }
    }

    // Convert to JSON array string
    return toJsonArray(results);
  }

  static Map<String, String> parseSimpleJson(String json) {
    var result = new HashMap<String, String>();
    var cleaned = json.substring(1, json.length() - 1); // Remove { }
    var pairs = cleaned.split(", \"");

    for (var pair : pairs) {
      var kv = pair.replace("\"", "").split(": ", 2);
      if (kv.length == 2) {
        result.put(kv[0], kv[1]);
      }
    }
    return result;
  }

  static String toJsonArray(List<Map<String, Object>> list) {
    var sb = new StringBuilder("[");
    for (int i = 0; i < list.size(); i++) {
      if (i > 0) sb.append(",");
      sb.append("\n  ").append(toJson(list.get(i)));
    }
    sb.append("\n]");
    return sb.toString();
  }

  static String toJson(Map<String, Object> map) {
    var sb = new StringBuilder("{");
    var first = true;
    for (var entry : map.entrySet()) {
      if (!first) sb.append(", ");
      first = false;
      sb.append("\"").append(entry.getKey()).append("\": ");

      var value = entry.getValue();
      if (value instanceof String) {
        sb.append("\"").append(value).append("\"");
      } else if (value instanceof Map) {
        sb.append(toJson((Map<String, Object>) value));
      } else {
        sb.append(value);
      }
    }
    sb.append("}");
    return sb.toString();
  }

  static String loadLatestResults() {
    return loadLatestResults(Path.of("."));
  }

  static String loadLatestResults(Path dataDir) {
    try {
      // Find the latest results file
      var resultsFiles = Files.list(dataDir)
          .filter(p -> p.getFileName().toString().startsWith("results-") && p.getFileName().toString().endsWith(".njson"))
          .sorted((a, b) -> b.getFileName().toString().compareTo(a.getFileName().toString()))
          .toList();

      if (resultsFiles.isEmpty()) {
        return "[]";
      }

      var latestFile = resultsFiles.get(0);
      var njsonContent = Files.readString(latestFile);
      return convertNJsonToExpectedFormat(njsonContent);

    } catch (IOException e) {
      e.printStackTrace();
      return "[]";
    }
  }

  static String searchResultFiles(Path dataDir, String query) {
    try {
      var resultsFiles = Files.list(dataDir)
          .filter(p -> p.getFileName().toString().startsWith("results-") && p.getFileName().toString().endsWith(".njson"))
          .filter(p -> query.isEmpty() || p.getFileName().toString().toLowerCase().contains(query.toLowerCase()))
          .sorted((a, b) -> b.getFileName().toString().compareTo(a.getFileName().toString()))
          .map(p -> p.getFileName().toString())
          .toList();

      var sb = new StringBuilder("[");
      for (int i = 0; i < resultsFiles.size(); i++) {
        if (i > 0) sb.append(",");
        sb.append("\"").append(resultsFiles.get(i)).append("\"");
      }
      sb.append("]");
      return sb.toString();

    } catch (IOException e) {
      e.printStackTrace();
      return "[]";
    }
  }

  static String loadSpecificFile(Path dataDir, String filename) {
    try {
      var filePath = dataDir.resolve(filename);
      if (!Files.exists(filePath)) {
        return "[]";
      }

      // Handle JMH JSON files
      if (filename.startsWith("jmh-result-") && filename.endsWith(".json")) {
        return Files.readString(filePath);
      }
      
      // Handle NJSON result files
      if (filename.startsWith("results-") && filename.endsWith(".njson")) {
        var njsonContent = Files.readString(filePath);
        return convertNJsonToExpectedFormat(njsonContent);
      }
      
      return "[]";

    } catch (IOException e) {
      e.printStackTrace();
      return "[]";
    }
  }

  static String getLatestFilename(Path dataDir) {
    try {
      var resultsFiles = Files.list(dataDir)
          .filter(p -> p.getFileName().toString().startsWith("results-") && p.getFileName().toString().endsWith(".njson"))
          .sorted((a, b) -> b.getFileName().toString().compareTo(a.getFileName().toString()))
          .toList();

      return resultsFiles.isEmpty() ? "no-files-found" : resultsFiles.get(0).getFileName().toString();

    } catch (IOException e) {
      return "error-loading-files";
    }
  }

  // New JMH + Sizes helper functions
  static String loadLatestJMHResults(Path dataDir) {
    try {
      var jmhFiles = Files.list(dataDir)
          .filter(p -> p.getFileName().toString().startsWith("jmh-result-") && p.getFileName().toString().endsWith(".json"))
          .sorted((a, b) -> b.getFileName().toString().compareTo(a.getFileName().toString()))
          .toList();

      if (jmhFiles.isEmpty()) {
        return "[]";
      }

      var latestFile = jmhFiles.get(0);
      return Files.readString(latestFile);

    } catch (IOException e) {
      e.printStackTrace();
      return "[]";
    }
  }

  static String loadLatestSizes(Path dataDir) {
    try {
      var sizesFiles = Files.list(dataDir)
          .filter(p -> p.getFileName().toString().startsWith("sizes-") && p.getFileName().toString().endsWith(".json"))
          .sorted((a, b) -> b.getFileName().toString().compareTo(a.getFileName().toString()))
          .toList();

      if (sizesFiles.isEmpty()) {
        return "{}";
      }

      var latestFile = sizesFiles.get(0);
      return Files.readString(latestFile);

    } catch (IOException e) {
      e.printStackTrace();
      return "{}";
    }
  }

  static String searchJMHFiles(Path dataDir, String query) {
    try {
      var jmhFiles = Files.list(dataDir)
          .filter(p -> p.getFileName().toString().startsWith("jmh-result-") && p.getFileName().toString().endsWith(".json"))
          .filter(p -> query.isEmpty() || p.getFileName().toString().toLowerCase().contains(query.toLowerCase()))
          .sorted((a, b) -> b.getFileName().toString().compareTo(a.getFileName().toString()))
          .map(p -> p.getFileName().toString())
          .toList();

      var sb = new StringBuilder("[");
      for (int i = 0; i < jmhFiles.size(); i++) {
        if (i > 0) sb.append(",");
        sb.append("\"").append(jmhFiles.get(i)).append("\"");
      }
      sb.append("]");
      return sb.toString();

    } catch (IOException e) {
      e.printStackTrace();
      return "[]";
    }
  }

  static String loadSpecificJMHFile(Path dataDir, String filename) {
    try {
      var filePath = dataDir.resolve(filename);
      if (!Files.exists(filePath) || !filename.startsWith("jmh-result-") || !filename.endsWith(".json")) {
        return "[]";
      }

      return Files.readString(filePath);

    } catch (IOException e) {
      e.printStackTrace();
      return "[]";
    }
  }

  static String getLatestJMHFilename(Path dataDir) {
    try {
      var jmhFiles = Files.list(dataDir)
          .filter(p -> p.getFileName().toString().startsWith("jmh-result-") && p.getFileName().toString().endsWith(".json"))
          .sorted((a, b) -> b.getFileName().toString().compareTo(a.getFileName().toString()))
          .toList();

      return jmhFiles.isEmpty() ? "no-jmh-files-found" : jmhFiles.get(0).getFileName().toString();

    } catch (IOException e) {
      return "error-loading-jmh-files";
    }
  }

  static String getLatestSizesFilename(Path dataDir) {
    try {
      var sizesFiles = Files.list(dataDir)
          .filter(p -> p.getFileName().toString().startsWith("sizes-") && p.getFileName().toString().endsWith(".json"))
          .sorted((a, b) -> b.getFileName().toString().compareTo(a.getFileName().toString()))
          .toList();

      return sizesFiles.isEmpty() ? "no-sizes-files-found" : sizesFiles.get(0).getFileName().toString();

    } catch (IOException e) {
      return "error-loading-sizes-files";
    }
  }
}