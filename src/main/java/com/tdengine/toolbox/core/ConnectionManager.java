package com.tdengine.toolbox.core;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.tdengine.toolbox.logger.Logger;
import com.tdengine.toolbox.util.TdEngineException;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.net.URI;
import java.net.URLDecoder;
import java.sql.*;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 连接管理器
 * 支持 RESTful 和 JDBC 两种连接方式
 * 
 * @author TDengine Toolbox
 * @version 1.0.0
 */
public class ConnectionManager {
    
    private final Logger logger;
    private final String url;
    private final String user;
    private final String password;
    private final boolean useRest;
    private final AtomicBoolean initialized = new AtomicBoolean(false);
    
    // JDBC 连接池
    private final ConcurrentHashMap<Long, Connection> jdbcConnections = new ConcurrentHashMap<>();
    
    // REST 连接配置
    private HttpClient httpClient;
    private String restBaseUrl;
    private String authHeader;
    private String restDb;
    
    /**
     * 构造函数
     * @param url 连接 URL
     * @param user 用户名
     * @param password 密码
     * @param useRest 是否使用 REST 连接
     * @param logger 日志记录器
     */
    public ConnectionManager(String url, String user, String password, boolean useRest, Logger logger) {
        this.url = url;
        this.user = user;
        this.password = password;
        this.useRest = useRest;
        this.logger = logger;
    }
    
    /**
     * 初始化连接管理器
     * @throws TdEngineException 初始化失败
     */
    public void initialize() throws TdEngineException {
        if (initialized.get()) {
            return;
        }
        
        try {
            if (useRest) {
                initializeRestConnection();
            } else {
                initializeJdbcConnection();
            }
            initialized.set(true);
            logger.info("连接管理器初始化成功，使用" + (useRest ? "REST" : "JDBC") + "连接方式");
        } catch (Exception e) {
            throw new TdEngineException("CONNECTION_INIT_FAILED", "连接管理器初始化失败", e);
        }
    }
    
    /**
     * 初始化 REST 连接
     */
    private void initializeRestConnection() throws Exception {
        httpClient = HttpClients.createDefault();

        // 解析 REST URL
        if (url.startsWith("rest://")) {
            URI uri = new URI(url.replace("rest://", "http://"));

            // 解析数据库名：优先取查询参数 db/database，其次取路径首段
            String db = null;
            // from query ?db=xxx or ?database=xxx
            String query = uri.getQuery();
            if (query != null && !query.isEmpty()) {
                String[] pairs = query.split("&");
                for (String p : pairs) {
                    int eq = p.indexOf('=');
                    String k = eq >= 0 ? p.substring(0, eq) : p;
                    String v = eq >= 0 ? p.substring(eq + 1) : "";
                    k = k == null ? null : k.trim();
                    if (k == null) continue;
                    if ("db".equalsIgnoreCase(k) || "database".equalsIgnoreCase(k)) {
                        db = URLDecoder.decode(v, "UTF-8");
                        break;
                    }
                }
            }
            // from path /<db>
            String rawPath = uri.getPath();
            if (db == null && rawPath != null && !rawPath.trim().isEmpty() && !"/".equals(rawPath)) {
                String path = rawPath.startsWith("/") ? rawPath.substring(1) : rawPath;
                if (!path.isEmpty()) {
                    // 仅取第一个段作为数据库名
                    int slash = path.indexOf('/');
                    db = (slash > 0) ? path.substring(0, slash) : path;
                }
            }

            String base = "http://" + uri.getHost() + ":" + uri.getPort() + "/rest/sql";
            if (db != null && !db.isEmpty()) {
                restBaseUrl = base + "?db=" + db;
            } else {
                restBaseUrl = base;
            }
            this.restDb = db;

            // 生成认证头
            String credentials = user + ":" + password;
            String encodedCredentials = Base64.getEncoder().encodeToString(credentials.getBytes("UTF-8"));
            authHeader = "Basic " + encodedCredentials;
        } else {
            throw new IllegalArgumentException("REST URL 必须以 rest:// 开头");
        }
    }

    /**
     * 是否为 REST 连接
     */
    public boolean isRest() {
        return useRest;
    }

    /**
     * 从 URL 解析得到的默认数据库（REST 模式）
     */
    public String getRestDatabase() {
        return restDb;
    }
    
    /**
     * 初始化 JDBC 连接
     */
    private void initializeJdbcConnection() throws Exception {
        // 加载 TDengine JDBC 驱动
        Class.forName("com.taosdata.jdbc.TSDBDriver");
        
        // 测试连接
        try (Connection conn = DriverManager.getConnection(url, user, password)) {
            logger.info("JDBC 连接测试成功");
        }
    }
    
    /**
     * 执行 SQL 语句
     * @param sql SQL 语句
     * @return 是否执行成功
     * @throws TdEngineException 执行异常
     */
    public boolean execute(String sql) throws TdEngineException {
        if (!initialized.get()) {
            throw new TdEngineException("MANAGER_NOT_INITIALIZED", "连接管理器未初始化");
        }
        
        if (useRest) {
            return executeRestSql(sql);
        } else {
            return executeJdbcSql(sql);
        }
    }

    /**
     * 执行查询 SQL，返回统一结果结构。
     */
    public QueryResult query(String sql) throws TdEngineException {
        if (!initialized.get()) {
            throw new TdEngineException("MANAGER_NOT_INITIALIZED", "连接管理器未初始化");
        }
        if (useRest) {
            return queryRestSql(sql);
        } else {
            return queryJdbcSql(sql);
        }
    }
    
    /**
     * 执行 REST SQL
     */
    private boolean executeRestSql(String sql) throws TdEngineException {
        try {
            HttpPost post = new HttpPost(restBaseUrl);
            post.setHeader("Authorization", authHeader);
            post.setHeader("Content-Type", "text/plain; charset=UTF-8");

            // TDengine REST /rest/sql 期望原始 SQL 文本作为请求体
            StringEntity entity = new StringEntity(sql, "UTF-8");
            post.setEntity(entity);

            HttpResponse response = httpClient.execute(post);
            int httpCode = response.getStatusLine() != null ? response.getStatusLine().getStatusCode() : 0;
            HttpEntity responseEntity = response.getEntity();
            String responseBody = responseEntity != null ? EntityUtils.toString(responseEntity) : "";

            // 容错：非 200 也尝试解析主体获得 desc
            JsonObject result = null;
            try {
                result = JsonParser.parseString(responseBody).getAsJsonObject();
            } catch (Exception ignore) {
                // ignore parse error for non-JSON body
            }

            String status = null;
            String desc = null;
            Integer code = null;
            if (result != null) {
                status = result.has("status") && !result.get("status").isJsonNull() ? result.get("status").getAsString() : null;
                desc = result.has("desc") && !result.get("desc").isJsonNull() ? result.get("desc").getAsString() : null;
                if (result.has("code") && !result.get("code").isJsonNull()) {
                    try { code = result.get("code").getAsInt(); } catch (Exception ignore) {}
                }
            }

            boolean successByStatus = "succ".equalsIgnoreCase(status);
            boolean successByCode = (code != null && code == 0);
            if (httpCode == 200 && (successByStatus || successByCode)) {
                logger.debug("REST SQL 执行成功: " + sql);
                return true;
            }

            String lowerDesc = desc == null ? "" : desc.toLowerCase();
            String errorCode;
            if ((code != null && code == 9731) || lowerDesc.contains("table does not exist")) {
                errorCode = "TABLE_NOT_EXIST";
            } else if ((code != null && code == 9730) ||
                    lowerDesc.contains("column does not exist") ||
                    lowerDesc.contains("invalid column name") ||
                    lowerDesc.contains("unknown column")) {
                errorCode = "COLUMN_NOT_EXIST";
            } else if (lowerDesc.contains("data type mismatch") || lowerDesc.contains("type mismatch")) {
                errorCode = "COLUMN_TYPE_MISMATCH";
            } else if (lowerDesc.contains("db is not specified")) {
                errorCode = "DB_NOT_SPECIFIED";
            } else {
                errorCode = "REST_SQL_ERROR";
            }

            StringBuilder msg = new StringBuilder("REST SQL 执行失败");
            if (code != null) msg.append(" [code=").append(code).append("]");
            if (desc != null && !desc.isEmpty()) msg.append(": ").append(desc);
            if (lowerDesc.contains("db is not specified")) {
                msg.append("。请在 REST URL 中通过路径 /<db> 或查询参数 ?db=<db> 指定数据库，或在表名中使用 db.table，或先执行 USE <db>;");
            }

            throw new TdEngineException(errorCode, msg.toString());

        } catch (TdEngineException e) {
            // 保留我们抛出的错误码与信息，便于上层做修复逻辑
            throw e;
        } catch (IOException e) {
            throw new TdEngineException("REST_CONNECTION_ERROR", "REST 连接异常", e);
        } catch (Exception e) {
            throw new TdEngineException("REST_EXECUTE_ERROR", "REST SQL 执行异常", e);
        }
    }

    /**
     * 执行 REST 查询 SQL
     */
    private QueryResult queryRestSql(String sql) throws TdEngineException {
        try {
            HttpPost post = new HttpPost(restBaseUrl);
            post.setHeader("Authorization", authHeader);
            post.setHeader("Content-Type", "text/plain; charset=UTF-8");
            post.setEntity(new StringEntity(sql, "UTF-8"));

            HttpResponse response = httpClient.execute(post);
            int httpCode = response.getStatusLine() != null ? response.getStatusLine().getStatusCode() : 0;
            HttpEntity responseEntity = response.getEntity();
            String responseBody = responseEntity != null ? EntityUtils.toString(responseEntity) : "";

            JsonObject result = null;
            try { result = JsonParser.parseString(responseBody).getAsJsonObject(); } catch (Exception ignore) {}

            String status = null; String desc = null; Integer code = null; Integer rows = null;
            java.util.List<ColumnMeta> columns = new java.util.ArrayList<>();
            java.util.List<java.util.List<Object>> dataRows = new java.util.ArrayList<>();

            if (result != null) {
                if (result.has("status") && !result.get("status").isJsonNull()) status = result.get("status").getAsString();
                if (result.has("desc") && !result.get("desc").isJsonNull()) desc = result.get("desc").getAsString();
                if (result.has("code") && !result.get("code").isJsonNull()) { try { code = result.get("code").getAsInt(); } catch (Exception ignore) {} }
                if (result.has("rows") && !result.get("rows").isJsonNull()) { try { rows = result.get("rows").getAsInt(); } catch (Exception ignore) {} }

                // 解析列（避免使用 var 以兼容 JDK8）
                if (result.has("column_meta") && result.get("column_meta").isJsonArray()) {
                    com.google.gson.JsonArray metaArr = result.get("column_meta").getAsJsonArray();
                    for (int mi = 0; mi < metaArr.size(); mi++) {
                        try {
                            com.google.gson.JsonArray arr = metaArr.get(mi).getAsJsonArray();
                            String name = arr.get(0).getAsString();
                            String type = arr.size() > 1 && !arr.get(1).isJsonNull() ? arr.get(1).getAsString() : null;
                            Integer len = (arr.size() > 2 && !arr.get(2).isJsonNull()) ? arr.get(2).getAsInt() : null;
                            columns.add(new ColumnMeta(name, type, len));
                        } catch (Exception ignore) { }
                    }
                }

                // 解析数据
                if (result.has("data") && result.get("data").isJsonArray()) {
                    int colCount = columns.size();
                    com.google.gson.JsonArray dataArr = result.get("data").getAsJsonArray();
                    for (int ri = 0; ri < dataArr.size(); ri++) {
                        com.google.gson.JsonArray rowArr = dataArr.get(ri).getAsJsonArray();
                        java.util.List<Object> row = new java.util.ArrayList<>();
                        for (int i = 0; i < rowArr.size(); i++) {
                            com.google.gson.JsonElement cell = rowArr.get(i);
                            String colType = (i < colCount && columns.get(i).getType() != null)
                                    ? columns.get(i).getType().toUpperCase() : null;
                            row.add(convertRestCell(cell, colType));
                        }
                        dataRows.add(row);
                    }
                }
            }

            boolean successByStatus = "succ".equalsIgnoreCase(status);
            boolean successByCode = (code != null && code == 0);
            if (httpCode == 200 && (successByStatus || successByCode)) {
                return new QueryResult(columns, dataRows, rows);
            }

            // 错误映射与抛出
            String lowerDesc = desc == null ? "" : desc.toLowerCase();
            String errorCode;
            if ((code != null && code == 9731) || lowerDesc.contains("table does not exist")) {
                errorCode = "TABLE_NOT_EXIST";
            } else if ((code != null && code == 9730) ||
                    lowerDesc.contains("column does not exist") ||
                    lowerDesc.contains("invalid column name") ||
                    lowerDesc.contains("unknown column")) {
                errorCode = "COLUMN_NOT_EXIST";
            } else if (lowerDesc.contains("data type mismatch") || lowerDesc.contains("type mismatch")) {
                errorCode = "COLUMN_TYPE_MISMATCH";
            } else if (lowerDesc.contains("db is not specified")) {
                errorCode = "DB_NOT_SPECIFIED";
            } else {
                errorCode = "REST_SQL_ERROR";
            }
            StringBuilder msg = new StringBuilder("REST 查询失败");
            if (code != null) msg.append(" [code=").append(code).append("]");
            if (desc != null && !desc.isEmpty()) msg.append(": ").append(desc);
            throw new TdEngineException(errorCode, msg.toString());

        } catch (TdEngineException e) {
            throw e;
        } catch (IOException e) {
            throw new TdEngineException("REST_CONNECTION_ERROR", "REST 连接异常", e);
        } catch (Exception e) {
            throw new TdEngineException("REST_EXECUTE_ERROR", "REST 查询执行异常", e);
        }
    }

    private Object convertRestCell(com.google.gson.JsonElement cell, String colType) {
        if (cell == null || cell.isJsonNull()) return null;
        if (!cell.isJsonPrimitive()) return cell.toString();
        com.google.gson.JsonPrimitive p = cell.getAsJsonPrimitive();
        if (p.isBoolean()) return p.getAsBoolean();
        if (p.isNumber()) {
            if (colType != null) {
                if (colType.contains("BOOL")) return p.getAsInt() == 1;
                if (colType.contains("INT") || colType.contains("TINYINT") || colType.contains("SMALLINT") || colType.contains("BIGINT")) {
                    try { return p.getAsLong(); } catch (Exception ignore) { return p.getAsDouble(); }
                }
                if (colType.contains("FLOAT") || colType.contains("DOUBLE")) {
                    return p.getAsDouble();
                }
            }
            // 无列类型信息，返回更通用的 Double 或 Long
            try { return p.getAsLong(); } catch (Exception ignore) { return p.getAsDouble(); }
        }
        // 其余按字符串
        return p.getAsString();
    }

    /**
     * 执行 JDBC 查询 SQL
     */
    private QueryResult queryJdbcSql(String sql) throws TdEngineException {
        Connection conn = null;
        Statement stmt = null;
        ResultSet rs = null;
        try {
            conn = getJdbcConnection();
            stmt = conn.createStatement();
            rs = stmt.executeQuery(sql);
            ResultSetMetaData meta = rs.getMetaData();
            int cols = meta.getColumnCount();
            java.util.List<ColumnMeta> columns = new java.util.ArrayList<>();
            String[] typeNames = new String[cols];
            for (int i = 1; i <= cols; i++) {
                String typeName = meta.getColumnTypeName(i);
                typeNames[i - 1] = typeName;
                columns.add(new ColumnMeta(meta.getColumnLabel(i), typeName, meta.getColumnDisplaySize(i)));
            }
            java.util.List<java.util.List<Object>> rows = new java.util.ArrayList<>();
            while (rs.next()) {
                java.util.List<Object> row = new java.util.ArrayList<>();
                for (int i = 1; i <= cols; i++) {
                    row.add(readJdbcCell(rs, i, typeNames[i - 1]));
                }
                rows.add(row);
            }
            return new QueryResult(columns, rows, rows.size());
        } catch (SQLException e) {
            String message = e.getMessage() == null ? "" : e.getMessage().toLowerCase();
            if (message.contains("table does not exist")) {
                throw new TdEngineException("TABLE_NOT_EXIST", "表不存在: " + e.getMessage(), e);
            } else if (message.contains("column does not exist")) {
                throw new TdEngineException("COLUMN_NOT_EXIST", "字段不存在: " + e.getMessage(), e);
            } else if (message.contains("data type mismatch")) {
                throw new TdEngineException("COLUMN_TYPE_MISMATCH", "字段类型不匹配: " + e.getMessage(), e);
            } else {
                throw new TdEngineException("JDBC_SQL_ERROR", "JDBC 查询失败: " + e.getMessage(), e);
            }
        } catch (Exception e) {
            throw new TdEngineException("JDBC_EXECUTE_ERROR", "JDBC 查询执行异常", e);
        } finally {
            if (rs != null) try { rs.close(); } catch (SQLException ignore) {}
            closeStatement(stmt);
            returnJdbcConnection(conn);
        }
    }

    private Object readJdbcCell(ResultSet rs, int idx, String typeName) throws SQLException {
        String t = typeName == null ? null : typeName.trim().toUpperCase();
        if (t == null) {
            Object o = rs.getObject(idx);
            return o;
        }
        switch (t) {
            case "BOOL":
            case "BOOLEAN": {
                boolean v = rs.getBoolean(idx);
                return rs.wasNull() ? null : v;
            }
            case "TINYINT":
            case "SMALLINT":
            case "INT":
            case "INTEGER": {
                int v = rs.getInt(idx);
                return rs.wasNull() ? null : v;
            }
            case "BIGINT": {
                long v = rs.getLong(idx);
                return rs.wasNull() ? null : v;
            }
            case "FLOAT": {
                float v = rs.getFloat(idx);
                return rs.wasNull() ? null : v;
            }
            case "DOUBLE": {
                double v = rs.getDouble(idx);
                return rs.wasNull() ? null : v;
            }
            case "TIMESTAMP": {
                java.sql.Timestamp v = rs.getTimestamp(idx);
                return v; // 可为 null
            }
            case "VARCHAR":
            case "NCHAR": {
                String v = rs.getString(idx);
                return v; // 可为 null
            }
            default: {
                // 对于 BINARY/VARBINARY 或驱动返回的带长度类型名（如 BINARY(64)），统一处理
                if (t.contains("BINARY")) {
                    byte[] v = rs.getBytes(idx);
                    if (v == null) return null;
                    if (JDBC_BINARY_AS_STRING) {
                        return decodeBinaryToString(v);
                    }
                    return v; // 按需返回原始字节
                }
                Object o = rs.getObject(idx);
                return o;
            }
        }
    }

    // 将 TDengine 的 BINARY 按 UTF-8 文本解码：截断首个 0x00 之后的填充字节
    private static String decodeBinaryToString(byte[] bytes) {
        int end = bytes.length;
        for (int i = 0; i < bytes.length; i++) {
            if (bytes[i] == 0) { end = i; break; }
        }
        if (end != bytes.length) {
            return new String(Arrays.copyOf(bytes, end), StandardCharsets.UTF_8);
        }
        return new String(bytes, StandardCharsets.UTF_8);
    }

    private static final boolean JDBC_BINARY_AS_STRING = Boolean.parseBoolean(
            System.getProperty("tdengine.toolbox.jdbc.binaryAsString", "true"));
    
    /**
     * 执行 JDBC SQL
     */
    private boolean executeJdbcSql(String sql) throws TdEngineException {
        Connection conn = null;
        Statement stmt = null;
        
        try {
            conn = getJdbcConnection();
            stmt = conn.createStatement();
            
            boolean result = stmt.execute(sql);
            logger.debug("JDBC SQL 执行成功: " + sql);
            
            return true;
            
        } catch (SQLException e) {
            // 分析 SQL 异常类型
            String message = e.getMessage().toLowerCase();
            if (message.contains("table does not exist")) {
                throw new TdEngineException("TABLE_NOT_EXIST", "表不存在: " + e.getMessage(), e);
            } else if (message.contains("column does not exist")) {
                throw new TdEngineException("COLUMN_NOT_EXIST", "字段不存在: " + e.getMessage(), e);
            } else if (message.contains("data type mismatch")) {
                throw new TdEngineException("COLUMN_TYPE_MISMATCH", "字段类型不匹配: " + e.getMessage(), e);
            } else {
                throw new TdEngineException("JDBC_SQL_ERROR", "JDBC SQL 执行失败: " + e.getMessage(), e);
            }
            
        } catch (Exception e) {
            throw new TdEngineException("JDBC_EXECUTE_ERROR", "JDBC SQL 执行异常", e);
            
        } finally {
            closeStatement(stmt);
            returnJdbcConnection(conn);
        }
    }
    
    /**
     * 获取 JDBC 连接
     */
    private Connection getJdbcConnection() throws SQLException {
        long threadId = Thread.currentThread().getId();
        
        Connection conn = jdbcConnections.get(threadId);
        if (conn == null || conn.isClosed()) {
            conn = DriverManager.getConnection(url, user, password);
            jdbcConnections.put(threadId, conn);
        }
        
        return conn;
    }
    
    /**
     * 归还 JDBC 连接
     */
    private void returnJdbcConnection(Connection conn) {
        // 简单实现：保持连接在连接池中
        // 实际生产环境可能需要更复杂的连接池管理
    }
    
    /**
     * 关闭 Statement
     */
    private void closeStatement(Statement stmt) {
        if (stmt != null) {
            try {
                stmt.close();
            } catch (SQLException e) {
                logger.warn("关闭 Statement 失败", e);
            }
        }
    }
    
    /**
     * 关闭连接管理器
     */
    public void shutdown() {
        if (!initialized.get()) {
            return;
        }
        
        try {
            // 关闭所有 JDBC 连接
            for (Connection conn : jdbcConnections.values()) {
                try {
                    if (conn != null && !conn.isClosed()) {
                        conn.close();
                    }
                } catch (SQLException e) {
                    logger.warn("关闭 JDBC 连接失败", e);
                }
            }
            jdbcConnections.clear();
            
            // 关闭 HTTP 客户端
            if (httpClient != null) {
                try {
                    httpClient.getConnectionManager().shutdown();
                } catch (Exception e) {
                    logger.warn("关闭 HTTP 客户端失败", e);
                }
            }
            
            initialized.set(false);
            logger.info("连接管理器已关闭");
            
        } catch (Exception e) {
            logger.error("关闭连接管理器异常", e);
        }
    }
    
    /**
     * 检查连接是否可用
     * @return 连接是否可用
     */
    public boolean isConnected() {
        if (!initialized.get()) {
            return false;
        }
        
        try {
            // 执行简单的查询测试连接
            execute("SELECT SERVER_VERSION()");
            return true;
        } catch (Exception e) {
            logger.debug("连接检查失败", e);
            return false;
        }
    }
}
