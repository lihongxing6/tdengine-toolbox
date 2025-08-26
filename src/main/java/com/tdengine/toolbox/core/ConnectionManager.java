package com.tdengine.toolbox.core;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
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
            restBaseUrl = "http://" + uri.getHost() + ":" + uri.getPort() + "/rest/sql";
            
            // 生成认证头
            String credentials = user + ":" + password;
            String encodedCredentials = Base64.getEncoder().encodeToString(credentials.getBytes("UTF-8"));
            authHeader = "Basic " + encodedCredentials;
        } else {
            throw new IllegalArgumentException("REST URL 必须以 rest:// 开头");
        }
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
     * 执行 REST SQL
     */
    private boolean executeRestSql(String sql) throws TdEngineException {
        try {
            HttpPost post = new HttpPost(restBaseUrl);
            post.setHeader("Authorization", authHeader);
            post.setHeader("Content-Type", "application/json");
            
            JSONObject requestBody = new JSONObject();
            requestBody.put("sql", sql);
            
            StringEntity entity = new StringEntity(requestBody.toString(), "UTF-8");
            post.setEntity(entity);
            
            HttpResponse response = httpClient.execute(post);
            HttpEntity responseEntity = response.getEntity();
            String responseBody = EntityUtils.toString(responseEntity);
            
            JSONObject result = JSON.parseObject(responseBody);
            String status = result.getString("status");
            
            if ("succ".equals(status)) {
                logger.debug("REST SQL 执行成功: " + sql);
                return true;
            } else {
                String desc = result.getString("desc");
                throw new TdEngineException("REST_SQL_ERROR", "REST SQL 执行失败: " + desc);
            }
            
        } catch (IOException e) {
            throw new TdEngineException("REST_CONNECTION_ERROR", "REST 连接异常", e);
        } catch (Exception e) {
            throw new TdEngineException("REST_EXECUTE_ERROR", "REST SQL 执行异常", e);
        }
    }
    
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