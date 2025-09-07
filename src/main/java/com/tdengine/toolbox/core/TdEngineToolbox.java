package com.tdengine.toolbox.core;

import com.tdengine.toolbox.ddl.Field;
import com.tdengine.toolbox.ddl.Table;
import com.tdengine.toolbox.logger.DefaultLogger;
import com.tdengine.toolbox.logger.Logger;
import com.tdengine.toolbox.util.TdEngineException;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * TDengine 工具箱核心执行器
 * 静态类，提供全局调用入口
 * 
 * @author TDengine Toolbox
 * @version 1.0.0
 */
public class TdEngineToolbox {
    
    private static volatile ConnectionManager connectionManager;
    private static volatile Logger logger = new DefaultLogger();
    private static final AtomicBoolean initialized = new AtomicBoolean(false);
    private static final int MAX_SQL_LENGTH = 1_048_576; // TDengine 单条 SQL 最大长度（字符）
    private static volatile boolean strictTypeCheck = Boolean.parseBoolean(
            System.getProperty("tdengine.toolbox.strictType", "false"));
    
    // 私有构造函数，防止实例化
    private TdEngineToolbox() {}
    
    /**
     * 初始化工具箱
     * @param url 连接 URL
     * @param user 用户名  
     * @param password 密码
     * @param useRest 是否使用 REST 连接方式
     * @throws TdEngineException 初始化异常
     */
    public static synchronized void init(String url, String user, String password, boolean useRest) throws TdEngineException {
        if (initialized.get()) {
            logger.warn("TdEngineToolbox 已经初始化，忽略重复初始化");
            return;
        }
        
        validateInitParameters(url, user, password);
        
        try {
            connectionManager = new ConnectionManager(url, user, password, useRest, logger);
            connectionManager.initialize();
            
            initialized.set(true);
            logger.info("TdEngineToolbox 初始化成功");
            logger.info("连接URL: " + url);
            logger.info("用户名: " + user);
            logger.info("连接方式: " + (useRest ? "REST" : "JDBC"));
            
        } catch (Exception e) {
            initialized.set(false);
            connectionManager = null;
            throw new TdEngineException("TOOLBOX_INIT_FAILED", "TdEngineToolbox 初始化失败", e);
        }
    }
    
    /**
     * 简化初始化方法（自动解析 URL 中的连接方式）
     * @param url 连接 URL
     * @throws TdEngineException 初始化异常
     */
    public static void init(String url) throws TdEngineException {
        // 从 URL 中解析用户名、密码和连接方式
        if (url == null || url.trim().isEmpty()) {
            throw new TdEngineException("INVALID_URL", "连接 URL 不能为空");
        }
        
        boolean useRest = url.startsWith("rest://");
        String user = "root";
        String password = "taosdata";
        
        // 简单的 URL 参数解析
        if (url.contains("?")) {
            String queryString = url.substring(url.indexOf("?") + 1);
            String[] params = queryString.split("&");
            
            for (String param : params) {
                String[] kv = param.split("=");
                if (kv.length == 2) {
                    String key = kv[0].trim();
                    String value = kv[1].trim();
                    if ("user".equals(key)) {
                        user = value;
                    } else if ("password".equals(key)) {
                        password = value;
                    }
                }
            }
        }
        
        init(url, user, password, useRest);
    }
    
    /**
     * 插入数据
     * @param table 表对象
     * @throws TdEngineException 插入异常
     */
    public static void insert(Table table) throws TdEngineException {
        validateInitialized();
        validateTable(table);

        try {
            // 值与声明类型的潜在隐式转换监控（例如 10.2 -> INT 截断）
            detectAndHandleCoercions(table);
            String insertSql;
            if (connectionManager.isRest()) {
                String db = connectionManager.getRestDatabase();
                insertSql = table.toInsertSqlWithDb(db);
            } else {
                insertSql = table.toInsertSql();
            }
            logger.debug("准备执行插入语句: " + insertSql);
            
            boolean success = false;
            TdEngineException lastException = null;
            
            // 尝试执行插入，如果失败则进行智能修复
            for (int attempt = 0; attempt < 3; attempt++) {
                try {
                    connectionManager.execute(insertSql);
                    success = true;
                    logger.info("数据插入成功: " + table.getFullName());
                    break;
                    
                } catch (TdEngineException e) {
                    lastException = e;
                    logger.debug("插入失败 (尝试 " + (attempt + 1) + "/3): " + e.getMessage());
                    
                    if (e.isTableNotExist()) {
                        // 表不存在，自动建表
                        handleTableNotExist(table);
                    } else if (e.isColumnNotExist()) {
                        // 字段不存在，自动添加字段
                        handleColumnNotExist(table, e);
                    } else if (e.isColumnTypeMismatch()) {
                        // 字段类型不匹配，自动修改字段类型
                        handleColumnTypeMismatch(table, e);
                    } else {
                        // 其他异常，无法自动修复
                        throw e;
                    }
                }
            }
            
            if (!success && lastException != null) {
                throw new TdEngineException("INSERT_FAILED_AFTER_RETRY", "插入失败，已尝试修复但仍然失败", lastException);
            }
            
        } catch (TdEngineException e) {
            throw e;
        } catch (Exception e) {
            throw new TdEngineException("INSERT_ERROR", "插入数据时发生异常", e);
        }
    }

    /**
     * 执行查询 SQL，返回通用结果结构。
     */
    public static QueryResult query(String sql) throws TdEngineException {
        validateInitialized();
        if (sql == null || sql.trim().isEmpty()) {
            throw new TdEngineException("INVALID_SQL", "SQL 不能为空");
        }
        return connectionManager.query(sql);
    }
    
    /**
     * 批量插入数据
     * @param tables 表对象列表
     * @throws TdEngineException 插入异常
     */
    public static void insertBatch(List<Table> tables) throws TdEngineException {
        if (tables == null || tables.isEmpty()) {
            logger.warn("批量插入：表列表为空，忽略操作");
            return;
        }
        
        logger.info("开始批量插入，共 " + tables.size() + " 条记录");

        final String insertPrefix = "INSERT INTO ";
        StringBuilder sb = new StringBuilder(insertPrefix);
        int chunkStartIndex = 0; // 当前批次开始的表索引
        int countInChunk = 0;
        int totalInserted = 0;

        for (int i = 0; i < tables.size(); i++) {
            Table t = tables.get(i);
            // 逐条监控潜在类型隐式转换
            detectAndHandleCoercions(t);
            String seg = connectionManager.isRest()
                    ? t.toInsertSegmentWithDb(connectionManager.getRestDatabase())
                    : t.toInsertSegment();
            if (seg.isEmpty()) continue;

            int extraLen = (countInChunk == 0 ? 0 : 1) + seg.length(); // 1 表示分隔的换行
            if (sb.length() + extraLen + 1 /* 末尾分号 */ > MAX_SQL_LENGTH) {
                // 执行当前批次
                if (countInChunk > 0) {
                    try {
                        sb.append(';');
                        connectionManager.execute(sb.toString());
                        totalInserted += countInChunk;
                    } catch (TdEngineException e) {
                        // 发生错误：退化为逐条插入，复用单条的自愈逻辑
                        logger.warn("批量执行失败，退化为逐条插入。原因: " + e.getMessage());
                        for (int j = chunkStartIndex; j < chunkStartIndex + countInChunk; j++) {
                            insert(tables.get(j));
                            totalInserted++;
                        }
                    }
                }
                // 开启下一批次
                sb.setLength(0);
                sb.append(insertPrefix).append(seg);
                chunkStartIndex = i;
                countInChunk = 1;
            } else {
                if (countInChunk > 0) sb.append('\n');
                sb.append(seg);
                countInChunk++;
            }
        }

        // 执行最后一批
        if (countInChunk > 0) {
            try {
                sb.append(';');
                connectionManager.execute(sb.toString());
                totalInserted += countInChunk;
            } catch (TdEngineException e) {
                logger.warn("最后一批批量执行失败，退化为逐条插入。原因: " + e.getMessage());
                for (int j = chunkStartIndex; j < chunkStartIndex + countInChunk; j++) {
                    insert(tables.get(j));
                    totalInserted++;
                }
            }
        }

        logger.info("批量插入完成，成功插入 " + totalInserted + " 条记录");
    }

    /**
     * 开启/关闭严格类型检查（默认 false）。开启后检测到可能的类型截断/隐式转换将抛出异常。
     */
    public static void setStrictTypeCheck(boolean enabled) {
        strictTypeCheck = enabled;
    }

    // 检测并记录可能的类型隐式转换（如向 INT 写入 10.2）。
    private static void detectAndHandleCoercions(Table table) throws TdEngineException {
        for (Field f : table.getFields()) {
            String type = baseType(f.getType());
            Object v = f.getValue();
            if (v == null) continue;

            if (isIntegerType(type) && v instanceof Number) {
                if (hasFractional((Number) v)) {
                    String msg = String.format(
                            "字段 '%s' 声明为 %s，但值为带小数的 %s（%s），数据库可能会截断小数部分",
                            f.getName(), type, v.getClass().getSimpleName(), v.toString());
                    if (strictTypeCheck) {
                        throw new TdEngineException("VALUE_TYPE_COERCION", msg);
                    } else {
                        logger.warn(msg);
                    }
                }
            }

            if ("BOOL".equals(type) || "BOOLEAN".equals(type)) {
                if (!(v instanceof Boolean)) {
                    String sv = String.valueOf(v).toLowerCase();
                    boolean acceptable = "true".equals(sv) || "false".equals(sv) || "1".equals(sv) || "0".equals(sv);
                    if (!acceptable) {
                        String msg = String.format(
                                "字段 '%s' 声明为 BOOL，但值 '%s' 可能被转换为 0，建议使用 true/false 或 1/0",
                                f.getName(), v);
                        if (strictTypeCheck) {
                            throw new TdEngineException("VALUE_TYPE_COERCION", msg);
                        } else {
                            logger.warn(msg);
                        }
                    }
                }
            }
        }
    }

    private static boolean isIntegerType(String baseType) {
        return "INT".equals(baseType) || "BIGINT".equals(baseType)
                || "SMALLINT".equals(baseType) || "TINYINT".equals(baseType);
    }

    private static boolean hasFractional(Number n) {
        if (n instanceof Float || n instanceof Double) {
            double d = n.doubleValue();
            return d != Math.rint(d);
        }
        if (n instanceof java.math.BigDecimal) {
            java.math.BigDecimal bd = (java.math.BigDecimal) n;
            return bd.stripTrailingZeros().scale() > 0;
        }
        // 其他整数类型
        return false;
    }

    private static String baseType(String type) {
        if (type == null) return "";
        String t = type.trim().toUpperCase();
        int idx = t.indexOf('(');
        if (idx > 0) t = t.substring(0, idx);
        return t;
    }

    
    /**
     * 设置日志记录器
     * @param logger 日志记录器
     */
    public static void setLogger(Logger logger) {
        if (logger != null) {
            TdEngineToolbox.logger = logger;
        }
    }
    
    /**
     * 关闭工具箱
     */
    public static synchronized void shutdown() {
        if (!initialized.get()) {
            return;
        }
        
        try {
            if (connectionManager != null) {
                connectionManager.shutdown();
            }
            
            initialized.set(false);
            connectionManager = null;
            logger.info("TdEngineToolbox 已关闭");
            
        } catch (Exception e) {
            logger.error("关闭 TdEngineToolbox 时发生异常", e);
        }
    }
    
    /**
     * 检查工具箱是否已初始化
     * @return 是否已初始化
     */
    public static boolean isInitialized() {
        return initialized.get();
    }
    
    /**
     * 检查连接是否正常
     * @return 连接是否正常
     */
    public static boolean isConnected() {
        return initialized.get() && connectionManager != null && connectionManager.isConnected();
    }
    
    // ==================== 私有方法 ====================
    
    /**
     * 验证初始化参数
     */
    private static void validateInitParameters(String url, String user, String password) throws TdEngineException {
        if (url == null || url.trim().isEmpty()) {
            throw new TdEngineException("INVALID_URL", "连接 URL 不能为空");
        }
        if (user == null || user.trim().isEmpty()) {
            throw new TdEngineException("INVALID_USER", "用户名不能为空");
        }
        if (password == null) {
            throw new TdEngineException("INVALID_PASSWORD", "密码不能为空");
        }
    }
    
    /**
     * 验证是否已初始化
     */
    private static void validateInitialized() throws TdEngineException {
        if (!initialized.get()) {
            throw new TdEngineException("NOT_INITIALIZED", "TdEngineToolbox 未初始化，请先调用 init() 方法");
        }
        if (connectionManager == null) {
            throw new TdEngineException("CONNECTION_MANAGER_NULL", "连接管理器为空");
        }
    }
    
    /**
     * 验证表对象
     */
    private static void validateTable(Table table) throws TdEngineException {
        if (table == null) {
            throw new TdEngineException("INVALID_TABLE", "表对象不能为空");
        }
        if (table.getName() == null || table.getName().trim().isEmpty()) {
            throw new TdEngineException("INVALID_TABLE_NAME", "表名不能为空");
        }
        if (table.getFieldCount() == 0) {
            throw new TdEngineException("EMPTY_FIELDS", "表必须包含至少一个字段");
        }
        if (!table.isValid()) {
            throw new TdEngineException("INVALID_TABLE_DATA", "表数据无效");
        }
    }
    
    /**
     * 处理表不存在的情况
     */
    private static void handleTableNotExist(Table table) throws TdEngineException {
        String tableNameForLog = connectionManager.isRest()
                ? table.getQualifiedName(connectionManager.getRestDatabase())
                : table.getFullName();
        logger.info("表不存在，自动创建表: " + tableNameForLog);

        String createSql = connectionManager.isRest()
                ? table.toCreateSqlWithDb(connectionManager.getRestDatabase())
                : table.toCreateSql();
        logger.debug("执行建表语句: " + createSql);

        connectionManager.execute(createSql);
        logger.info("自动建表成功: " + tableNameForLog);
    }
    
    /**
     * 处理字段不存在的情况
     */
    private static void handleColumnNotExist(Table table, TdEngineException exception) throws TdEngineException {
        logger.info("字段不存在，尝试精准添加缺失字段。原始信息: " + exception.getMessage());

        String tableName = connectionManager.isRest()
                ? table.getQualifiedName(connectionManager.getRestDatabase())
                : table.getFullName();

        // 精准解析缺失字段名
        String missing = exception.extractMissingColumnName();
        if (missing != null && !missing.trim().isEmpty()) {
            Field f = table.getField(missing.trim());
            if (f != null) {
                try {
                    String alterSql = String.format("ALTER TABLE %s ADD COLUMN %s %s", tableName, f.getName(), f.getType());
                    logger.debug("执行添加缺失字段语句: " + alterSql);
                    connectionManager.execute(alterSql);
                    logger.info("已添加缺失字段: " + f.getName());
                    return; // 精准添加完成，返回
                } catch (TdEngineException e) {
                    logger.debug("精准添加字段失败: " + e.getMessage() + "，将回退为添加所有字段。");
                }
            } else {
                logger.warn("提取到缺失字段 '" + missing + "'，但在待插入数据中未找到同名字段，回退为添加所有字段。");
            }
        } else {
            logger.debug("无法从异常中解析缺失字段名，回退为添加所有字段。");
        }

        // 回退策略：为表中所有字段执行 ADD（已存在的忽略错误）
        for (Field field : table.getFields()) {
            try {
                String alterSql = String.format("ALTER TABLE %s ADD COLUMN %s %s", tableName, field.getName(), field.getType());
                logger.debug("执行添加字段语句: " + alterSql);
                connectionManager.execute(alterSql);
            } catch (TdEngineException e) {
                logger.debug("添加字段失败（可能已存在）: " + e.getMessage());
            }
        }

        logger.info("字段添加完成（回退策略）");
    }
    
    /**
     * 处理字段类型不匹配的情况
     */
    private static void handleColumnTypeMismatch(Table table, TdEngineException exception) throws TdEngineException {
        logger.info("字段类型不匹配，尝试修改字段类型: " + exception.getMessage());
        
        // TDengine 不支持直接修改字段类型，需要先删除再添加
        // 简化处理：为表中所有字段都重新定义
        for (Field field : table.getFields()) {
            try {
                // 删除字段
                String tableName = connectionManager.isRest()
                        ? table.getQualifiedName(connectionManager.getRestDatabase())
                        : table.getFullName();
                String dropSql = String.format("ALTER TABLE %s DROP COLUMN %s",
                        tableName, field.getName());
                logger.debug("执行删除字段语句: " + dropSql);
                connectionManager.execute(dropSql);
                
                // 添加字段
                String addSql = String.format("ALTER TABLE %s ADD COLUMN %s %s",
                        tableName, field.getName(), field.getType());
                logger.debug("执行添加字段语句: " + addSql);
                connectionManager.execute(addSql);
                
            } catch (TdEngineException e) {
                logger.debug("修改字段类型失败: " + e.getMessage());
            }
        }
        
        logger.info("字段类型修改完成");
    }
}
