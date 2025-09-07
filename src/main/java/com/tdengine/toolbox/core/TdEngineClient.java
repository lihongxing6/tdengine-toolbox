package com.tdengine.toolbox.core;

import com.tdengine.toolbox.ddl.Field;
import com.tdengine.toolbox.ddl.Table;
import com.tdengine.toolbox.logger.DefaultLogger;
import com.tdengine.toolbox.logger.Logger;
import com.tdengine.toolbox.util.TdEngineException;

import java.sql.Timestamp;
import java.util.Date;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 可多实例的 TDengine 客户端。
 * 与全局单例 TdEngineToolbox 并存，适用于多库/多连接场景。
 */
public class TdEngineClient {

    private final ConnectionManager connectionManager;
    private final Logger logger;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private volatile boolean strictTypeCheck = Boolean.parseBoolean(
            System.getProperty("tdengine.toolbox.strictType", "false"));

    private static final int MAX_SQL_LENGTH = 1_048_576; // TDengine 单条 SQL 最大长度（字符）

    private TdEngineClient(ConnectionManager cm, Logger logger) throws TdEngineException {
        this.connectionManager = cm;
        this.logger = logger == null ? new DefaultLogger() : logger;
        this.connectionManager.initialize();
    }

    // ============ 工厂方法 ============

    public static TdEngineClient rest(String url) throws TdEngineException {
        return rest(url, new DefaultLogger());
    }

    public static TdEngineClient rest(String url, Logger logger) throws TdEngineException {
        String[] up = parseUserPass(url);
        ConnectionManager cm = new ConnectionManager(url, up[0], up[1], true, logger == null ? new DefaultLogger() : logger);
        return new TdEngineClient(cm, logger);
    }

    public static TdEngineClient jdbc(String url, String user, String password) throws TdEngineException {
        return jdbc(url, user, password, new DefaultLogger());
    }

    public static TdEngineClient jdbc(String url, String user, String password, Logger logger) throws TdEngineException {
        ConnectionManager cm = new ConnectionManager(url, user, password, false, logger == null ? new DefaultLogger() : logger);
        return new TdEngineClient(cm, logger);
    }

    public static TdEngineClient of(String url) throws TdEngineException {
        return of(url, new DefaultLogger());
    }

    public static TdEngineClient of(String url, Logger logger) throws TdEngineException {
        if (url == null || url.trim().isEmpty()) {
            throw new TdEngineException("INVALID_URL", "连接 URL 不能为空");
        }
        boolean useRest = url.startsWith("rest://");
        if (useRest) {
            return rest(url, logger);
        }
        String[] up = parseUserPass(url);
        return jdbc(url, up[0], up[1], logger);
    }

    // ============ 对外 API ============

    public void setStrictTypeCheck(boolean enabled) { this.strictTypeCheck = enabled; }

    public boolean isConnected() { return !closed.get() && connectionManager.isConnected(); }

    public void close() { if (!closed.getAndSet(true)) connectionManager.shutdown(); }

    public void insert(Table table) throws TdEngineException {
        ensureOpen();
        validateTable(table);

        // 值与声明类型的潜在隐式转换检测
        detectAndHandleCoercions(table);

        String insertSql = connectionManager.isRest()
                ? table.toInsertSqlWithDb(connectionManager.getRestDatabase())
                : table.toInsertSql();
        logger.debug("准备执行插入语句: " + insertSql);

        boolean success = false;
        TdEngineException lastException = null;
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
                    handleTableNotExist(table);
                } else if (e.isColumnNotExist()) {
                    handleColumnNotExist(table, e);
                } else if (e.isColumnTypeMismatch()) {
                    handleColumnTypeMismatch(table, e);
                } else {
                    throw e;
                }
            }
        }

        if (!success && lastException != null) {
            throw new TdEngineException("INSERT_FAILED_AFTER_RETRY", "插入失败，已尝试修复但仍然失败", lastException);
        }
    }

    public void insertBatch(List<Table> tables) throws TdEngineException {
        ensureOpen();
        if (tables == null || tables.isEmpty()) {
            logger.warn("批量插入：表列表为空，忽略操作");
            return;
        }
        logger.info("开始批量插入，共 " + tables.size() + " 条记录");

        final String insertPrefix = "INSERT INTO ";
        StringBuilder sb = new StringBuilder(insertPrefix);
        int chunkStartIndex = 0;
        int countInChunk = 0;
        int totalInserted = 0;

        for (int i = 0; i < tables.size(); i++) {
            Table t = tables.get(i);
            validateTable(t);
            detectAndHandleCoercions(t);
            String seg = connectionManager.isRest()
                    ? t.toInsertSegmentWithDb(connectionManager.getRestDatabase())
                    : t.toInsertSegment();
            if (seg.isEmpty()) continue;

            int extraLen = (countInChunk == 0 ? 0 : 1) + seg.length();
            if (sb.length() + extraLen + 1 > MAX_SQL_LENGTH) {
                if (countInChunk > 0) {
                    try {
                        sb.append(';');
                        connectionManager.execute(sb.toString());
                        totalInserted += countInChunk;
                    } catch (TdEngineException e) {
                        logger.warn("批量执行失败，退化为逐条插入。原因: " + e.getMessage());
                        for (int j = chunkStartIndex; j < chunkStartIndex + countInChunk; j++) {
                            insert(tables.get(j));
                            totalInserted++;
                        }
                    }
                }
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
     * 执行查询 SQL，返回通用结果结构。
     */
    public QueryResult query(String sql) throws TdEngineException {
        ensureOpen();
        if (sql == null || sql.trim().isEmpty()) {
            throw new TdEngineException("INVALID_SQL", "SQL 不能为空");
        }
        return connectionManager.query(sql);
    }

    // ============ 私有方法 ============

    private void ensureOpen() throws TdEngineException {
        if (closed.get()) throw new TdEngineException("CLIENT_CLOSED", "TdEngineClient 已关闭");
    }

    private void validateTable(Table table) throws TdEngineException {
        if (table == null) throw new TdEngineException("INVALID_TABLE", "表对象不能为空");
        if (table.getName() == null || table.getName().trim().isEmpty()) {
            throw new TdEngineException("INVALID_TABLE_NAME", "表名不能为空");
        }
        if (table.getFieldCount() == 0) throw new TdEngineException("EMPTY_FIELDS", "表必须包含至少一个字段");
        if (!table.isValid()) throw new TdEngineException("INVALID_TABLE_DATA", "表数据无效");
    }

    private void handleTableNotExist(Table table) throws TdEngineException {
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

    private void handleColumnNotExist(Table table, TdEngineException exception) throws TdEngineException {
        logger.info("字段不存在，尝试精准添加缺失字段。原始信息: " + exception.getMessage());

        String tableName = connectionManager.isRest()
                ? table.getQualifiedName(connectionManager.getRestDatabase())
                : table.getFullName();

        String missing = exception.extractMissingColumnName();
        if (missing != null && !missing.trim().isEmpty()) {
            Field f = table.getField(missing.trim());
            if (f != null) {
                try {
                    String alterSql = String.format("ALTER TABLE %s ADD COLUMN %s %s", tableName, f.getName(), f.getType());
                    logger.debug("执行添加缺失字段语句: " + alterSql);
                    connectionManager.execute(alterSql);
                    logger.info("已添加缺失字段: " + f.getName());
                    return;
                } catch (TdEngineException e) {
                    logger.debug("精准添加字段失败: " + e.getMessage() + "，将回退为添加所有字段。");
                }
            } else {
                logger.warn("提取到缺失字段 '" + missing + "'，但在待插入数据中未找到同名字段，回退为添加所有字段。");
            }
        } else {
            logger.debug("无法从异常中解析缺失字段名，回退为添加所有字段。");
        }

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

    private void handleColumnTypeMismatch(Table table, TdEngineException exception) throws TdEngineException {
        logger.info("字段类型不匹配，尝试修改字段类型: " + exception.getMessage());

        for (Field field : table.getFields()) {
            try {
                String tableName = connectionManager.isRest()
                        ? table.getQualifiedName(connectionManager.getRestDatabase())
                        : table.getFullName();
                String dropSql = String.format("ALTER TABLE %s DROP COLUMN %s", tableName, field.getName());
                logger.debug("执行删除字段语句: " + dropSql);
                connectionManager.execute(dropSql);

                String addSql = String.format("ALTER TABLE %s ADD COLUMN %s %s", tableName, field.getName(), field.getType());
                logger.debug("执行添加字段语句: " + addSql);
                connectionManager.execute(addSql);
            } catch (TdEngineException e) {
                logger.debug("修改字段类型失败: " + e.getMessage());
            }
        }
        logger.info("字段类型修改完成");
    }

    private static String[] parseUserPass(String url) {
        String user = "root";
        String password = "taosdata";
        if (url != null && url.contains("?")) {
            String query = url.substring(url.indexOf("?") + 1);
            String[] params = query.split("&");
            for (String p : params) {
                String[] kv = p.split("=");
                if (kv.length == 2) {
                    String k = kv[0].trim();
                    String v = kv[1].trim();
                    if ("user".equals(k)) user = v;
                    if ("password".equals(k)) password = v;
                }
            }
        }
        return new String[]{user, password};
    }

    // ==== 值与声明类型的潜在隐式转换检测 ====
    private void detectAndHandleCoercions(Table table) throws TdEngineException {
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
        return false;
    }

    private static String baseType(String type) {
        if (type == null) return "";
        String t = type.trim().toUpperCase();
        int idx = t.indexOf('(');
        if (idx > 0) t = t.substring(0, idx);
        return t;
    }
}
