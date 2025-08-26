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
            String insertSql = table.toInsertSql();
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
     * 批量插入数据
     * @param tables 表对象列表
     * @throws TdEngineException 插入异常
     */
    public static void insertBatch(List<Table> tables) throws TdEngineException {
        if (tables == null || tables.isEmpty()) {
            logger.warn("批量插入：表列表为空，忽略操作");
            return;
        }
        
        logger.info("开始批量插入，共 " + tables.size() + " 个表");
        
        int successCount = 0;
        for (int i = 0; i < tables.size(); i++) {
            try {
                Table table = tables.get(i);
                insert(table);
                successCount++;
                logger.debug("批量插入进度: " + (i + 1) + "/" + tables.size());
                
            } catch (TdEngineException e) {
                logger.error("批量插入失败，表: " + tables.get(i).getFullName() + ", 错误: " + e.getMessage());
                throw new TdEngineException("BATCH_INSERT_FAILED", 
                    "批量插入失败，成功 " + successCount + " 个，失败在第 " + (i + 1) + " 个", e);
            }
        }
        
        logger.info("批量插入完成，成功插入 " + successCount + " 个表");
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
        logger.info("表不存在，自动创建表: " + table.getFullName());
        
        String createSql = table.toCreateSql();
        logger.debug("执行建表语句: " + createSql);
        
        connectionManager.execute(createSql);
        logger.info("自动建表成功: " + table.getFullName());
    }
    
    /**
     * 处理字段不存在的情况
     */
    private static void handleColumnNotExist(Table table, TdEngineException exception) throws TdEngineException {
        logger.info("字段不存在，尝试添加字段: " + exception.getMessage());
        
        // 简化处理：为表中所有字段都添加到数据库表中
        // 实际实现中可以解析异常消息，确定具体是哪个字段不存在
        for (Field field : table.getFields()) {
            try {
                String alterSql = String.format("ALTER TABLE %s ADD COLUMN %s %s", 
                    table.getFullName(), field.getName(), field.getType());
                logger.debug("执行添加字段语句: " + alterSql);
                connectionManager.execute(alterSql);
            } catch (TdEngineException e) {
                // 字段可能已存在，忽略错误
                logger.debug("添加字段失败（可能已存在）: " + e.getMessage());
            }
        }
        
        logger.info("字段添加完成");
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
                String dropSql = String.format("ALTER TABLE %s DROP COLUMN %s", 
                    table.getFullName(), field.getName());
                logger.debug("执行删除字段语句: " + dropSql);
                connectionManager.execute(dropSql);
                
                // 添加字段
                String addSql = String.format("ALTER TABLE %s ADD COLUMN %s %s", 
                    table.getFullName(), field.getName(), field.getType());
                logger.debug("执行添加字段语句: " + addSql);
                connectionManager.execute(addSql);
                
            } catch (TdEngineException e) {
                logger.debug("修改字段类型失败: " + e.getMessage());
            }
        }
        
        logger.info("字段类型修改完成");
    }
}