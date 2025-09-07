package com.tdengine.toolbox.util;

/**
 * TDengine 工具箱自定义异常
 * 
 * @author TDengine Toolbox
 * @version 1.0.0
 */
public class TdEngineException extends Exception {
    
    private static final long serialVersionUID = 1L;
    
    private final String errorCode;
    
    /**
     * 构造函数
     * @param message 错误消息
     */
    public TdEngineException(String message) {
        super(message);
        this.errorCode = "UNKNOWN";
    }
    
    /**
     * 构造函数
     * @param message 错误消息
     * @param cause 原始异常
     */
    public TdEngineException(String message, Throwable cause) {
        super(message, cause);
        this.errorCode = "UNKNOWN";
    }
    
    /**
     * 构造函数
     * @param errorCode 错误代码
     * @param message 错误消息
     */
    public TdEngineException(String errorCode, String message) {
        super(message);
        this.errorCode = errorCode;
    }
    
    /**
     * 构造函数
     * @param errorCode 错误代码
     * @param message 错误消息
     * @param cause 原始异常
     */
    public TdEngineException(String errorCode, String message, Throwable cause) {
        super(message, cause);
        this.errorCode = errorCode;
    }
    
    /**
     * 获取错误代码
     * @return 错误代码
     */
    public String getErrorCode() {
        return errorCode;
    }
    
    /**
     * 判断是否为表不存在错误
     * @return 是否为表不存在错误
     */
    public boolean isTableNotExist() {
        return "TABLE_NOT_EXIST".equals(errorCode) || 
               (getMessage() != null && getMessage().toLowerCase().contains("table does not exist"));
    }
    
    /**
     * 判断是否为字段不存在错误
     * @return 是否为字段不存在错误
     */
    public boolean isColumnNotExist() {
        if ("COLUMN_NOT_EXIST".equals(errorCode)) return true;
        String msg = getMessage();
        if (msg == null) return false;
        String m = msg.toLowerCase();
        return m.contains("column does not exist") ||
               m.contains("invalid column name") ||
               m.contains("unknown column");
    }
    
    /**
     * 判断是否为字段类型不匹配错误
     * @return 是否为字段类型不匹配错误
     */
    public boolean isColumnTypeMismatch() {
        return "COLUMN_TYPE_MISMATCH".equals(errorCode) ||
               (getMessage() != null && getMessage().toLowerCase().contains("data type mismatch"));
    }

    /**
     * 从错误消息中尽力提取缺失的列名（仅在列不存在类错误时有值）。
     * 支持匹配：
     * - "Invalid column name: weather"
     * - "Unknown column 'weather'"
     * - "Column does not exist: weather"
     */
    public String extractMissingColumnName() {
        String msg = getMessage();
        if (msg == null) return null;
        String m = msg.trim();

        // 1) Invalid column name: weather
        java.util.regex.Matcher mm = java.util.regex.Pattern
                .compile("(?i)invalid\\s+column\\s+name:?\\s*([A-Za-z0-9_]+)")
                .matcher(m);
        if (mm.find()) {
            return mm.group(1);
        }

        // 2) Unknown column 'weather'
        mm = java.util.regex.Pattern
                .compile("(?i)unknown\\s+column\\s+'([^']+)'")
                .matcher(m);
        if (mm.find()) {
            return mm.group(1);
        }

        // 3) Column does not exist: weather
        mm = java.util.regex.Pattern
                .compile("(?i)column\\s+does\\s+not\\s+exist:?\\s*([A-Za-z0-9_]+)")
                .matcher(m);
        if (mm.find()) {
            return mm.group(1);
        }

        return null;
    }
}
