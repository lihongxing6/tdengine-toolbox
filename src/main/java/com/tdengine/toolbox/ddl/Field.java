package com.tdengine.toolbox.ddl;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Date;

/**
 * 字段对象
 * 封装字段名称、类型和值，支持类型转换和值格式化
 * 
 * @author TDengine Toolbox
 * @version 1.0.0
 */
public class Field {
    
    private final String name;
    private final String type;
    private final Object value;
    
    /**
     * 构造函数
     * @param name 字段名称
     * @param type 字段类型（支持简写）
     * @param value 字段值
     */
    public Field(String name, String type, Object value) {
        this.name = name;
        this.type = normalizeType(type);
        this.value = value;
    }
    
    /**
     * 获取字段名称
     * @return 字段名称
     */
    public String getName() {
        return name;
    }
    
    /**
     * 获取规范化后的字段类型
     * @return 字段类型
     */
    public String getType() {
        return type;
    }
    
    /**
     * 获取字段值
     * @return 字段值
     */
    public Object getValue() {
        return value;
    }
    
    /**
     * 格式化字段值用于 SQL 语句
     * @return 格式化后的字符串值
     */
    public String formatValue() {
        if (value == null) {
            return "NULL";
        }

        String baseType = baseType(type);
        switch (baseType) {
            case "TIMESTAMP":
                return formatTimestamp(value);
            case "VARCHAR":
            case "NCHAR":
                return formatString(value);
            case "BOOL":
            case "BOOLEAN":
                return formatBoolean(value);
            case "DOUBLE":
            case "FLOAT":
            case "INT":
            case "BIGINT":
            case "SMALLINT":
            case "TINYINT":
                return formatNumber(value);
            case "BINARY":
                return formatBinary(value);
            default:
                return String.valueOf(value);
        }
    }
    
    /**
     * 规范化类型名称
     * 将简写类型转换为标准类型名称
     * @param type 原始类型
     * @return 规范化后的类型名称
     */
    public static String normalizeType(String type) {
        if (type == null || type.trim().isEmpty()) {
            throw new IllegalArgumentException("字段类型不能为空");
        }

        String raw = type.trim();
        String lower = raw.toLowerCase();

        // 数值类型映射（大小写/首字母大写均支持）
        if (lower.equals("d") || lower.equals("double")) return "DOUBLE";
        if (lower.equals("f") || lower.equals("float")) return "FLOAT";
        if (lower.equals("i") || lower.equals("int")) return "INT";
        if (lower.equals("l") || lower.equals("long")) return "BIGINT";

        // 时间类型映射
        if (lower.equals("t") || lower.equals("timestamp")) return "TIMESTAMP";

        // 字符串类型映射（支持 varchar / varchar(n) / s 简写）
        if (lower.equals("s") || lower.equals("varchar")) {
            return "VARCHAR(255)"; // 默认长度 255
        }
        if (lower.startsWith("varchar(")) {
            return raw.toUpperCase(); // 保留长度
        }

        // NCHAR 类似处理（可选）
        if (lower.equals("nchar")) return "NCHAR(255)";
        if (lower.startsWith("nchar(")) return raw.toUpperCase();

        // 布尔类型映射
        if (lower.equals("b") || lower.equals("bool") || lower.equals("boolean")) return "BOOL";

        // 其他类型直接转大写（如 BINARY、VARCHAR(100) 等）
        return raw.toUpperCase();
    }
    
    /**
     * 验证字段值是否有效
     * @return 是否有效
     */
    public boolean isValidValue() {
        if (value == null) {
            return true;  // NULL 值总是有效的
        }
        
        String baseType = baseType(type);
        switch (baseType) {
            case "TIMESTAMP":
                return isValidTimestamp(value);
            case "VARCHAR":
            case "NCHAR":
                return isValidString(value);
            case "BOOL":
            case "BOOLEAN":
                return isValidBoolean(value);
            case "DOUBLE":
                return isValidDouble(value);
            case "FLOAT":
                return isValidFloat(value);
            case "INT":
                return isValidInt(value);
            case "BIGINT":
                return isValidBigInt(value);
            default:
                return true;  // 其他类型暂时认为都有效
        }
    }

    /**
     * 提取基础类型（去掉长度等修饰），统一为大写
     */
    private static String baseType(String type) {
        if (type == null) return "";
        String t = type.trim().toUpperCase();
        int idx = t.indexOf('(');
        if (idx > 0) {
            t = t.substring(0, idx);
        }
        return t;
    }
    
    /**
     * 格式化时间戳
     */
    private String formatTimestamp(Object value) {
        if (value instanceof Long) {
            return "'" + new Timestamp((Long) value) + "'";
        }
        if (value instanceof Date) {
            return "'" + new Timestamp(((Date) value).getTime()) + "'";
        }
        if (value instanceof Timestamp) {
            return "'" + value + "'";
        }
        return "'" + value + "'";
    }
    
    /**
     * 格式化字符串
     */
    private String formatString(Object value) {
        String str = String.valueOf(value);
        // 转义单引号
        str = str.replace("'", "''");
        return "'" + str + "'";
    }
    
    /**
     * 格式化布尔值
     */
    private String formatBoolean(Object value) {
        if (value instanceof Boolean) {
            return ((Boolean) value) ? "1" : "0";
        }
        String str = String.valueOf(value).toLowerCase();
        return ("true".equals(str) || "1".equals(str)) ? "1" : "0";
    }
    
    /**
     * 格式化数字
     */
    private String formatNumber(Object value) {
        return String.valueOf(value);
    }
    
    /**
     * 格式化二进制数据
     */
    private String formatBinary(Object value) {
        // 简化处理，实际使用中可能需要更复杂的二进制处理
        return "'" + value + "'";
    }
    
    // 验证方法
    private boolean isValidTimestamp(Object value) {
        return value instanceof Long || value instanceof Date || value instanceof Timestamp;
    }
    
    private boolean isValidString(Object value) {
        return true;  // 任何对象都可以转换为字符串
    }
    
    private boolean isValidBoolean(Object value) {
        if (value instanceof Boolean) {
            return true;
        }
        String str = String.valueOf(value).toLowerCase();
        return "true".equals(str) || "false".equals(str) || "1".equals(str) || "0".equals(str);
    }
    
    private boolean isValidDouble(Object value) {
        return value instanceof Number;
    }
    
    private boolean isValidFloat(Object value) {
        return value instanceof Number;
    }
    
    private boolean isValidInt(Object value) {
        if (value instanceof Number) {
            long longValue = ((Number) value).longValue();
            return longValue >= Integer.MIN_VALUE && longValue <= Integer.MAX_VALUE;
        }
        return false;
    }
    
    private boolean isValidBigInt(Object value) {
        return value instanceof Number;
    }
    
    @Override
    public String toString() {
        return String.format("Field{name='%s', type='%s', value=%s}", name, type, value);
    }
}

