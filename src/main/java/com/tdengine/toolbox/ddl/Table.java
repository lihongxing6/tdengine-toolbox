package com.tdengine.toolbox.ddl;

import java.util.ArrayList;
import java.util.List;

/**
 * 表对象
 * 管理字段列表，生成建表语句和插入语句
 * 
 * @author TDengine Toolbox
 * @version 1.0.0
 */
public class Table {
    
    private final String name;
    private String database;
    private final List<Field> fields;
    
    /**
     * 构造函数
     * @param name 表名
     */
    public Table(String name) {
        if (name == null || name.trim().isEmpty()) {
            throw new IllegalArgumentException("表名不能为空");
        }
        this.name = name.trim();
        this.fields = new ArrayList<>();
    }
    
    /**
     * 构造函数，指定数据库名和表名
     * @param database 数据库名
     * @param name 表名
     */
    public Table(String database, String name) {
        this(name);
        this.database = database;
    }
    
    /**
     * 获取表名
     * @return 表名
     */
    public String getName() {
        return name;
    }
    
    /**
     * 获取数据库名
     * @return 数据库名
     */
    public String getDatabase() {
        return database;
    }
    
    /**
     * 设置数据库名
     * @param database 数据库名
     * @return Table 实例，支持链式调用
     */
    public Table setDatabase(String database) {
        this.database = database;
        return this;
    }
    
    /**
     * 获取完整表名（包含数据库前缀）
     * @return 完整表名
     */
    public String getFullName() {
        if (database != null && !database.trim().isEmpty()) {
            return database.trim() + "." + name;
        }
        return name;
    }
    
    /**
     * 获取字段列表
     * @return 字段列表
     */
    public List<Field> getFields() {
        return new ArrayList<>(fields);  // 返回副本，避免外部修改
    }
    
    /**
     * 添加字段
     * @param field 字段对象
     * @return Table 实例，支持链式调用
     */
    public Table addField(Field field) {
        if (field == null) {
            throw new IllegalArgumentException("字段不能为空");
        }
        
        // 检查字段名是否重复
        for (Field existingField : fields) {
            if (existingField.getName().equalsIgnoreCase(field.getName())) {
                throw new IllegalArgumentException("字段名重复: " + field.getName());
            }
        }
        
        // 验证字段值的有效性
        if (!field.isValidValue()) {
            throw new IllegalArgumentException("字段值无效: " + field);
        }
        
        fields.add(field);
        return this;
    }
    
    /**
     * 添加字段（便捷方法）
     * @param name 字段名
     * @param type 字段类型
     * @param value 字段值
     * @return Table 实例，支持链式调用
     */
    public Table addField(String name, String type, Object value) {
        return addField(new Field(name, type, value));
    }
    
    /**
     * 清空所有字段
     * @return Table 实例，支持链式调用
     */
    public Table clearFields() {
        fields.clear();
        return this;
    }
    
    /**
     * 生成建表 SQL 语句
     * @return 建表 SQL
     */
    public String toCreateSql() {
        if (fields.isEmpty()) {
            throw new IllegalStateException("至少需要一个字段才能生成建表语句");
        }
        
        StringBuilder sql = new StringBuilder();
        sql.append("CREATE TABLE IF NOT EXISTS ").append(getFullName()).append(" (");
        
        for (int i = 0; i < fields.size(); i++) {
            Field field = fields.get(i);
            if (i > 0) {
                sql.append(", ");
            }
            sql.append(field.getName()).append(" ").append(field.getType());
        }
        
        sql.append(")");
        return sql.toString();
    }
    
    /**
     * 生成插入 SQL 语句
     * @return 插入 SQL 语句列表
     */
    public List<String> toInsertSqls() {
        List<String> sqls = new ArrayList<>();
        
        if (fields.isEmpty()) {
            return sqls;
        }
        
        // 生成单条插入语句
        StringBuilder sql = new StringBuilder();
        sql.append("INSERT INTO ").append(getFullName()).append(" (");
        
        // 添加字段名
        for (int i = 0; i < fields.size(); i++) {
            if (i > 0) {
                sql.append(", ");
            }
            sql.append(fields.get(i).getName());
        }
        
        sql.append(") VALUES (");
        
        // 添加字段值
        for (int i = 0; i < fields.size(); i++) {
            if (i > 0) {
                sql.append(", ");
            }
            sql.append(fields.get(i).formatValue());
        }
        
        sql.append(")");
        sqls.add(sql.toString());
        
        return sqls;
    }
    
    /**
     * 生成单条插入语句
     * @return 插入 SQL 语句
     */
    public String toInsertSql() {
        List<String> sqls = toInsertSqls();
        return sqls.isEmpty() ? "" : sqls.get(0);
    }
    
    /**
     * 检查表是否包含指定字段
     * @param fieldName 字段名
     * @return 是否包含
     */
    public boolean hasField(String fieldName) {
        if (fieldName == null) {
            return false;
        }
        
        for (Field field : fields) {
            if (field.getName().equalsIgnoreCase(fieldName)) {
                return true;
            }
        }
        return false;
    }
    
    /**
     * 获取指定名称的字段
     * @param fieldName 字段名
     * @return 字段对象，如果不存在则返回 null
     */
    public Field getField(String fieldName) {
        if (fieldName == null) {
            return null;
        }
        
        for (Field field : fields) {
            if (field.getName().equalsIgnoreCase(fieldName)) {
                return field;
            }
        }
        return null;
    }
    
    /**
     * 获取字段数量
     * @return 字段数量
     */
    public int getFieldCount() {
        return fields.size();
    }
    
    /**
     * 检查表定义是否有效
     * @return 是否有效
     */
    public boolean isValid() {
        if (fields.isEmpty()) {
            return false;
        }
        
        for (Field field : fields) {
            if (!field.isValidValue()) {
                return false;
            }
        }
        return true;
    }
    
    @Override
    public String toString() {
        return String.format("Table{name='%s', database='%s', fields=%d}", name, database, fields.size());
    }
}