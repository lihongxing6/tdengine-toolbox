package com.tdengine.toolbox.example;

import com.tdengine.toolbox.core.TdEngineToolbox;
import com.tdengine.toolbox.ddl.Field;
import com.tdengine.toolbox.ddl.Table;
import com.tdengine.toolbox.util.TdEngineException;

import java.util.ArrayList;
import java.util.List;

/**
 * TDengine Toolbox 基础使用示例
 * 演示如何使用工具库进行数据插入
 * 
 * @author TDengine Toolbox
 * @version 1.0.0
 */
public class BasicExample {
    
    public static void main(String[] args) {
        
        try {
            // 示例1: 使用 REST 连接方式
            restExample();
            
            // 示例2: 使用 JDBC 连接方式
            // jdbcExample();
            
            // 示例3: 批量插入示例
            // batchInsertExample();
            
        } catch (TdEngineException e) {
            System.err.println("执行示例时发生错误: " + e.getMessage());
            e.printStackTrace();
        } finally {
            // 关闭工具箱
            TdEngineToolbox.shutdown();
        }
    }
    
    /**
     * REST 连接示例
     */
    public static void restExample() throws TdEngineException {
        System.out.println("=== REST 连接示例 ===");
        
        // 初始化连接（REST 方式）
        TdEngineToolbox.init("rest://127.0.0.1:6041/test?user=root&password=taosdata");
        
        // 创建表对象并添加字段
        Table table = new Table("sensor_data")
                .addField(new Field("ts", "t", System.currentTimeMillis()))
                .addField(new Field("temperature", "d", 23.5))
                .addField(new Field("humidity", "f", 65.2f))
                .addField(new Field("location", "s", "Beijing"));
        
        // 插入数据（自动建表和修复字段）
        TdEngineToolbox.insert(table);
        
        System.out.println("REST 连接插入完成");
    }
    
    /**
     * JDBC 连接示例
     */
    public static void jdbcExample() throws TdEngineException {
        System.out.println("=== JDBC 连接示例 ===");
        
        // 初始化连接（JDBC 方式）
        TdEngineToolbox.init("jdbc:TAOS://127.0.0.1:6030/test?user=root&password=taosdata", 
                           "root", "taosdata", false);
        
        // 创建更复杂的表
        Table weatherTable = new Table("weather_station")
                .addField("ts", "timestamp", System.currentTimeMillis())
                .addField("station_id", "i", 1001)
                .addField("temperature", "d", -5.5)
                .addField("pressure", "f", 1013.25f)
                .addField("wind_speed", "f", 12.5f)
                .addField("wind_direction", "i", 270)
                .addField("humidity", "i", 85)
                .addField("visibility", "d", 10.2)
                .addField("weather_desc", "varchar", "Light Snow");
        
        // 插入数据
        TdEngineToolbox.insert(weatherTable);
        
        System.out.println("JDBC 连接插入完成");
    }
    
    /**
     * 批量插入示例
     */
    public static void batchInsertExample() throws TdEngineException {
        System.out.println("=== 批量插入示例 ===");
        
        // 初始化连接
        TdEngineToolbox.init("rest://127.0.0.1:6041/test?user=root&password=taosdata");
        
        List<Table> tables = new ArrayList<>();
        
        // 创建多个时间点的数据
        long currentTime = System.currentTimeMillis();
        
        for (int i = 0; i < 5; i++) {
            Table table = new Table("batch_sensor_data")
                    .addField("ts", "t", currentTime + i * 1000)  // 每秒一条数据
                    .addField("sensor_id", "i", 100 + i)
                    .addField("value", "d", 20.0 + i * 0.5)
                    .addField("status", "b", i % 2 == 0);  // 布尔值交替
            
            tables.add(table);
        }
        
        // 批量插入
        TdEngineToolbox.insertBatch(tables);
        
        System.out.println("批量插入完成，共插入 " + tables.size() + " 条记录");
    }
    
    /**
     * 字段类型演示
     */
    public static void fieldTypeExample() throws TdEngineException {
        System.out.println("=== 字段类型演示 ===");
        
        TdEngineToolbox.init("rest://127.0.0.1:6041/test");
        
        Table table = new Table("data_types_demo")
                // 时间戳（主键）
                .addField("ts", "t", System.currentTimeMillis())
                
                // 数值类型
                .addField("double_val", "d", 123.456789)
                .addField("float_val", "f", 67.89f)
                .addField("int_val", "i", 12345)
                .addField("bigint_val", "l", 1234567890123L)
                
                // 字符串类型
                .addField("varchar_val", "s", "Hello TDengine")
                
                // 布尔类型
                .addField("bool_val", "b", true)
                
                // 使用完整类型名称
                .addField("full_double", "DOUBLE", 999.999)
                .addField("full_varchar", "VARCHAR(100)", "完整类型名称");
        
        TdEngineToolbox.insert(table);
        
        System.out.println("字段类型演示完成");
    }
    
    /**
     * 错误处理演示
     */
    public static void errorHandlingExample() {
        System.out.println("=== 错误处理演示 ===");
        
        try {
            // 尝试在未初始化时插入数据
            Table table = new Table("test_table")
                    .addField("ts", "t", System.currentTimeMillis())
                    .addField("value", "d", 100.0);
            
            TdEngineToolbox.insert(table);
            
        } catch (TdEngineException e) {
            System.out.println("捕获到预期的错误: " + e.getErrorCode() + " - " + e.getMessage());
        }
        
        try {
            // 初始化后正常插入
            TdEngineToolbox.init("rest://127.0.0.1:6041/test");
            
            Table table = new Table("error_demo")
                    .addField("ts", "t", System.currentTimeMillis())
                    .addField("message", "s", "错误处理演示");
            
            TdEngineToolbox.insert(table);
            System.out.println("错误处理演示 - 正常插入成功");
            
        } catch (TdEngineException e) {
            System.err.println("插入失败: " + e.getMessage());
        }
    }
}