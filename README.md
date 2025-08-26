# TDengine Toolbox

面向对象、轻量、易扩展的 TDengine 工具库，支持自动管理表结构和字段变动。

## 特性

- **双连接方式**：支持 RESTful 和 JDBC 两种连接方式
- **智能插入**：自动建表、添加字段、修改字段类型
- **零日志依赖**：提供简单日志接口，可桥接任意日志框架
- **类型简写**：支持字段类型简写，如 `d` 代表 `DOUBLE`
- **链式调用**：流畅的 API 设计，支持链式操作
- **连接池管理**：内置连接池，高效管理数据库连接

## 快速开始

### Maven 依赖

```xml
<dependency>
    <groupId>com.tdengine</groupId>
    <artifactId>tdengine-toolbox</artifactId>
    <version>1.0.0</version>
</dependency>
```

### 基础使用

```java
import com.tdengine.toolbox.core.TdEngineToolbox;
import com.tdengine.toolbox.ddl.Table;
import com.tdengine.toolbox.ddl.Field;

// 初始化连接
TdEngineToolbox.init("rest://127.0.0.1:6041/test?user=root&password=taosdata");

// 创建表对象并添加字段
Table table = new Table("sensor_data")
    .addField(new Field("ts", "t", System.currentTimeMillis()))
    .addField(new Field("temperature", "d", 23.5))
    .addField(new Field("location", "s", "Beijing"));

// 执行智能插入
TdEngineToolbox.insert(table);

// 关闭工具箱
TdEngineToolbox.shutdown();
```

## 核心功能

### 1. 智能插入逻辑

工具库会自动处理以下情况：

- **表不存在** → 自动执行 `CREATE TABLE`
- **字段不存在** → 自动执行 `ALTER TABLE ADD COLUMN`
- **字段类型不匹配** → 自动执行 `ALTER TABLE DROP COLUMN` + `ADD COLUMN`

### 2. 支持的数据类型

| 简写 | 完整类型 | 说明 |
|------|----------|------|
| `d`, `D`, `double` | DOUBLE | 双精度浮点数 |
| `f`, `F`, `float` | FLOAT | 单精度浮点数 |
| `i`, `I`, `int` | INT | 32位整数 |
| `l`, `L`, `long` | BIGINT | 64位整数 |
| `t`, `T`, `timestamp` | TIMESTAMP | 时间戳 |
| `s`, `S`, `varchar` | VARCHAR(255) | 字符串 |
| `b`, `B`, `bool`, `boolean` | BOOL | 布尔值 |

### 3. 连接方式

#### REST 连接
```java
TdEngineToolbox.init("rest://127.0.0.1:6041/database?user=root&password=taosdata");
```

#### JDBC 连接
```java
TdEngineToolbox.init("jdbc:TAOS://127.0.0.1:6030/database", "root", "taosdata", false);
```

### 4. 批量插入

```java
List<Table> tables = new ArrayList<>();

// 创建多个表对象
for (int i = 0; i < 100; i++) {
    Table table = new Table("batch_data")
        .addField("ts", "t", System.currentTimeMillis() + i * 1000)
        .addField("value", "d", i * 0.1);
    tables.add(table);
}

// 批量插入
TdEngineToolbox.insertBatch(tables);
```

## 架构设计

```
com.tdengine.toolbox
├── core/              # 核心执行器
│   ├── TdEngineToolbox.java
│   └── ConnectionManager.java
├── ddl/               # 数据定义层
│   ├── Table.java
│   └── Field.java
├── logger/            # 日志接口
│   ├── Logger.java
│   └── DefaultLogger.java
└── util/              # 工具类
    └── TdEngineException.java
```

## 高级用法

### 自定义日志

```java
// 实现自定义日志记录器
Logger customLogger = new Logger() {
    @Override
    public void info(String message) {
        MyLogger.log(message);
    }
    // 实现其他方法...
};

// 设置日志记录器
TdEngineToolbox.setLogger(customLogger);
```

### 错误处理

```java
try {
    TdEngineToolbox.insert(table);
} catch (TdEngineException e) {
    if (e.isTableNotExist()) {
        // 处理表不存在错误
    } else if (e.isColumnNotExist()) {
        // 处理字段不存在错误
    }
}
```

### 连接状态检查

```java
if (TdEngineToolbox.isConnected()) {
    // 连接正常，执行操作
    TdEngineToolbox.insert(table);
}
```

## 配置选项

### 系统属性

- `tdengine.toolbox.debug=true` - 启用调试日志

### 环境要求

- Java 8+
- TDengine 2.4+
- Maven 3.6+

## 示例项目

参考 [`BasicExample.java`](src/main/java/com/tdengine/toolbox/example/BasicExample.java) 获取更多使用示例。

## 贡献

欢迎提交 Issue 和 Pull Request！

## 许可证

本项目采用 Apache 2.0 许可证。

## 更新日志

### v1.0.0
- 初始版本发布
- 支持 REST 和 JDBC 双连接方式
- 实现智能插入逻辑
- 支持批量操作
- 提供完整的使用示例