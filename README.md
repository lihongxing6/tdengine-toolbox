# TDengine Toolbox

轻量的 TDengine JDBC/REST 工具箱，提供智能插入（自动建表/加列/类型修复）、批量写入与可插拔日志。

## Maven 引用

在你的项目 `pom.xml` 增加依赖（发布后请使用最新版本号）：

```xml
<dependency>
    <groupId>io.github.lihongxing6</groupId>
    <artifactId>tdengine-toolbox</artifactId>
    <version>1.0.0</version>
</dependency>
```

## 快速调用

工具箱为静态入口（无需 new）。最少三步：初始化 → 插入 → 关闭。

```java
import com.tdengine.toolbox.core.TdEngineToolbox;
import com.tdengine.toolbox.ddl.Table;
import com.tdengine.toolbox.ddl.Field;
import com.tdengine.toolbox.util.TdEngineException;

public class QuickStart {
    public static void main(String[] args) throws TdEngineException {
        // 1) 初始化（REST 方式）: URL 中可包含数据库、用户名、密码
        TdEngineToolbox.init("rest://127.0.0.1:6041/test?user=root&password=taosdata");

        // 2) 构造表数据并插入（智能建表/修复字段）
        Table table = new Table("sensor_data")
            .addField(new Field("ts", "t", System.currentTimeMillis()))
            .addField(new Field("temperature", "d", 23.5))
            .addField(new Field("humidity", "f", 65.2f))
            .addField(new Field("location", "s", "Beijing"));

        TdEngineToolbox.insert(table);

        // 3) 关闭
        TdEngineToolbox.shutdown();
    }
}
```

JDBC 初始化与调用：

```java
TdEngineToolbox.init(
    "jdbc:TAOS://127.0.0.1:6030/test", // JDBC URL
    "root",                               // 用户名
    "taosdata",                           // 密码
    false                                  // false = JDBC, true = REST
);

Table t = new Table("weather_station")
    .addField("ts", "timestamp", System.currentTimeMillis())
    .addField("station_id", "i", 1001)
    .addField("temperature", "d", -5.5)
    .addField("weather_desc", "varchar(100)", "Light Snow");

TdEngineToolbox.insert(t);
```

## 批量插入

```java
List<Table> tables = new ArrayList<>();
long base = System.currentTimeMillis();
for (int i = 0; i < 5; i++) {
    tables.add(new Table("batch_sensor")
        .addField("ts", "t", base + i * 1000)
        .addField("sensor_id", "i", 100 + i)
        .addField("value", "d", 20.0 + i * 0.5)
        .addField("status", "b", i % 2 == 0));
}
TdEngineToolbox.insertBatch(tables);
```

## 字段类型与简写

以下写法等价，大小写不敏感：

- DOUBLE：`d`, `double`, `Double`
- FLOAT：`f`, `float`, `Float`
- INT：`i`, `int`, `Int`
- BIGINT：`l`, `long`, `Long`
- TIMESTAMP：`t`, `timestamp`
- VARCHAR(n)：`s`, `varchar`, `varchar(100)`（默认 255）
- NCHAR(n)：`nchar`, `nchar(64)`（默认 255）
- BOOL：`b`, `bool`, `boolean`

字符串类型（`VARCHAR`, `NCHAR`）会自动加引号；布尔类型会格式化为 `1/0`。

## 日志与调试

- 启用调试日志：启动参数加 `-Dtdengine.toolbox.debug=true`
- 自定义日志实现：实现 `com.tdengine.toolbox.logger.Logger` 接口，并调用 `TdEngineToolbox.setLogger(yourLogger)`

## 错误处理

`TdEngineToolbox.insert(...)` 遇到以下错误会自动修复并重试：

- 表不存在：自动 `CREATE TABLE`
- 字段不存在：自动 `ALTER TABLE ADD COLUMN`
- 字段类型不匹配：自动 `ALTER TABLE DROP COLUMN` + `ADD COLUMN`

可通过 `TdEngineException` 判断错误类型：`isTableNotExist()`、`isColumnNotExist()`、`isColumnTypeMismatch()`。

## 注意事项

- REST 方式请确保 URL 中包含数据库名（例如 `rest://host:6041/test?...`）。
- 若在 REST 下使用其他数据库名，也可通过 `new Table("db", "table")` 或 `table.setDatabase("db")` 指定。
- 使用完成务必调用 `TdEngineToolbox.shutdown()` 释放连接资源。

## 目录结构

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

<!-- 父 POM使用说明已迁移至 docs/DEV_NOTES.md（仅供作者参考） -->
