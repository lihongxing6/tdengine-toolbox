# TDEngine工具箱 (tdengine-toolbox)

TDEngine数据库工具箱 - 一个用于简化TDEngine数据库操作的Java工具库。

## 功能特性

- 简化的连接管理
- 类型安全的数据操作
- 内置日志系统
- 异常处理机制

## 快速开始

```java
// 创建工具箱实例
TdEngineToolbox toolbox = new TdEngineToolbox("jdbc:TAOS://localhost:6030/test", "root", "taosdata");

// 使用示例
BasicExample example = new BasicExample();
example.run();
```

## 项目结构

- `core/` - 核心功能模块
- `ddl/` - 数据定义语言相关
- `logger/` - 日志系统
- `util/` - 工具类
- `example/` - 使用示例
