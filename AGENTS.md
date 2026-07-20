# Java后端项目规范

## 项目通用规范
1. 接口业务逻辑中需要使用异常的场景，建议使用BusinessException，以返回标准数据格式

## MyBatis-Plus 使用规范
1. 后端新增数据库表对应的 DO/Mapper 时，应同步新增 `com.mtx.trade.service.db.XxxDbService` 和 `com.mtx.trade.service.db.impl.XxxDbServiceImpl`。
2. `XxxDbService` 继承 `IService<XxxDO>`，`XxxDbServiceImpl` 继承 `ServiceImpl<XxxMapper, XxxDO>` 并实现对应 DB Service。
3. 业务服务中优先注入 `XxxDbService` 使用 MyBatis-Plus 提供的 `getById`、`list`、`getOne`、`save`、`updateById`、`removeById`、`saveBatch` 等通用方法；复杂跨表编排仍保留在业务 Service 或 Repository 中。

## SQL规范
1. 每天的新变更sql文件按模块名-天命名放在docs/sql目录下， 形如：trade-pipeline-2026-07-20.sql

## 接口规范
### 通用规约
1. Controller 只能使用 `GET`、`POST`
    1. `GET` 只能用于查询，不允许触发新增、修改、删除、同步、上传、分析等副作用操作
    2. `POST` 用于所有带副作用的操作，包括新增、修改、删除、同步、上传、分析、导入、导出触发等
2. 接口名要让人只看路径就知道这个接口做什么，HTTP Method 只负责区分查询还是变更，不承担具体业务语义
3. 所有接口返回最外层必须是 `com.mtx.trade.common.dto.ResponseData`，返回类型形如 `ResponseData<XxxVO>`，`ResponseData<String>`
4. 根据业务情况，决定是否需要分页功能，分页对象: `com.telesales.training.dto.common.PageResult`，分页接口返回类型形如 `ResponseData<PageResult<XxxVO>>`