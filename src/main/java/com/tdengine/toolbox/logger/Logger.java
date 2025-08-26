package com.tdengine.toolbox.logger;

/**
 * 日志接口
 * 提供简单的日志记录能力，用户可以桥接到任意日志框架
 * 
 * @author TDengine Toolbox
 * @version 1.0.0
 */
public interface Logger {
    
    /**
     * 记录调试级别日志
     * @param message 日志消息
     */
    void debug(String message);
    
    /**
     * 记录调试级别日志
     * @param message 日志消息
     * @param throwable 异常信息
     */
    void debug(String message, Throwable throwable);
    
    /**
     * 记录信息级别日志
     * @param message 日志消息
     */
    void info(String message);
    
    /**
     * 记录信息级别日志
     * @param message 日志消息
     * @param throwable 异常信息
     */
    void info(String message, Throwable throwable);
    
    /**
     * 记录警告级别日志
     * @param message 日志消息
     */
    void warn(String message);
    
    /**
     * 记录警告级别日志
     * @param message 日志消息
     * @param throwable 异常信息
     */
    void warn(String message, Throwable throwable);
    
    /**
     * 记录错误级别日志
     * @param message 日志消息
     */
    void error(String message);
    
    /**
     * 记录错误级别日志
     * @param message 日志消息
     * @param throwable 异常信息
     */
    void error(String message, Throwable throwable);
    
    /**
     * 判断是否启用调试级别日志
     * @return 是否启用
     */
    boolean isDebugEnabled();
    
    /**
     * 判断是否启用信息级别日志
     * @return 是否启用
     */
    boolean isInfoEnabled();
    
    /**
     * 判断是否启用警告级别日志
     * @return 是否启用
     */
    boolean isWarnEnabled();
    
    /**
     * 判断是否启用错误级别日志
     * @return 是否启用
     */
    boolean isErrorEnabled();
}