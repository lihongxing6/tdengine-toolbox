package com.tdengine.toolbox.logger;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * 默认日志实现
 * 使用 System.out 和 System.err 进行日志输出
 * 
 * @author TDengine Toolbox
 * @version 1.0.0
 */
public class DefaultLogger implements Logger {
    
    private final String name;
    private final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    
    /**
     * 构造函数
     * @param name 日志记录器名称
     */
    public DefaultLogger(String name) {
        this.name = name;
    }
    
    /**
     * 构造函数，使用默认名称
     */
    public DefaultLogger() {
        this("TDengineToolbox");
    }
    
    @Override
    public void debug(String message) {
        if (isDebugEnabled()) {
            logMessage("DEBUG", message, null);
        }
    }
    
    @Override
    public void debug(String message, Throwable throwable) {
        if (isDebugEnabled()) {
            logMessage("DEBUG", message, throwable);
        }
    }
    
    @Override
    public void info(String message) {
        if (isInfoEnabled()) {
            logMessage("INFO", message, null);
        }
    }
    
    @Override
    public void info(String message, Throwable throwable) {
        if (isInfoEnabled()) {
            logMessage("INFO", message, throwable);
        }
    }
    
    @Override
    public void warn(String message) {
        if (isWarnEnabled()) {
            logMessage("WARN", message, null);
        }
    }
    
    @Override
    public void warn(String message, Throwable throwable) {
        if (isWarnEnabled()) {
            logMessage("WARN", message, throwable);
        }
    }
    
    @Override
    public void error(String message) {
        if (isErrorEnabled()) {
            logMessage("ERROR", message, null);
        }
    }
    
    @Override
    public void error(String message, Throwable throwable) {
        if (isErrorEnabled()) {
            logMessage("ERROR", message, throwable);
        }
    }
    
    @Override
    public boolean isDebugEnabled() {
        return "true".equalsIgnoreCase(System.getProperty("tdengine.toolbox.debug", "false"));
    }
    
    @Override
    public boolean isInfoEnabled() {
        return true;
    }
    
    @Override
    public boolean isWarnEnabled() {
        return true;
    }
    
    @Override
    public boolean isErrorEnabled() {
        return true;
    }
    
    /**
     * 统一日志消息输出方法
     */
    private void logMessage(String level, String message, Throwable throwable) {
        String timestamp = dateFormat.format(new Date());
        String logLine = String.format("[%s] %s - %s - %s", timestamp, level, name, message);
        
        if ("ERROR".equals(level) || "WARN".equals(level)) {
            System.err.println(logLine);
        } else {
            System.out.println(logLine);
        }
        
        if (throwable != null) {
            throwable.printStackTrace(System.err);
        }
    }
}