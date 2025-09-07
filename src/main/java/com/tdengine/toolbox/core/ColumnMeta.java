package com.tdengine.toolbox.core;

/**
 * 列元数据
 */
public class ColumnMeta {
    private final String name;
    private final String type;
    private final Integer length; // 可为空

    public ColumnMeta(String name, String type, Integer length) {
        this.name = name;
        this.type = type;
        this.length = length;
    }

    public String getName() { return name; }
    public String getType() { return type; }
    public Integer getLength() { return length; }
}

