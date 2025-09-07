package com.tdengine.toolbox.core;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 查询结果通用结构（同时适配 JDBC 与 REST）。
 */
public class QueryResult {
    private final List<ColumnMeta> columns;
    private final List<List<Object>> rows;
    private final Integer rowsAffected; // 对于非查询或影响行数可用

    public QueryResult(List<ColumnMeta> columns, List<List<Object>> rows, Integer rowsAffected) {
        this.columns = columns == null ? Collections.emptyList() : new ArrayList<>(columns);
        this.rows = rows == null ? Collections.emptyList() : new ArrayList<>(rows);
        this.rowsAffected = rowsAffected;
    }

    public List<ColumnMeta> getColumns() { return new ArrayList<>(columns); }
    public List<List<Object>> getRows() { return new ArrayList<>(rows); }
    public int rowCount() { return rows.size(); }
    public Integer getRowsAffected() { return rowsAffected; }

    /**
     * 将结果转换为 List<Map<String,Object>>，每行一个 Map，key 为列名，value 为对应值。
     */
    public List<Map<String, Object>> toMapList() {
        List<Map<String, Object>> list = new ArrayList<>(rows.size());
        int colCount = columns.size();
        // 预先提取列名，保持顺序
        List<String> names = new ArrayList<>(colCount);
        for (int i = 0; i < colCount; i++) {
            String name = columns.get(i).getName();
            names.add(name == null ? ("col" + i) : name);
        }
        for (List<Object> row : rows) {
            Map<String, Object> map = new HashMap<>();
            int n = Math.min(colCount, row.size());
            for (int i = 0; i < n; i++) {
                map.put(names.get(i), row.get(i));
            }
            list.add(map);
        }
        return list;
    }
}
