package com.alibaba.otter.canal.util;

import com.alibaba.otter.canal.protocol.CanalEntry;

import java.util.List;
import java.util.Objects;

/**
 * Lookup helpers for extracting column values from a Canal
 * {@link CanalEntry.RowData} before-/after-image.
 *
 * @author [@Loong Wan](https://github.com/loong10k)
 * @since 1.0.0
 */
public class RowDataUtil {

    /**
     * Returns the before-image value of the named column.
     *
     * @param rowData    the row data to inspect
     * @param columnName the column name (case-insensitive)
     * @return the before-image value, or {@code null} if not present
     */
    public static String getBeforeValue(CanalEntry.RowData rowData, String columnName) {
        if(Objects.isNull(rowData)){
            return null;
        }
        List<CanalEntry.Column> beforeColumnsList = rowData.getBeforeColumnsList();
        if(Objects.isNull(beforeColumnsList)){
            return null;
        }
        for (CanalEntry.Column column : beforeColumnsList) {
            if (column.getName().equalsIgnoreCase(columnName)) {
                return Objects.toString(column.getValue(), null);
            }
        }
        return null;
    }

    /**
     * Returns the after-image value of the named column.
     *
     * @param rowData    the row data to inspect
     * @param columnName the column name (case-insensitive)
     * @return the after-image value, or {@code null} if not present
     */
    public static String getAfterValue(CanalEntry.RowData rowData, String columnName) {
        if(Objects.isNull(rowData)){
            return null;
        }
        List<CanalEntry.Column> afterColumnsList = rowData.getAfterColumnsList();
        if(Objects.isNull(afterColumnsList)){
            return null;
        }
        for (CanalEntry.Column column : afterColumnsList) {
            if (column.getName().equalsIgnoreCase(columnName)) {
                return  Objects.toString(column.getValue(), null);
            }
        }
        return null;
    }

    /**
     * Returns the value of the named column, preferring the before-image and
     * falling back to the after-image when the before-image is absent.
     *
     * @param rowData    the row data to inspect
     * @param columnName the column name (case-insensitive)
     * @return the resolved value, or {@code null} if not present in either image
     */
    public static String getValue(CanalEntry.RowData rowData, String columnName) {
        String value = getBeforeValue(rowData, columnName);
        if(Objects.isNull(value)){
            return getAfterValue(rowData, columnName);
        }
        return value;
    }

}
