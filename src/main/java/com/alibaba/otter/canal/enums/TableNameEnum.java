package com.alibaba.otter.canal.enums;

import java.util.StringJoiner;

public enum TableNameEnum {

    ALL("*", "*");

    String schemaName;
    String tableName;

    TableNameEnum(String schemaName, String tableName) {
        this.schemaName = schemaName;
        this.tableName = tableName;
    }

    @Override
    public String toString() {
        StringJoiner joiner = new StringJoiner(".").add(schemaName).add(tableName);
        return joiner.toString();
    }

}
