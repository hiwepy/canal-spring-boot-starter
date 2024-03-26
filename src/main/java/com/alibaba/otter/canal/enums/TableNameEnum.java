package com.alibaba.otter.canal.enums;

import java.util.StringJoiner;

public enum TableNameEnum {

    ALL("*", "*", "*");

    public static final CharSequence DELIMITER = ".";

    String destination;
    String schema;
    String table;

    TableNameEnum(String destination, String schema, String table) {
        this.destination = destination;
        this.schema = schema;
        this.table = table;
    }

    public String getDestination() {
        return destination;
    }

    public String getSchema() {
        return schema;
    }

    public String getTable() {
        return table;
    }

    @Override
    public String toString() {
        StringJoiner joiner = new StringJoiner(".").add(schema).add(table);
        return joiner.toString();
    }

}
