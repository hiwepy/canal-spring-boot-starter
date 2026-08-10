package com.alibaba.otter.canal.enums;

import java.util.StringJoiner;

/**
 * Wildcard table name constants used to bind an {@code EntryHandler} to every
 * destination/schema/table.
 * <p>
 * The single constant {@link #ALL} matches any destination, schema and table
 * using the {@code "*"} wildcard for each segment.
 * </p>
 *
 * @author <a href="https://github.com/loong10k">Loong Wan</a>
 * @since 1.0.0
 */
public enum TableNameEnum {

    /** Wildcard matching any destination, schema and table. */
    ALL("*", "*", "*");

    /** Delimiter used when joining schema and table segments. */
    public static final CharSequence DELIMITER = ".";

    /** Destination segment of the wildcard. */
    String destination;
    /** Schema segment of the wildcard. */
    String schema;
    /** Table segment of the wildcard. */
    String table;

    TableNameEnum(String destination, String schema, String table) {
        this.destination = destination;
        this.schema = schema;
        this.table = table;
    }

    /** @return the destination segment */
    public String getDestination() {
        return destination;
    }

    /** @return the schema segment */
    public String getSchema() {
        return schema;
    }

    /** @return the table segment */
    public String getTable() {
        return table;
    }

    @Override
    public String toString() {
        StringJoiner joiner = new StringJoiner(".").add(schema).add(table);
        return joiner.toString();
    }

}
