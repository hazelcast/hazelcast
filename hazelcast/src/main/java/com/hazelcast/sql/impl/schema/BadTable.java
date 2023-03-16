package com.hazelcast.sql.impl.schema;

import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.optimizer.PlanObjectKey;

import java.util.Collections;
import java.util.List;

/**
 * Placeholder for a table/mapping that exists in the catalog but is not valid.
 * Allows to partially parse SQL query and generate meaningful error message
 * for the user if the mapping is used in the query.
 */
public final class BadTable extends Table {
    private final QueryException cause;

    public BadTable(String schemaName, String sqlName, Throwable cause) {
        super(schemaName, sqlName, Collections.emptyList(), new ConstantTableStatistics(0));
        this.cause = new QueryException(String.format("Mapping '%s' references unknown class: %s", sqlName, cause),
                cause);
    }

    // not all methods throw to allow some parsing to occur
    @Override
    public List<TableField> getFields() {
        throw cause;
    }

    @Override
    public PlanObjectKey getObjectKey() {
        throw cause;
    }
}
