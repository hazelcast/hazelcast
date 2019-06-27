package com.hazelcast.sql.impl;

import com.hazelcast.internal.query.QueryHandleImpl;
import com.hazelcast.sql.SqlCursor;
import com.hazelcast.sql.SqlRow;

import javax.annotation.Nonnull;
import java.util.Iterator;

/**
 * Cursor implementation.
 */
public class SqlCursorImpl implements SqlCursor {
    /** Handle. */
    private final QueryHandleImpl handle;

    public SqlCursorImpl(QueryHandleImpl handle) {
        this.handle = handle;
    }

    @Override @Nonnull
    public Iterator<SqlRow> iterator() {
        return handle.getConsumer().iterator();
    }

    @Override
    public void close() throws Exception {
        handle.close();
    }
}
