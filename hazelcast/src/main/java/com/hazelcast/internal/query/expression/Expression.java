package com.hazelcast.internal.query.expression;

import com.hazelcast.sql.impl.QueryContext;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.nio.serialization.DataSerializable;

public interface Expression<T> extends DataSerializable {

    T eval(QueryContext ctx, Row row);
}
