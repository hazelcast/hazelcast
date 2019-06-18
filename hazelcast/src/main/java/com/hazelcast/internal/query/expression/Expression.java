package com.hazelcast.internal.query.expression;

import com.hazelcast.internal.query.QueryContext;
import com.hazelcast.internal.query.io.Row;
import com.hazelcast.nio.serialization.DataSerializable;

public interface Expression<T> extends DataSerializable {

    T eval(QueryContext ctx, Row row);
}
