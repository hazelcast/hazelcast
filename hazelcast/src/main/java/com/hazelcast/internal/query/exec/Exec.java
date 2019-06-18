package com.hazelcast.internal.query.exec;

import com.hazelcast.internal.query.QueryContext;
import com.hazelcast.internal.query.io.Row;

import java.util.function.Consumer;

/**
 * Basic execution stage.
 */
public interface Exec {

    void setup(QueryContext ctx);

    IterationResult next();

    void consume(Consumer<Row> consumer);

    int remainingRows();
}
