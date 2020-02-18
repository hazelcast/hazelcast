/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.sql.impl;

import com.hazelcast.sql.impl.exec.RootExec;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.row.RowBatch;
import com.hazelcast.sql.impl.state.QueryStateRowSource;

/**
 * Consumer of results.
 */
public interface QueryResultConsumer extends QueryStateRowSource {
    /**
     * Perform one-time setup.
     *
     * @param root Root.
     */
    void setup(RootExec root);

    /**
     * Consume rows from the batch.
     *
     * @param batch Batch.
     * @param startPos Start position in the batch.
     * @return Number of rows consumed.
     */
    int consume(RowBatch batch, int startPos);

    /**
     * Consume rows from the source.
     *
     * @param source Row source.
     * @return {@code True} if consumed.
     */
    boolean consume(Iterable<Row> source);

    /**
     * Mark results as finished.
     */
    void done();

    @Override
    default boolean isExplain() {
        return false;
    }
}
