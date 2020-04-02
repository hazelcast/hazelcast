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

package com.hazelcast.sql.impl.exec.root;

import com.hazelcast.sql.impl.QueryResultProducer;
import com.hazelcast.sql.impl.worker.QueryFragmentContext;
import com.hazelcast.sql.impl.row.Row;

import java.util.List;

/**
 * Consumer of results from {@link RootExec}.
 */
public interface RootResultConsumer extends QueryResultProducer {
    /**
     * Perform one-time setup.
     *
     * @param context Context.
     */
    void setup(QueryFragmentContext context);

    /**
     * Consume rows from the root operator. The implementation should either consume all rows, or none. If the rows are consumed,
     * the ownership is transferred to the consumer, i.e. no copy of the batch is required.
     *
     * @param batch Rows.
     * @param last Whether this is the last batch.
     * @return {@code true} if rows were consumed, {@code false} otherwise.
     */
    boolean consume(List<Row> batch, boolean last);
}
