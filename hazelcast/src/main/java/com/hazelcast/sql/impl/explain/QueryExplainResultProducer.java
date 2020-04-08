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

package com.hazelcast.sql.impl.explain;

import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.QueryResultProducer;
import com.hazelcast.sql.impl.row.Row;

import java.util.Iterator;

/**
 * Result producer for EXPLAIN statement.
 */
// TODO: Remove this class.
public class QueryExplainResultProducer implements QueryResultProducer {

    private final QueryExplain explain;
    private final Iterator<Row> iterator;

    public QueryExplainResultProducer(QueryExplain explain) {
        this.explain = explain;

        iterator = explain.asRows().iterator();
    }

    @Override
    public Iterator<Row> iterator() {
        return iterator;
    }

    @Override
    public void onError(QueryException error) {
        // No-op.
    }
}
