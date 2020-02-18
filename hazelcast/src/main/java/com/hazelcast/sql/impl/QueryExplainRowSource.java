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

import com.hazelcast.sql.HazelcastSqlException;
import com.hazelcast.sql.SqlRow;
import com.hazelcast.sql.impl.state.QueryStateRowSource;

import java.util.Iterator;

public class QueryExplainRowSource implements QueryStateRowSource {
    /** Explain */
    private final QueryExplain explain;

    public QueryExplainRowSource(QueryExplain explain) {
        this.explain = explain;
    }

    @Override
    public Iterator<SqlRow> iterator() {
        return explain.asRows().iterator();
    }

    @Override
    public boolean isExplain() {
        return true;
    }

    @Override
    public void onError(HazelcastSqlException error) {
        // No-op, since EXPLAIN is local operation.
    }
}
