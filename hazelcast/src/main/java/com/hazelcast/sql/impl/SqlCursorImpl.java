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
import com.hazelcast.sql.SqlCursor;
import com.hazelcast.sql.SqlErrorCode;
import com.hazelcast.sql.SqlRow;
import com.hazelcast.sql.impl.state.QueryInitiatorState;
import com.hazelcast.sql.impl.state.QueryState;

import javax.annotation.Nonnull;
import java.util.Iterator;

/**
 * Cursor implementation.
 */
public class SqlCursorImpl implements SqlCursor {
    /** Query state. */
    private final QueryState state;

    /** Iterator. */
    private Iterator<SqlRow> iterator;

    public SqlCursorImpl(QueryState state) {
        this.state = state;
    }

    @Override @Nonnull
    public Iterator<SqlRow> iterator() {
        if (iterator == null) {
            Iterator<SqlRow> iterator0 = getQueryInitiatorState().getRowSource().iterator();

            iterator = iterator0;

            return iterator0;
        } else {
            throw HazelcastSqlException.error("Iteartor can be requested only once.");
        }
    }

    @Override
    public void close() {
        state.cancel(HazelcastSqlException.error(SqlErrorCode.CANCELLED, "Query was cancelled by user."));
    }

    public QueryId getQueryId() {
        return getQueryInitiatorState().getQueryId();
    }

    public QueryPlan getPlan() {
        return getQueryInitiatorState().getPlan();
    }

    private QueryInitiatorState getQueryInitiatorState() {
        return state.getInitiatorState();
    }
}
