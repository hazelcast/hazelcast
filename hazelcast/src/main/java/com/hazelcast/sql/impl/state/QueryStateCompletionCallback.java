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

package com.hazelcast.sql.impl.state;

import com.hazelcast.sql.SqlErrorCode;
import com.hazelcast.sql.impl.SqlServiceImpl;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Callback invoked when the query is completed.
 */
public class QueryStateCompletionCallback {
    /** SQL service. */
    private final SqlServiceImpl service;

    /** State registry. */
    private final QueryStateRegistry stateRegistry;

    /** Guard to protect from multiple invocations. */
    private final AtomicBoolean guard = new AtomicBoolean();

    public QueryStateCompletionCallback(SqlServiceImpl service, QueryStateRegistry stateRegistry) {
        this.service = service;
        this.stateRegistry = stateRegistry;
    }

    public boolean onCompleted(
        QueryState state,
        int errorCode,
        String errorMessage,
        UUID originatingMemberId,
        boolean propagate
    ) {
        // Make sure that only one thread completes the qyery.
        if (!guard.compareAndSet(false, true)) {
            return false;
        }

        try {
            // If this is an error and it should be propagated, initiate distributed cancel.
            if (errorCode != SqlErrorCode.OK && propagate) {
                service.cancelQuery(state, new QueryCancelInfo(originatingMemberId, errorCode, errorMessage));
            }
        } finally {
            // Remove the query from the state registry.
            stateRegistry.onQueryFinished(state.getQueryId());
        }

        return true;
    }
}
