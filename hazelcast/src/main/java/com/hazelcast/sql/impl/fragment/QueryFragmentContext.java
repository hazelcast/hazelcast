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

package com.hazelcast.sql.impl.fragment;

import com.hazelcast.sql.impl.QueryId;
import com.hazelcast.sql.impl.exec.root.RootResultConsumer;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
import com.hazelcast.sql.impl.state.QueryState;
import com.hazelcast.sql.impl.worker.QueryFragmentExecutable;
import com.hazelcast.sql.impl.worker.QueryFragmentWorkerPool;

import java.util.List;

/**
 * Context of a running query fragment.
 */
public final class QueryFragmentContext implements ExpressionEvalContext {

    private final QueryState state;
    private final List<Object> arguments;
    private final QueryFragmentWorkerPool fragmentPool;
    private final RootResultConsumer rootConsumer;

    private QueryFragmentExecutable fragmentExecutable;

    public QueryFragmentContext(
        QueryState state,
        List<Object> arguments,
        QueryFragmentWorkerPool fragmentPool,
        RootResultConsumer rootConsumer
    ) {
        assert arguments != null;

        this.state = state;
        this.arguments = arguments;
        this.fragmentPool = fragmentPool;
        this.rootConsumer = rootConsumer;
    }

    public void setFragmentExecutable(QueryFragmentExecutable fragmentExecutable) {
        this.fragmentExecutable = fragmentExecutable;
    }

    public QueryId getQueryId() {
        return state.getQueryId();
    }

    public RootResultConsumer getRootConsumer() {
        return rootConsumer;
    }

    @Override
    public List<Object> getArguments() {
        return arguments;
    }

    public void reschedule() {
        fragmentExecutable.schedule(fragmentPool);
    }

    public void checkCancelled() {
        state.checkCancelled();
    }
}
