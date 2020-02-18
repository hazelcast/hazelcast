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

import com.hazelcast.spi.impl.NodeEngine;

import java.util.List;

/**
 * Context of the running query.
 */
public class QueryFragmentContext {
    /** Current query context. */
    private static final ThreadLocal<QueryFragmentContext> CURRENT = new ThreadLocal<>();

    /** Shared query context. */
    private final QueryContext context;

    /** Root consumer. */
    private final QueryResultConsumer rootConsumer;

    /** Fragment executable. */
    private QueryFragmentExecutable fragmentExecutable;

    public QueryFragmentContext(
        QueryContext context,
        QueryResultConsumer rootConsumer
    ) {
        this.context = context;
        this.rootConsumer = rootConsumer;
    }

    public static QueryFragmentContext getCurrentContext() {
        return CURRENT.get();
    }

    public static void setCurrentContext(QueryFragmentContext context) {
        CURRENT.set(context);
    }

    public void setFragmentExecutable(QueryFragmentExecutable fragmentExecutable) {
        this.fragmentExecutable = fragmentExecutable;
    }

    public NodeEngine getNodeEngine() {
        return context.getNodeEngine();
    }

    public QueryId getQueryId() {
        return context.getQueryId();
    }

    public QueryResultConsumer getRootConsumer() {
        return rootConsumer;
    }

    public Object getArgument(int idx) {
        List<Object> arguments = context.getArguments();

        if (arguments == null || idx >= arguments.size()) {
            throw new IllegalArgumentException("Argument not found: " + idx);
        }

        return arguments.get(idx);
    }

    public void reschedule() {
        fragmentExecutable.schedule(context.getWorkerPool());
    }

    public void checkCancelled() {
        context.checkCancelled();
    }
}
