/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.sql.impl.worker.task;

import com.hazelcast.sql.impl.QueryFragmentDescriptor;
import com.hazelcast.sql.impl.QueryResultConsumer;
import com.hazelcast.sql.impl.operation.QueryExecuteOperation;

/**
 * A task to start fragment execution.
 */
public class StartFragmentQueryTask implements QueryTask {
    /** Operation. */
    private final QueryExecuteOperation operation;

    /** Fragment descriptor. */
    private final QueryFragmentDescriptor fragment;

    /** Query result consumer. */
    private final QueryResultConsumer consumer;

    public StartFragmentQueryTask(
        QueryExecuteOperation operation,
        QueryFragmentDescriptor fragment,
        QueryResultConsumer consumer
    ) {
        this.operation = operation;
        this.fragment = fragment;
        this.consumer = consumer;
    }

    public QueryExecuteOperation getOperation() {
        return operation;
    }

    public QueryFragmentDescriptor getFragment() {
        return fragment;
    }

    public QueryResultConsumer getConsumer() {
        return consumer;
    }
}
