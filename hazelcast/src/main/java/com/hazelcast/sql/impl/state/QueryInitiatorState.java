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

import com.hazelcast.sql.impl.QueryId;
import com.hazelcast.sql.impl.QueryResultProducer;
import com.hazelcast.sql.impl.plan.Plan;

/**
 * Query state which is specific to the initiator node only.
 */
public class QueryInitiatorState {

    private final QueryId queryId;
    private final Plan plan;
    private final QueryResultProducer resultProducer;
    private final long timeout;

    public QueryInitiatorState(
        QueryId queryId,
        Plan plan,
        QueryResultProducer resultProducer,
        long timeout
    ) {
        this.queryId = queryId;
        this.plan = plan;
        this.resultProducer = resultProducer;
        this.timeout = timeout;
    }

    public QueryId getQueryId() {
        return queryId;
    }

    public Plan getPlan() {
        return plan;
    }

    public QueryResultProducer getResultProducer() {
        return resultProducer;
    }

    public long getTimeout() {
        return timeout;
    }
}
