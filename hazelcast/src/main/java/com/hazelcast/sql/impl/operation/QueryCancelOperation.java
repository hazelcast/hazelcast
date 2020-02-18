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

package com.hazelcast.sql.impl.operation;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.sql.impl.QueryId;
import com.hazelcast.sql.impl.state.QueryCancelInfo;

import java.io.IOException;

/**
 * Operation to cancel query execution on participant node. Cancellation process is two-phase:
 * 1) When a participant would like to cancel the query, this request is sent to the query coordinator
 * 2) When coordinator receives cancel request from a participant (including self), it is broadcast to
 *    other participants.
 *
 * This two-phase process is required to minimize the number of messages passed between nodes. Consider a query cancel due to
 * timeout. If only broadcast is used from participants, then every participant will send a message to all other participants,
 * leading to N*N messages in the worst case. With two-phase approach query coordinator ensures that broadcast is performed no
 * more than once, putting upper bound on the total number of cancel messages to 2*N.
 */
public class QueryCancelOperation extends QueryIdAwareOperation {
    /** Cancel reason. */
    private QueryCancelInfo cancelInfo;

    public QueryCancelOperation() {
        // No-op.
    }

    public QueryCancelOperation(long epochWatermark, QueryId queryId, QueryCancelInfo cancelInfo) {
        super(epochWatermark, queryId);

        this.cancelInfo = cancelInfo;
    }

    public QueryCancelInfo getCancelInfo() {
        return cancelInfo;
    }

    @Override
    protected void writeInternal1(ObjectDataOutput out) throws IOException {
        cancelInfo.writeData(out);
    }

    @Override
    protected void readInternal1(ObjectDataInput in) throws IOException {
        cancelInfo = new QueryCancelInfo();

        cancelInfo.readData(in);
    }
}
