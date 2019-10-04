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

package com.hazelcast.cp.internal.raft.impl.state;

import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.cp.internal.raft.impl.RaftEndpoint;
import com.hazelcast.internal.util.SimpleCompletableFuture;

/**
 * State maintained by the leader of the Raft group during the leadership
 * transfer process.
 */
public class LeadershipTransferState {

    private int term;
    private RaftEndpoint endpoint;
    private SimpleCompletableFuture resultFuture;

    LeadershipTransferState(int term, RaftEndpoint endpoint, SimpleCompletableFuture resultFuture) {
        this.term = term;
        this.endpoint = endpoint;
        this.resultFuture = resultFuture;
    }

    public int term() {
        return term;
    }

    public RaftEndpoint endpoint() {
        return endpoint;
    }

    public boolean complete(Object value) {
        return resultFuture.setResult(value);
    }

    public void notify(RaftEndpoint targetEndpoint, final SimpleCompletableFuture otherFuture) {
        if (this.endpoint.equals(targetEndpoint)) {
            resultFuture.andThen(new ExecutionCallback() {
                @Override
                public void onResponse(Object response) {
                    otherFuture.complete(response);
                }

                @Override
                public void onFailure(Throwable t) {
                    otherFuture.complete(t);
                }
            });
        } else {
            otherFuture.setResult(new IllegalStateException("There is an ongoing leadership transfer process to " + endpoint));
        }
    }
}
