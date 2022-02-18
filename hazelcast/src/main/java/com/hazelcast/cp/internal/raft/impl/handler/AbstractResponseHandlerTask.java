/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cp.internal.raft.impl.handler;

import com.hazelcast.cp.internal.raft.impl.RaftEndpoint;
import com.hazelcast.cp.internal.raft.impl.RaftNodeImpl;
import com.hazelcast.cp.internal.raft.impl.task.RaftNodeStatusAwareTask;

/**
 * Base class for response handler tasks.
 * Subclasses must implement {@link #handleResponse()}.
 * <p>
 * If {@link #sender()} is not a known member, then response is ignored.
 */
public abstract class AbstractResponseHandlerTask extends RaftNodeStatusAwareTask {

    AbstractResponseHandlerTask(RaftNodeImpl raftNode) {
        super(raftNode);
    }

    @Override
    protected final void innerRun() {
        RaftEndpoint sender = sender();
        if (!raftNode.state().isKnownMember(sender)) {
            logger.warning("Won't run, since " + sender + " is unknown to us");
            return;
        }

        handleResponse();
    }

    protected abstract void handleResponse();

    protected abstract RaftEndpoint sender();

}
