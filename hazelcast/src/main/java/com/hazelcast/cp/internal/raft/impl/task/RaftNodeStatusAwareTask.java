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

package com.hazelcast.cp.internal.raft.impl.task;

import com.hazelcast.logging.ILogger;
import com.hazelcast.cp.internal.raft.impl.RaftNodeImpl;

/**
 * Base class for tasks need to know current {@link RaftNodeImpl}.
 * If this RaftNode is terminated or stepped down, task will be skipped.
 * <p>
 * Subclasses must implement {@link #innerRun()} method.
 */
public abstract class RaftNodeStatusAwareTask implements Runnable {

    protected final RaftNodeImpl raftNode;
    protected final ILogger logger;

    protected RaftNodeStatusAwareTask(RaftNodeImpl raftNode) {
        this.raftNode = raftNode;
        this.logger = raftNode.getLogger(getClass());
    }

    @Override
    public final void run() {
        if (raftNode.isTerminatedOrSteppedDown()) {
            logger.fine("Won't run, since raft node is terminated");
            return;
        }

        try {
            innerRun();
        } catch (Throwable e) {
            logger.severe(e);
        }
    }

    protected abstract void innerRun();

}
