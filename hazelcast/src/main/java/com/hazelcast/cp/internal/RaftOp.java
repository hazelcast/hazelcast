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

package com.hazelcast.cp.internal;

import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.cp.CPGroupId;
import com.hazelcast.spi.impl.NodeEngine;

/**
 * Base operation class for operations to be replicated to and executed on
 * Raft group members.
 * <p>
 * {@code RaftOp} is stored in Raft log by leader and replicated to followers.
 * When at least majority of the members append it to their logs,
 * the log entry which it belongs is committed and {@code RaftOp} is executed
 * eventually on each member.
 * <p>
 * Note that, implementations of {@code RaftOp} must be deterministic.
 * They should perform the same action and produce the same result always,
 * independent of where and when they are executed.
 * <p>
 * {@link #run(CPGroupId, long)} method must be implemented by subclasses.
 */
public abstract class RaftOp implements DataSerializable {

    private transient NodeEngine nodeEngine;

    /**
     * Contains actual Raft operation logic. State change represented by
     * this operation should be applied and execution result should be
     * returned to the caller.
     *
     * @param groupId groupId of the specific Raft group
     * @param commitIndex commitIndex of the log entry keeping this operation
     * @return result of the operation execution
     */
    public abstract Object run(CPGroupId groupId, long commitIndex) throws Exception;

    public NodeEngine getNodeEngine() {
        return nodeEngine;
    }

    public RaftOp setNodeEngine(NodeEngine nodeEngine) {
        this.nodeEngine = nodeEngine;
        return this;
    }

    public <T> T getService() {
        return nodeEngine.getService(getServiceName());
    }

    protected ILogger getLogger() {
        return getNodeEngine().getLogger(getClass());
    }

    protected abstract String getServiceName();

    protected void toString(StringBuilder sb) {
    }

    @Override
    public final String toString() {
        StringBuilder sb = new StringBuilder(getClass().getName()).append('{');
        sb.append("serviceName='").append(getServiceName()).append('\'');
        toString(sb);
        sb.append('}');
        return sb.toString();
    }
}
