/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.raft.service.atomiclong.operation;

import com.hazelcast.raft.QueryPolicy;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.impl.IndeterminateOperationStateAware;
import com.hazelcast.raft.service.atomiclong.RaftAtomicLongDataSerializerHook;
import com.hazelcast.raft.service.atomiclong.proxy.RaftAtomicLongProxy;

/**
 * Operation for {@link RaftAtomicLongProxy#localGet(QueryPolicy)}
 */
public class LocalGetOp extends AbstractAtomicLongOp implements IndeterminateOperationStateAware {

    public LocalGetOp() {
        super();
    }

    public LocalGetOp(String name) {
        super(name);
    }

    @Override
    public Object run(RaftGroupId groupId, long commitIndex) {
        return getAtomicLong(groupId).value();
    }

    @Override
    public boolean isRetryableOnIndeterminateOperationState() {
        return true;
    }

    @Override
    public int getId() {
        return RaftAtomicLongDataSerializerHook.LOCAL_GET_OP;
    }
}
