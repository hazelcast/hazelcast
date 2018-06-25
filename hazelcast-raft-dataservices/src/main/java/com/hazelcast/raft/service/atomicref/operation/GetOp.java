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

package com.hazelcast.raft.service.atomicref.operation;

import com.hazelcast.core.IAtomicReference;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.impl.IndeterminateOperationStateAware;
import com.hazelcast.raft.service.atomicref.RaftAtomicReferenceDataSerializerHook;

/**
 * Operation for {@link IAtomicReference#get()}
 */
public class GetOp extends AbstractAtomicRefOp implements IndeterminateOperationStateAware, IdentifiedDataSerializable {

    public GetOp() {
    }

    public GetOp(String name) {
        super(name);
    }

    @Override
    public Object run(RaftGroupId groupId, long commitIndex) {
        return getAtomicRef(groupId).get();
    }

    public boolean isRetryableOnIndeterminateOperationState() {
        return true;
    }

    @Override
    public int getId() {
        return RaftAtomicReferenceDataSerializerHook.GET_OP;
    }
}
