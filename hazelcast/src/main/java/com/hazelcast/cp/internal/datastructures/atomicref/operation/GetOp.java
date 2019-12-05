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

package com.hazelcast.cp.internal.datastructures.atomicref.operation;

import com.hazelcast.cp.IAtomicReference;
import com.hazelcast.cp.internal.datastructures.atomicref.AtomicRefDataSerializerHook;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.internal.IndeterminateOperationStateAware;

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
    public Object run(CPGroupId groupId, long commitIndex) {
        return getAtomicRef(groupId).get();
    }

    public boolean isRetryableOnIndeterminateOperationState() {
        return true;
    }

    @Override
    public int getClassId() {
        return AtomicRefDataSerializerHook.GET_OP;
    }
}
