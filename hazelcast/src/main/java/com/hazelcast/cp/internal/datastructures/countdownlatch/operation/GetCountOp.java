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

package com.hazelcast.cp.internal.datastructures.countdownlatch.operation;

import com.hazelcast.cp.ICountDownLatch;
import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.internal.IndeterminateOperationStateAware;
import com.hazelcast.cp.internal.datastructures.countdownlatch.CountDownLatchDataSerializerHook;
import com.hazelcast.cp.internal.datastructures.countdownlatch.CountDownLatchService;

/**
 * Operation for {@link ICountDownLatch#getCount()}
 */
public class GetCountOp extends AbstractCountDownLatchOp implements IndeterminateOperationStateAware {

    public GetCountOp() {
    }

    public GetCountOp(String name) {
        super(name);
    }

    @Override
    public Object run(CPGroupId groupId, long commitIndex) {
        CountDownLatchService service = getService();
        return service.getRemainingCount(groupId, name);
    }

    @Override
    public boolean isRetryableOnIndeterminateOperationState() {
        return true;
    }

    @Override
    public int getClassId() {
        return CountDownLatchDataSerializerHook.GET_COUNT_OP;
    }
}
