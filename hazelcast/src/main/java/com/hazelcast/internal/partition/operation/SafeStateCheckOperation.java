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

package com.hazelcast.internal.partition.operation;

import com.hazelcast.internal.partition.InternalPartitionService;
import com.hazelcast.internal.partition.impl.PartitionDataSerializerHook;
import com.hazelcast.partition.PartitionService;
import com.hazelcast.spi.impl.AllowedDuringPassiveState;

/**
 * Checks whether a node is safe or not.
 * Safe means, first backups of partitions those owned by local member are sync with primary.
 *
 * @see PartitionService#isClusterSafe
 * @see PartitionService#isMemberSafe
 */
public class SafeStateCheckOperation extends AbstractPartitionOperation implements AllowedDuringPassiveState {

    private transient boolean safe;

    @Override
    public void run() throws Exception {
        final InternalPartitionService service = getService();
        safe = service.isMemberStateSafe();
    }

    @Override
    public Object getResponse() {
        return safe;
    }

    @Override
    public int getClassId() {
        return PartitionDataSerializerHook.SAFE_STATE_CHECK;
    }
}
