/*
 * Copyright (c) 2008-2012, Hazel Bilisim Ltd. All Rights Reserved.
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

package com.hazelcast.impl.spi;

import com.hazelcast.nio.Address;

class SingleTargetInvocation extends SinglePartitionInvocation {
    private final Address target;

    SingleTargetInvocation(NodeService nodeService, String serviceName, Operation op, Address target, int tryCount, long tryPauseMillis) {
        super(nodeService, serviceName, op, null, 0, tryCount, tryPauseMillis);
        this.target = target;
    }

    @Override
    Address getTarget() {
        return target;
    }

    @Override
    public int getPartitionId() {
        return -1;
    }
}
