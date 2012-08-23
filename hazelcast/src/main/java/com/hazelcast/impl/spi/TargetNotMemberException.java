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

public class TargetNotMemberException extends RetryableException {
    public TargetNotMemberException(Address target) {
        this(target, -1, null);
    }

    public TargetNotMemberException(Address target, int partitionId, String operationName) {
        this(target, partitionId, operationName, null);
    }

    public TargetNotMemberException(Address target, int partitionId,
                                    String operationName, String serviceName) {
        super("Not Member! target:" + target + ", partitionId: " + partitionId
                + ", operation: " + operationName + ", service: " + serviceName);
    }
}
