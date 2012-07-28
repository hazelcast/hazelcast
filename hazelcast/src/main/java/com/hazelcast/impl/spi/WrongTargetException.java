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

public class WrongTargetException extends RuntimeException implements RetryableException {
    public WrongTargetException(Address thisAddress, Address target) {
        this(thisAddress, target, -1, null);
    }

    public WrongTargetException(Address thisAddress, Address target, int partitionId, String operationName) {
        this(thisAddress, target, partitionId, operationName, null);
    }

    public WrongTargetException(Address thisAddress, Address target, int partitionId,
                                String operationName, String serviceName) {
        super("WrongTarget! this:" + thisAddress + ", target:" + target
              + ", partitionId: " + partitionId + ", operation: " + operationName + ", service: " + serviceName);
    }

}
