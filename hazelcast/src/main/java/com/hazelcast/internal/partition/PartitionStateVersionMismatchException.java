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

package com.hazelcast.internal.partition;

import com.hazelcast.core.HazelcastException;

/**
 * Thrown when local partition stamp doesn't match the stamp
 * of master member while running a migration/replication operation.
 */
 //RU_COMPAT_4_0:
 // PartitionStateVersionMismatchException name is kept to provide rolling upgrade
 // guarantees. Otherwise this class would be renamed to PartitionStateStampMismatchException.
public class PartitionStateVersionMismatchException extends HazelcastException {

    public PartitionStateVersionMismatchException(String message) {
        super(message);
    }

    public PartitionStateVersionMismatchException(int masterStamp, int localStamp) {
        this("Local partition stamp is not equal to master's stamp!" + " Local: " + localStamp + ", Master: " + masterStamp);
    }
}
