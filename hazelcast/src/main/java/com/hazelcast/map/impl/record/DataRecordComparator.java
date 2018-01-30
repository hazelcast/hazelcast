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

package com.hazelcast.map.impl.record;

import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.serialization.SerializationService;

public class DataRecordComparator implements RecordComparator {

    private final SerializationService serializationService;

    public DataRecordComparator(SerializationService serializationService) {
        this.serializationService = serializationService;
    }

    @Override
    public boolean isEqual(Object value1, Object value2) {
        if (value1 == value2) {
            return true;
        }
        if (value1 == null || value2 == null) {
            return false;
        }
        // the PartitioningStrategy is not needed here, since `Data.equals()` only checks the payload, not the partitionHash
        Data data1 = serializationService.toData(value1);
        Data data2 = serializationService.toData(value2);
        return data1.equals(data2);
    }
}
