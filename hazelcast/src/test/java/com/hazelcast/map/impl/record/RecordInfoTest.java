/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.SerializationServiceBuilder;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.serialization.SerializationService;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.map.impl.record.Record.NOT_AVAILABLE;
import static java.lang.System.currentTimeMillis;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class RecordInfoTest extends HazelcastTestSupport {

    private SerializationService ss;

    @Before
    public void setUp() throws Exception {
        SerializationServiceBuilder builder = new DefaultSerializationServiceBuilder();
        ss = builder.setVersion(InternalSerializationService.VERSION_1).build();
    }

    @Test
    public void storeTime_and_expirationTime_is_not_available_on_deserialized_when_not_set() throws Exception {
        set_storeTime_and_expirationTime(NOT_AVAILABLE);
    }

    @Test
    public void storeTime_and_expirationTime_is_available_on_deserialized_when_set() throws Exception {
        set_storeTime_and_expirationTime(currentTimeMillis());
    }

    private void set_storeTime_and_expirationTime(long time) {
        RecordInfo recordInfo = new RecordInfo();

        recordInfo.setLastStoredTime(time);
        recordInfo.setExpirationTime(time);

        Data data = ss.toData(recordInfo);
        RecordInfo deserializedRecordInfo = ss.toObject(data);

        assertEquals(time, deserializedRecordInfo.getLastStoredTime());
        assertEquals(time, deserializedRecordInfo.getExpirationTime());
    }
}
