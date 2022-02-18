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

package com.hazelcast.map.impl.nearcache.invalidation;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.nearcache.impl.invalidation.BatchNearCacheInvalidation;
import com.hazelcast.internal.nearcache.impl.invalidation.Invalidation;
import com.hazelcast.internal.nearcache.impl.invalidation.SingleNearCacheInvalidation;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static com.hazelcast.internal.nearcache.impl.NearCacheTestUtils.getBaseConfig;
import static com.hazelcast.test.Accessors.getSerializationService;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class InvalidationTest extends HazelcastTestSupport {

    private SerializationService serializationService;

    private SingleNearCacheInvalidation singleInvalidation;
    private BatchNearCacheInvalidation batchInvalidation;

    @Before
    public void setUp() {
        Config config = getBaseConfig();
        HazelcastInstance hz = createHazelcastInstance(config);
        serializationService = getSerializationService(hz);

        Data key = serializationService.toData("key");
        String mapName = "mapName";
        UUID sourceUuid = UUID.randomUUID();
        UUID partitionUuid = UUID.randomUUID();
        singleInvalidation = new SingleNearCacheInvalidation(key, mapName, sourceUuid, partitionUuid, 1);

        List<Invalidation> invalidations = Collections.singletonList(singleInvalidation);
        batchInvalidation = new BatchNearCacheInvalidation(mapName, invalidations);
    }

    @Test
    public void testSingleDeserialization() {
        Data serializedInvalidation = serializationService.toData(singleInvalidation);
        SingleNearCacheInvalidation deserializedInvalidation = serializationService.toObject(serializedInvalidation);

        assertInvalidation(singleInvalidation, deserializedInvalidation, true);
    }

    @Test
    public void testBatchDeserialization() {
        Data serializedInvalidation = serializationService.toData(batchInvalidation);
        BatchNearCacheInvalidation deserializedInvalidation = serializationService.toObject(serializedInvalidation);

        assertInvalidation(batchInvalidation, deserializedInvalidation, false);
        assertEquals(batchInvalidation.getInvalidations().size(), deserializedInvalidation.getInvalidations().size());
        for (Invalidation invalidation : deserializedInvalidation.getInvalidations()) {
            assertInvalidation(singleInvalidation, invalidation, true);
        }
    }

    private static void assertInvalidation(Invalidation expected, Invalidation actual, boolean hasKey) {
        if (hasKey) {
            assertEquals("Expected the same key", expected.getKey(), actual.getKey());
        }
        assertEquals("Expected the same data structure name", expected.getName(), actual.getName());
        assertEquals("Expected the same sourceUuid", expected.getSourceUuid(), actual.getSourceUuid());
        assertEquals("Expected the same partitionUuid", expected.getPartitionUuid(), actual.getPartitionUuid());
        assertEquals("Expected the same sequence", expected.getSequence(), actual.getSequence());
        assertEquals("Expected the same class", expected.getClass().getName(), actual.getClass().getName());
    }
}
