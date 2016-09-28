/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class BatchNearCacheInvalidationTest extends HazelcastTestSupport {

    HazelcastInstance node = createHazelcastInstance();
    InternalSerializationService ss = getSerializationService(node);

    @Test
    public void equals_itself_after_deserialization() throws Exception {
        Data key = ss.toData("key");
        String mapName = "mapName";
        String sourceUuid = "sourceUuid";

        List<Invalidation> invalidations = new ArrayList<Invalidation>();
        invalidations.add(new SingleNearCacheInvalidation(key, mapName, sourceUuid));
        invalidations.add(new ClearNearCacheInvalidation(mapName, sourceUuid));

        BatchNearCacheInvalidation batch = new BatchNearCacheInvalidation(invalidations, mapName);


        Data data = ss.toData(batch);
        Object object = ss.toObject(data);

        assertInstanceOf(BatchNearCacheInvalidation.class, object);

        List<Invalidation> actualInvalidations = ((BatchNearCacheInvalidation) object).getInvalidations();
        assertDeserializedEqualsExpected(key, mapName, sourceUuid, actualInvalidations);
    }

    private void assertDeserializedEqualsExpected(Data key, String mapName, String sourceUuid, List<Invalidation> invalidations) {
        for (Invalidation invalidation : invalidations) {

            if (invalidation instanceof SingleNearCacheInvalidation) {
                assertEquals(key, ((SingleNearCacheInvalidation) invalidation).getKey());
            }

            assertEquals(mapName, invalidation.getName());
            assertEquals(sourceUuid, invalidation.getSourceUuid());
        }
    }

}