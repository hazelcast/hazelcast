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

package com.hazelcast.map.impl.query;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.serialization.SerializationService;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.util.IterationType;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class QueryResultTest extends HazelcastTestSupport {

    private SerializationService serializationService;

    @Before
    public void setup() {
        HazelcastInstance hz = createHazelcastInstance();
        serializationService = getSerializationService(hz);
    }

    @Test
    public void serialization() {
        QueryResult expected = new QueryResult(IterationType.ENTRY, 100);
        QueryResultRow row = new QueryResultRow(serializationService.toData("1"), serializationService.toData("row"));
        expected.addRow(row);

        QueryResult actual = clone(expected);

        assertNotNull(actual);
        assertEquals(expected.getIterationType(), actual.getIterationType());
        assertEquals(1, actual.size());
        assertEquals(row, actual.iterator().next());
    }

    private QueryResult clone(QueryResult result) {
        Data data = serializationService.toData(result);
        return serializationService.toObject(data);
    }
}
