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

package com.hazelcast.serialization.compact.record;

import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.CompactSerializationConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.assertj.core.api.Assertions.assertThat;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public abstract class RecordSerializationIntegrationTest extends HazelcastTestSupport {

    protected final TestHazelcastFactory factory = new TestHazelcastFactory();

    @Before
    public abstract void setUp();

    @After
    public void tearDown() {
        factory.terminateAll();
    }

    public abstract HazelcastInstance getDriver();

    public abstract HazelcastInstance getDriverWithConfig(CompactSerializationConfig compactSerializationConfig);

    @Test
    public void shouldSerializeAndDeserializeRecord() {
        HazelcastInstance instance = getDriver();

        AllTypesRecord allTypesRecord = AllTypesRecord.create();
        IMap<Integer, AllTypesRecord> map = instance.getMap(randomMapName());

        map.put(1, allTypesRecord);

        AllTypesRecord loaded = map.get(1);
        assertThat(loaded).isEqualTo(allTypesRecord);
    }

    @Test
    public void shouldSerializeAndDeserializeRecordWhenRegisteredExplicitly() {
        CompactSerializationConfig compactSerializationConfig = new CompactSerializationConfig();
        compactSerializationConfig
                .setEnabled(true)
                .register(AllTypesRecord.class);

        HazelcastInstance instance = getDriverWithConfig(compactSerializationConfig);

        AllTypesRecord allTypesRecord = AllTypesRecord.create();
        IMap<Integer, AllTypesRecord> map = instance.getMap(randomMapName());

        map.put(1, allTypesRecord);

        AllTypesRecord loaded = map.get(1);
        assertThat(loaded).isEqualTo(allTypesRecord);
    }
}
