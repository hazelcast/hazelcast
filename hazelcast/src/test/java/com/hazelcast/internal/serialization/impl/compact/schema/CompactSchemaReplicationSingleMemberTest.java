/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.serialization.impl.compact.schema;

import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.instance.impl.TestUtil;
import com.hazelcast.internal.serialization.impl.compact.Schema;
import com.hazelcast.map.IMap;
import com.hazelcast.nio.serialization.genericrecord.GenericRecord;
import com.hazelcast.nio.serialization.genericrecord.GenericRecordBuilder;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collection;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class CompactSchemaReplicationSingleMemberTest extends HazelcastTestSupport {

    protected final TestHazelcastFactory factory = new TestHazelcastFactory();

    private HazelcastInstance driver;
    private HazelcastInstance member;

    @Before
    public void setup() {
        member = factory.newHazelcastInstance(getConfig());
        driver = getDriver();
    }

    @After
    public void teardown() {
        factory.terminateAll();
    }

    @Test
    public void testSchemaReplication() {
        int schemaCount = 10_000;
        IMap<Integer, GenericRecord> map = driver.getMap(randomMapName());
        for (int i = 0; i < schemaCount; i++) {
            map.put(i, createRecordWithUniqueSchema(i));
        }

        assertEquals(schemaCount, getReplicatedSchemaCount());
    }

    private GenericRecord createRecordWithUniqueSchema(int i) {
        return GenericRecordBuilder.compact(randomString())
                .setInt32(Integer.toString(i), i)
                .build();
    }

    protected HazelcastInstance getDriver() {
        return member;
    }

    protected int getReplicatedSchemaCount() {
        Node node = TestUtil.getNode(member);
        MemberSchemaService schemaService = node.getSchemaService();
        Collection<Schema> schemas = schemaService.getAllSchemas();
        for (Schema schema : schemas) {
            SchemaReplicationStatus status = schemaService.getReplicator().getReplicationStatus(schema);
            assertEquals(SchemaReplicationStatus.REPLICATED, status);
        }

        return schemas.size();
    }

    @Override
    protected Config getConfig() {
        return smallInstanceConfig();
    }
}
