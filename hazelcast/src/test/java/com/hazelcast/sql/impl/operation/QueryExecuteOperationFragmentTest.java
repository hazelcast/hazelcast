/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.sql.impl.operation;

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.sql.impl.SqlDataSerializerHook;
import com.hazelcast.sql.impl.physical.MockPhysicalNode;
import com.hazelcast.sql.impl.physical.PhysicalNode;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class QueryExecuteOperationFragmentTest {
    @Test
    public void testFragment() {
        PhysicalNode node = MockPhysicalNode.create(1, QueryDataType.INT);
        List<UUID> memberIds = Arrays.asList(UUID.randomUUID(), UUID.randomUUID());

        QueryExecuteOperationFragment fragment = new QueryExecuteOperationFragment(node, memberIds);

        assertEquals(node, fragment.getNode());
        assertEquals(memberIds, fragment.getMemberIds());
    }

    @Test
    public void testSerialization() {
        QueryExecuteOperationFragment original = new QueryExecuteOperationFragment(
            MockPhysicalNode.create(1, QueryDataType.INT),
            Arrays.asList(UUID.randomUUID(), UUID.randomUUID())
        );

        assertEquals(SqlDataSerializerHook.F_ID, original.getFactoryId());
        assertEquals(SqlDataSerializerHook.OPERATION_EXECUTE_FRAGMENT, original.getClassId());

        InternalSerializationService ss = new DefaultSerializationServiceBuilder().build();

        QueryExecuteOperationFragment restored = ss.toObject(ss.toData(original));

        assertEquals(original.getNode(), restored.getNode());
        assertEquals(original.getMemberIds(), restored.getMemberIds());
    }
}
