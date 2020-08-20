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

package com.hazelcast.sql.impl.plan.cache;

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.query.impl.getters.Extractors;
import com.hazelcast.sql.impl.SqlTestSupport;
import com.hazelcast.sql.impl.extract.GenericQueryTargetDescriptor;
import com.hazelcast.sql.impl.extract.QueryPath;
import com.hazelcast.sql.impl.extract.QueryTarget;
import com.hazelcast.sql.impl.extract.QueryTargetDescriptor;
import com.hazelcast.sql.impl.inject.JavaUpsertTargetDescriptor;
import com.hazelcast.sql.impl.inject.PortableUpsertTargetDescriptor;
import com.hazelcast.sql.impl.inject.UpsertTargetDescriptor;
import com.hazelcast.sql.impl.schema.TableField;
import com.hazelcast.sql.impl.schema.map.MapTableField;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collections;
import java.util.List;
import java.util.Set;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MapPlanObjectKeyTest extends SqlTestSupport {
    @Test
    public void test_partitioned() {
        String schema1 = "schema1";
        String schema2 = "schema2";

        String name1 = "map1";
        String name2 = "map2";

        List<TableField> fields1 = singletonList(new MapTableField("field1", QueryDataType.INT, true, QueryPath.KEY_PATH));
        List<TableField> fields2 = singletonList(new MapTableField("field2", QueryDataType.INT, true, QueryPath.KEY_PATH));

        Set<String> conflictingSchemas1 = Collections.singleton("schema1");
        Set<String> conflictingSchemas2 = Collections.singleton("schema2");

        QueryTargetDescriptor keyQueryDescriptor1 = GenericQueryTargetDescriptor.DEFAULT;
        QueryTargetDescriptor keyQueryDescriptor2 = new TestTargetDescriptor();
        UpsertTargetDescriptor keyUpsertDescriptor1 = new JavaUpsertTargetDescriptor(String.class.getName(), emptyMap());
        UpsertTargetDescriptor keyUpsertDescriptor2 = new PortableUpsertTargetDescriptor(1, 1, 1);

        QueryTargetDescriptor valueQueryDescriptor1 = GenericQueryTargetDescriptor.DEFAULT;
        QueryTargetDescriptor valueQueryDescriptor2 = new TestTargetDescriptor();
        UpsertTargetDescriptor valueUpsertDescriptor1 = new JavaUpsertTargetDescriptor(String.class.getName(), emptyMap());
        UpsertTargetDescriptor valueUpsertDescriptor2 = new PortableUpsertTargetDescriptor(1, 1, 1);

        PartitionedMapPlanObjectKey objectId = new PartitionedMapPlanObjectKey(schema1, name1, fields1, keyQueryDescriptor1, valueQueryDescriptor1, keyUpsertDescriptor1, valueUpsertDescriptor1, conflictingSchemas1);

        checkEquals(objectId, new PartitionedMapPlanObjectKey(schema1, name1, fields1, keyQueryDescriptor1, valueQueryDescriptor1, keyUpsertDescriptor1, valueUpsertDescriptor1, conflictingSchemas1), true);

        checkEquals(objectId, new PartitionedMapPlanObjectKey(schema2, name1, fields1, keyQueryDescriptor1, valueQueryDescriptor1, keyUpsertDescriptor1, valueUpsertDescriptor1, conflictingSchemas1), false);
        checkEquals(objectId, new PartitionedMapPlanObjectKey(schema1, name2, fields1, keyQueryDescriptor1, valueQueryDescriptor1, keyUpsertDescriptor1, valueUpsertDescriptor1, conflictingSchemas1), false);
        checkEquals(objectId, new PartitionedMapPlanObjectKey(schema1, name1, fields2, keyQueryDescriptor1, valueQueryDescriptor1, keyUpsertDescriptor1, valueUpsertDescriptor1, conflictingSchemas1), false);
        checkEquals(objectId, new PartitionedMapPlanObjectKey(schema1, name1, fields1, keyQueryDescriptor2, valueQueryDescriptor1, keyUpsertDescriptor1, valueUpsertDescriptor1, conflictingSchemas1), false);
        checkEquals(objectId, new PartitionedMapPlanObjectKey(schema1, name1, fields1, keyQueryDescriptor1, valueQueryDescriptor2, keyUpsertDescriptor1, valueUpsertDescriptor1, conflictingSchemas1), false);
        checkEquals(objectId, new PartitionedMapPlanObjectKey(schema1, name1, fields1, keyQueryDescriptor1, valueQueryDescriptor1, keyUpsertDescriptor2, valueUpsertDescriptor1, conflictingSchemas1), false);
        checkEquals(objectId, new PartitionedMapPlanObjectKey(schema1, name1, fields1, keyQueryDescriptor1, valueQueryDescriptor1, keyUpsertDescriptor1, valueUpsertDescriptor2, conflictingSchemas1), false);
        checkEquals(objectId, new PartitionedMapPlanObjectKey(schema1, name1, fields1, keyQueryDescriptor1, valueQueryDescriptor1, keyUpsertDescriptor1, valueUpsertDescriptor1, conflictingSchemas2), false);
    }

    private static class TestTargetDescriptor implements QueryTargetDescriptor {
        @Override
        public QueryTarget create(InternalSerializationService serializationService, Extractors extractors, boolean isKey) {
            return null;
        }

        @Override
        public void writeData(ObjectDataOutput out) {
            // No-op.
        }

        @Override
        public void readData(ObjectDataInput in) {
            // No-op.
        }
    }
}
