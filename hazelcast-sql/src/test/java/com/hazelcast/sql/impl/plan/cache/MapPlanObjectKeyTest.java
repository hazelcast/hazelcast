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

import com.hazelcast.config.IndexType;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.query.impl.getters.Extractors;
import com.hazelcast.sql.impl.SqlTestSupport;
import com.hazelcast.sql.impl.extract.GenericQueryTargetDescriptor;
import com.hazelcast.sql.impl.extract.QueryPath;
import com.hazelcast.sql.impl.extract.QueryTarget;
import com.hazelcast.sql.impl.extract.QueryTargetDescriptor;
import com.hazelcast.sql.impl.schema.TableField;
import com.hazelcast.sql.impl.schema.map.MapTableField;
import com.hazelcast.sql.impl.schema.map.MapTableIndex;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import static java.util.Collections.emptyList;
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

        QueryTargetDescriptor keyDescriptor1 = GenericQueryTargetDescriptor.DEFAULT;
        QueryTargetDescriptor keyDescriptor2 = new TestTargetDescriptor();

        QueryTargetDescriptor valueDescriptor1 = GenericQueryTargetDescriptor.DEFAULT;
        QueryTargetDescriptor valueDescriptor2 = new TestTargetDescriptor();

        List<MapTableIndex> indexes1 = singletonList(new MapTableIndex("idx", IndexType.SORTED, 0, emptyList(), emptyList()));
        List<MapTableIndex> indexes2 = singletonList(new MapTableIndex("idx", IndexType.HASH, 0, emptyList(), emptyList()));

        int distributionFieldOrdinal1 = 1;
        int distributionFieldOrdinal2 = 2;

        boolean hd1 = false;
        boolean hd2 = true;

        PartitionedMapPlanObjectKey objectId = new PartitionedMapPlanObjectKey(schema1, name1, fields1, conflictingSchemas1, keyDescriptor1, valueDescriptor1, indexes1, distributionFieldOrdinal1, hd1);

        checkEquals(objectId, new PartitionedMapPlanObjectKey(schema1, name1, fields1, conflictingSchemas1, keyDescriptor1, valueDescriptor1, indexes1, distributionFieldOrdinal1, hd1), true);

        checkEquals(objectId, new PartitionedMapPlanObjectKey(schema2, name1, fields1, conflictingSchemas1, keyDescriptor1, valueDescriptor1, indexes1, distributionFieldOrdinal1, hd1), false);
        checkEquals(objectId, new PartitionedMapPlanObjectKey(schema1, name2, fields1, conflictingSchemas1, keyDescriptor1, valueDescriptor1, indexes1, distributionFieldOrdinal1, hd1), false);
        checkEquals(objectId, new PartitionedMapPlanObjectKey(schema1, name1, fields2, conflictingSchemas1, keyDescriptor1, valueDescriptor1, indexes1, distributionFieldOrdinal1, hd1), false);
        checkEquals(objectId, new PartitionedMapPlanObjectKey(schema1, name1, fields1, conflictingSchemas2, keyDescriptor1, valueDescriptor1, indexes1, distributionFieldOrdinal1, hd1), false);
        checkEquals(objectId, new PartitionedMapPlanObjectKey(schema1, name1, fields1, conflictingSchemas1, keyDescriptor2, valueDescriptor1, indexes1, distributionFieldOrdinal1, hd1), false);
        checkEquals(objectId, new PartitionedMapPlanObjectKey(schema1, name1, fields1, conflictingSchemas1, keyDescriptor1, valueDescriptor2, indexes1, distributionFieldOrdinal1, hd1), false);
        checkEquals(objectId, new PartitionedMapPlanObjectKey(schema1, name1, fields1, conflictingSchemas1, keyDescriptor1, valueDescriptor1, indexes2, distributionFieldOrdinal1, hd1), false);
        checkEquals(objectId, new PartitionedMapPlanObjectKey(schema1, name1, fields1, conflictingSchemas1, keyDescriptor1, valueDescriptor1, indexes1, distributionFieldOrdinal2, hd1), false);
        checkEquals(objectId, new PartitionedMapPlanObjectKey(schema1, name1, fields1, conflictingSchemas1, keyDescriptor1, valueDescriptor1, indexes1, distributionFieldOrdinal1, hd2), false);
    }

    @Test
    public void test_replicated() {
        String schema1 = "schema1";
        String schema2 = "schema2";

        String name1 = "map1";
        String name2 = "map2";

        List<TableField> fields1 = singletonList(new MapTableField("field1", QueryDataType.INT, true, QueryPath.KEY_PATH));
        List<TableField> fields2 = singletonList(new MapTableField("field2", QueryDataType.INT, true, QueryPath.KEY_PATH));

        Set<String> conflictingSchemas1 = Collections.singleton("schema1");
        Set<String> conflictingSchemas2 = Collections.singleton("schema2");

        QueryTargetDescriptor keyDescriptor1 = GenericQueryTargetDescriptor.DEFAULT;
        QueryTargetDescriptor keyDescriptor2 = new TestTargetDescriptor();

        QueryTargetDescriptor valueDescriptor1 = GenericQueryTargetDescriptor.DEFAULT;
        QueryTargetDescriptor valueDescriptor2 = new TestTargetDescriptor();

        ReplicatedMapPlanObjectKey objectId = new ReplicatedMapPlanObjectKey(schema1, name1, fields1, conflictingSchemas1, keyDescriptor1, valueDescriptor1);

        checkEquals(objectId, new ReplicatedMapPlanObjectKey(schema1, name1, fields1, conflictingSchemas1, keyDescriptor1, valueDescriptor1), true);

        checkEquals(objectId, new ReplicatedMapPlanObjectKey(schema2, name1, fields1, conflictingSchemas1, keyDescriptor1, valueDescriptor1), false);
        checkEquals(objectId, new ReplicatedMapPlanObjectKey(schema1, name2, fields1, conflictingSchemas1, keyDescriptor1, valueDescriptor1), false);
        checkEquals(objectId, new ReplicatedMapPlanObjectKey(schema1, name1, fields2, conflictingSchemas1, keyDescriptor1, valueDescriptor1), false);
        checkEquals(objectId, new ReplicatedMapPlanObjectKey(schema1, name1, fields1, conflictingSchemas2, keyDescriptor1, valueDescriptor1), false);
        checkEquals(objectId, new ReplicatedMapPlanObjectKey(schema1, name1, fields1, conflictingSchemas1, keyDescriptor2, valueDescriptor1), false);
        checkEquals(objectId, new ReplicatedMapPlanObjectKey(schema1, name1, fields1, conflictingSchemas1, keyDescriptor1, valueDescriptor2), false);
    }

    private static class TestTargetDescriptor implements QueryTargetDescriptor {
        @Override
        public QueryTarget create(InternalSerializationService serializationService, Extractors extractors, boolean isKey) {
            return null;
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            // No-op.
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            // No-op.
        }
    }
}
