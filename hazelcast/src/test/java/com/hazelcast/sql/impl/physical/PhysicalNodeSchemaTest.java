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

package com.hazelcast.sql.impl.physical;

import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class PhysicalNodeSchemaTest {
    @Test
    public void testSingleSchema() {
        QueryDataType type0 = QueryDataType.VARCHAR;
        QueryDataType type1 = QueryDataType.DOUBLE;

        List<QueryDataType> types = new ArrayList<>(2);

        types.add(type0);
        types.add(type1);

        PhysicalNodeSchema schema = new PhysicalNodeSchema(types);

        assertEquals(types, schema.getTypes());
        assertEquals(type0, schema.getType(0));
        assertEquals(type1, schema.getType(1));

        assertEquals(type0.getTypeFamily().getEstimatedSize() + type1.getTypeFamily().getEstimatedSize(), schema.getRowWidth());
    }

    @Test
    public void testCombinedSchemas() {
        QueryDataType type0 = QueryDataType.VARCHAR;
        QueryDataType type1 = QueryDataType.DOUBLE;

        PhysicalNodeSchema schema0 = new PhysicalNodeSchema(Collections.singletonList(type0));
        PhysicalNodeSchema schema1 = new PhysicalNodeSchema(Collections.singletonList(type1));

        PhysicalNodeSchema combinedSchema = PhysicalNodeSchema.combine(schema0, schema1);

        assertEquals(2, combinedSchema.getTypes().size());
        assertEquals(type0, combinedSchema.getType(0));
        assertEquals(type1, combinedSchema.getType(1));
    }
}
