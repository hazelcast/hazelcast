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

package com.hazelcast.sql.impl.plan.node;

import com.hazelcast.sql.impl.CoreSqlTestSupport;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class PlanNodeSchemaTest extends CoreSqlTestSupport {
    @Test
    public void testSingleSchema() {
        QueryDataType type0 = QueryDataType.VARCHAR;
        QueryDataType type1 = QueryDataType.DOUBLE;

        List<QueryDataType> types = Arrays.asList(type0, type1);

        PlanNodeSchema schema = new PlanNodeSchema(types);

        assertEquals(types, schema.getTypes());
        assertEquals(type0, schema.getType(0));
        assertEquals(type1, schema.getType(1));

        assertEquals(type0.getTypeFamily().getEstimatedSize() + type1.getTypeFamily().getEstimatedSize(), schema.getEstimatedRowSize());
    }

    @Test
    public void testCombinedSchemas() {
        QueryDataType type0 = QueryDataType.VARCHAR;
        QueryDataType type1 = QueryDataType.DOUBLE;

        PlanNodeSchema schema0 = new PlanNodeSchema(Collections.singletonList(type0));
        PlanNodeSchema schema1 = new PlanNodeSchema(Collections.singletonList(type1));

        PlanNodeSchema combinedSchema = PlanNodeSchema.combine(schema0, schema1);

        assertEquals(2, combinedSchema.getTypes().size());
        assertEquals(type0, combinedSchema.getType(0));
        assertEquals(type1, combinedSchema.getType(1));
    }

    @Test
    public void testEquals() {
        List<QueryDataType> types1 = Arrays.asList(QueryDataType.INT, QueryDataType.VARCHAR);
        List<QueryDataType> types2 = Arrays.asList(QueryDataType.DECIMAL, QueryDataType.TIMESTAMP_WITH_TZ_OFFSET_DATE_TIME);

        checkEquals(new PlanNodeSchema(types1), new PlanNodeSchema(types1), true);
        checkEquals(new PlanNodeSchema(types1), new PlanNodeSchema(types2), false);
    }
}
