/*
 * Copyright 2021 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.sql;

import com.hazelcast.jet.SimpleTestInClusterSupport;
import com.hazelcast.jet.sql.impl.JetPlan;
import com.hazelcast.map.IMap;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;

import static com.hazelcast.jet.sql.impl.connector.TestJetSqlQueryUtils.planFromQuery;
import static java.util.Collections.emptyList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class SqlNullableColumnTest extends SimpleTestInClusterSupport {
    @BeforeClass
    public static void beforeClass() {
        initialize(1, smallInstanceConfig());
    }

    @Test
    public void testSelectWithNonNullSupport() {
        IMap<String, Integer> map = instance().getMap("map");
        map.put("key", 1);

        JetPlan jetPlan = planFromQuery(instance(), "SELECT __key, 1 FROM map", emptyList());
        assertNotNull(jetPlan);
        assertInstanceOf(JetPlan.SelectPlan.class, jetPlan);

        JetPlan.SelectPlan plan = (JetPlan.SelectPlan) jetPlan;
        List<SqlColumnMetadata> columns = plan.getRowMetadata().getColumns();
        assertEquals(columns.size(), 2);
        assertTrue(columns.get(0).isNullable());
        assertFalse(columns.get(1).isNullable());
    }
}
