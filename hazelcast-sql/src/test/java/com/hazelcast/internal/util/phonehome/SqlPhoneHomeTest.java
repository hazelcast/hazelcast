/*
 * Copyright 2023 Hazelcast Inc.
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

package com.hazelcast.internal.util.phonehome;

import com.hazelcast.instance.impl.Node;
import com.hazelcast.jet.sql.SqlTestSupport;
import com.hazelcast.sql.impl.InternalSqlService;
import com.hazelcast.sql.impl.SqlServiceImpl;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;

public class SqlPhoneHomeTest extends SqlTestSupport {
    private Node node;
    private PhoneHome phoneHome;

    @BeforeClass
    public static void beforeClass() throws Exception {
        initialize(1, regularInstanceConfig());
    }

    @Before
    public void before() {
        node = getNode(instance());
        phoneHome = new PhoneHome(node);
    }

    @Test
    public void testSqlQueriesSubmitted() {
        // given
        Map<String, String> parameters = phoneHome.phoneHome(true);
        assertEquals("0", parameters.get(PhoneHomeMetrics.SQL_QUERIES_SUBMITTED.getRequestParameterName()));

        InternalSqlService sqlService = node.getNodeEngine().getSqlService();
        assertInstanceOf(SqlServiceImpl.class, sqlService);

        // when
        // 'CREATE MAPPING' query.
        createMapping("map", int.class, int.class);
        parameters = phoneHome.phoneHome(true);

        // then
        assertEquals("1", parameters.get(PhoneHomeMetrics.SQL_QUERIES_SUBMITTED.getRequestParameterName()));
    }
}
