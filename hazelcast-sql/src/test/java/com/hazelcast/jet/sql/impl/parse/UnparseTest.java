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

package com.hazelcast.jet.sql.impl.parse;

import com.hazelcast.jet.sql.SqlTestSupport;
import com.hazelcast.jet.sql.impl.OptimizerContext;
import com.hazelcast.sql.impl.schema.SqlCatalog;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static java.util.Collections.emptyList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class UnparseTest extends SqlTestSupport {
    private OptimizerContext context;

    @BeforeClass
    public static void beforeClass() {
        initialize(1, smallInstanceConfig());
    }

    @Before
    public void before() {
        context = createContext();
    }

    @Test
    public void test_JSON_QUERY() {
        checkQuery("SELECT JSON_QUERY('[1,2,3]', '$' WITHOUT ARRAY WRAPPER NULL ON EMPTY NULL ON ERROR)");
        checkQuery("SELECT JSON_QUERY('[1,2,3]', '$' WITH UNCONDITIONAL ARRAY WRAPPER NULL ON EMPTY NULL ON ERROR)");
        checkQuery("SELECT JSON_QUERY('[1,2,3]', '$' WITH CONDITIONAL ARRAY WRAPPER NULL ON EMPTY NULL ON ERROR)");
        checkQuery("SELECT JSON_QUERY('[1,2,3]', '$' WITHOUT ARRAY WRAPPER ERROR ON EMPTY ERROR ON ERROR)");
        checkQuery("SELECT JSON_QUERY('[1,2,3]', '$' WITHOUT ARRAY WRAPPER EMPTY ARRAY ON EMPTY EMPTY OBJECT ON ERROR)");
    }

    @Test
    public void test_JSON_VALUE() {
        checkQuery("SELECT JSON_VALUE('[1,2,3]', '$[0]' RETURNING BIGINT)");
        checkQuery("SELECT JSON_VALUE('[1,2,3]', '$[0]' RETURNING BIGINT DEFAULT 1 ON EMPTY ERROR ON ERROR)");
        checkQuery("SELECT JSON_VALUE('[1,2,3]', '$[0]' RETURNING VARCHAR NULL ON EMPTY DEFAULT '1' ON ERROR)");
    }

    @Test
    public void test_JSON_OBJECT() {
        checkQuery("SELECT JSON_OBJECT()");
        checkQuery("SELECT JSON_OBJECT(KEY 'k1' VALUE 'v1' ABSENT ON NULL)");
        checkQuery("SELECT JSON_OBJECT(KEY 'k1' VALUE 'v1', KEY 'k2' VALUE 'v2' NULL ON NULL)");
    }

    @Test
    public void test_JSON_ARRAY() {
        checkQuery("SELECT JSON_ARRAY()");
        checkQuery("SELECT JSON_ARRAY(1, 'b', 3 ABSENT ON NULL)");
        checkQuery("SELECT JSON_ARRAY(1, 'b', 3 NULL ON NULL)");
    }

    private void checkQuery(String query) {
        final SqlNode node = context.parse(query).getNode();
        final SqlPrettyWriter writer = new SqlPrettyWriter(SqlPrettyWriter.config());
        node.unparse(writer, 0, 0);
        final String result = writer.toSqlString().toString();

        assertEquals(query, result);
        assertNotNull(context.parse(result).getNode());
    }

    private static OptimizerContext createContext() {
        return OptimizerContext.create(
                new SqlCatalog(emptyList()),
                emptyList(),
                emptyList(),
                1,
                name -> null
        );
    }
}
