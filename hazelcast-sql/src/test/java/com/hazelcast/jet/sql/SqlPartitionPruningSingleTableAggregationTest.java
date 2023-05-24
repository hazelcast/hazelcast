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

package com.hazelcast.jet.sql;

import com.hazelcast.jet.sql.impl.connector.map.model.Person;
import com.hazelcast.map.IMap;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.List;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class SqlPartitionPruningSingleTableAggregationTest extends SqlTestSupport {
    private String mapName = "test_map"; //generateRandomString(16);

    @BeforeClass
    public static void beforeClass() {
        initialize(2, null);
    }

    @Before
    public void createTestTable() {
        createMapping(mapName, Person.class, Integer.class);
        instance().getSql().execute("select get_ddl('relation',  '" + mapName + "')")
                .forEach(r -> System.out.println(r.<Object>getObject(0)));

        IMap<Object, Object> map = instance().getMap(mapName);

        for (int i = 0; i < 1000; i++) {
            String key = "key" + i%3;
            map.put(new Person(i, key), i);
        }
    }

    @Test
    public void test_countNoFilter() {
        test_countPartitioned(null);
    }

    @Test
    public void test_countFilterKeyAttr() {
        test_countPartitioned("name='key0'");
        test_countPartitioned("id=10");
    }

    private void test_countPartitioned(String filter) {
        String filterText = filter != null ? " WHERE " + filter : "";

        //TODO: how is distinct different?

        // no grouping
        analyzeQuery("select count(*) from " + mapName + filterText, null); //rows(1, 1000L));
        // group by key attr
        analyzeQuery("select count(*), name from " + mapName + filterText + " group by name", null);
        // group by key attr function
        analyzeQuery("select count(*), name from " + mapName + filterText + " group by name", null);
        // group by key attr and value (same for attr?)
        analyzeQuery("select count(*), name, this from " + mapName + filterText + " group by name, this", null);
    }

    private void analyzeQuery(String sql, List<Row> rows) {
        System.out.println("Query:\n" + sql);
        if (rows != null) {
            assertRowsAnyOrder(sql, rows);
        } else {
//            instance().getSql().execute(sql).forEach(System.out::println);
            instance().getSql().execute(sql).close();
        }
    }
}
