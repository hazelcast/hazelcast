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

import com.hazelcast.config.MapConfig;
import com.hazelcast.config.PartitioningAttributeConfig;
import com.hazelcast.jet.sql.impl.connector.map.model.Order;
import com.hazelcast.jet.sql.impl.connector.map.model.OrderKey;
import com.hazelcast.map.IMap;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.annotation.Nonnull;
import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class SqlPartitionPruningSingleTableTest extends SqlTestSupport {
    private String mapName = "orders";

    @BeforeClass
    public static void beforeClass() {
        initialize(5, null);
    }

    @Before
    public void createTestTable() {
        MapConfig mapConfig = instance().getConfig().getMapConfig(mapName);
        mapConfig.getPartitioningAttributeConfigs()
                .add(new PartitioningAttributeConfig("customerId"));
        instance().getConfig().addMapConfig(mapConfig);

        createMapping(mapName, OrderKey.class, Order.class);

        instance().getSql().execute("select get_ddl('relation',  '" + mapName + "')")
                .forEach(r -> System.out.println(r.<Object>getObject(0)));

        IMap<Object, Object> map = instance().getMap(mapName);

        final String[] countries = new String[]{"PL", "UA", "UK", "US"};
        int orderId = 1000;
        for (int custId = 1; custId < 10; ++custId) {
            // create skewed data
            for (int i = 0; i < 3 * custId; i++) {
                OrderKey key = new OrderKey("C" + custId, orderId++, countries[custId % countries.length]);
                Order data = new Order();
                data.setAmount(BigDecimal.valueOf(orderId + 5));
                data.setOrderDate(LocalDateTime.now().minusYears(1).plusHours(orderId));
                data.setPriority(orderId % 7 == 0 ? Order.Priority.URGENT : Order.Priority.NORMAL);
                data.setDeliveryDate(orderId % 2 == 0 ? null : data.getOrderDate().plusDays(4));
                map.put(key, data);
            }
        }
    }

    @Test
    public void test_basicSelect() {
        analyzeQuery("SELECT * FROM " + mapName + " WHERE customerId='C2'", null);
    }

    @Test
    public void test_selectFilterKeyPartitionAttr() {
        test_selectPartitioned("customerId='C2'", false, c2Amounts());
    }

    @Test
    public void test_selectDistinctFilterKeyPartitionAttr() {
        test_selectPartitioned("customerId='C2'", true, c2Amounts());
    }

    @Nonnull
    private static List<Row> c2Amounts() {
        return rows(1,
                BigDecimal.valueOf(1009),
                BigDecimal.valueOf(1010),
                BigDecimal.valueOf(1011),
                BigDecimal.valueOf(1012),
                BigDecimal.valueOf(1013),
                BigDecimal.valueOf(1014)
        );
    }

    @Test
    public void test_countNoFilter() {
        test_countPartitioned(null);
    }

    @Test
    public void test_countFilterKeyPartitionAttr() {
        test_countPartitioned("customerId='C2'");
    }

    @Test
    public void test_countFilterKeyNonPartitionAttr() {
        test_countPartitioned("country='PL'");
    }

    @Test
    public void test_countFilterKeyPartitionAndNonPartitionAttr() {
        // additional predicates should not confuse partition pruning - can be executed as residual filters
        test_countPartitioned("customerId='C2' and country like 'U%'");
    }

    @Test
    public void test_countFilterKeyPartitionAnValueAttr() {
        // additional predicates should not confuse partition pruning - can be executed as residual filters
        test_countPartitioned("customerId='C2' and cast(priority as varchar) = 'NORMAL'");
    }

    @Test
    public void test_countMultiplePartitions() {
        // this needs SEARCH operator support or converting to union?
        test_countPartitioned("customerId in ('C2', 'C3', 'C4')");
    }

    private void test_selectPartitioned(String filter, boolean distinct, List<Row> rows) {
        String filterText = filter != null ? " WHERE " + filter : "";

        //TODO: how is distinct different?
        //TODO: order by after aggregation
        //TODO: test query paramerters

        // order_amount is derived from orderId - can be used to check which records were returned
        analyzeQuery("select " + (distinct ? " distinct" : "") + " amount from " + mapName + filterText, rows);
    }

    private void test_countPartitioned(String filter) {
        String filterText = filter != null ? " WHERE " + filter : "";

        //TODO: how is distinct different?
        //TODO: order by after aggregation
        //TODO: test query paramerters

        // no grouping
        analyzeQuery("select count(*), sum(amount) from " + mapName + filterText, null);
        // group by key attr
        analyzeQuery("select count(*), sum(amount), customerId from " + mapName + filterText + " group by customerId", null);
        // group by key attr function
        analyzeQuery("select count(*), sum(amount), lower(customerId) from " + mapName + filterText + " group by lower(customerId)", null);
        // group by key attr and value (same for attr?)
        analyzeQuery("select count(*), sum(amount), customerId, priority from " + mapName + filterText + " group by customerId, priority", null);
        // group by value attr
        analyzeQuery("select count(*), sum(amount), priority from " + mapName + filterText + " group by priority", null);
    }

    private void analyzeQuery(String sql, List<Row> rows) {
        System.out.println("Query:\n" + sql);
        if (rows != null) {
            assertRowsAnyOrder(sql, rows);
        } else {
//            instance().getSql().execute(sql).forEach(System.out::println);
            try (SqlResult result = instance().getSql().execute(sql)) {
                result.stream()
                        .limit(10)
                        .forEach(System.out::println);
            }
        }
    }
}
