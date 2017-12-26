package com.hazelcast.dataseries;

import com.hazelcast.config.Config;
import com.hazelcast.config.DataSeriesConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.query.SqlPredicate;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class QueryWithIndexTest extends HazelcastTestSupport {

    @Test
    public void compileQuery() {
        Config config = new Config()
                .setProperty(GroupProperty.PARTITION_COUNT.getName(), "10")
                .addDataSeriesConfig(
                        new DataSeriesConfig("employees")
                                .setKeyClass(Long.class)
                                .setValueClass(Employee.class)
                                .addIndexField("age"));

        HazelcastInstance hz = createHazelcastInstance(config);

        DataSeries<Long, Employee> dataSeries = hz.getDataSeries("employees");
        dataSeries.append(1L, new Employee(20, 100, 200));
        dataSeries.append(1L, new Employee(20, 101, 200));
        dataSeries.append(1L, new Employee(20, 103, 200));
        dataSeries.append(1L, new Employee(21, 100, 201));
        dataSeries.append(1L, new Employee(22, 100, 202));

        PreparedQuery<Employee> preparedQuery = dataSeries.prepare(new SqlPredicate("age==20"));
        Map<String, Object> bindings = new HashMap<String, Object>();
        bindings.put("age", 20);
        assertEquals(3, preparedQuery.execute(bindings).size());
    }
}
