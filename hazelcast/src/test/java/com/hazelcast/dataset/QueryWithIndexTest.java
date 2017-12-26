package com.hazelcast.dataset;

import com.hazelcast.config.Config;
import com.hazelcast.config.DataSetConfig;
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
                .addDataSetConfig(
                        new DataSetConfig("employees")
                                .setKeyClass(Long.class)
                                .setValueClass(Employee.class)
                                .addIndexField("age"));

        HazelcastInstance hz = createHazelcastInstance(config);

        DataSet<Long, Employee> dataSet = hz.getDataSet("employees");
        dataSet.insert(1L, new Employee(20, 100, 200));
        dataSet.insert(1L, new Employee(20, 101, 200));
        dataSet.insert(1L, new Employee(20, 103, 200));
        dataSet.insert(1L, new Employee(21, 100, 201));
        dataSet.insert(1L, new Employee(22, 100, 202));

        PreparedQuery<Employee> preparedQuery = dataSet.prepare(new SqlPredicate("age==20"));
        Map<String, Object> bindings = new HashMap<String, Object>();
        bindings.put("age", 20);
        assertEquals(3, preparedQuery.execute(bindings).size());
    }
}
