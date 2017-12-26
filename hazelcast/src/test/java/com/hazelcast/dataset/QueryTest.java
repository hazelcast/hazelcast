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

public class QueryTest extends HazelcastTestSupport {

    @Test
    public void compileQuery() {
        Config config = new Config();
        config.setProperty(GroupProperty.PARTITION_COUNT.getName(), "10");
        config.addDataSetConfig(new DataSetConfig("employees").setKeyClass(Long.class).setValueClass(Employee.class));

        HazelcastInstance[] cluster = createHazelcastInstanceFactory(2).newInstances(config);

        DataSet<Long, Employee> dataSet = cluster[0].getDataSet("employees");
        dataSet.insert(1L, new Employee(20, 100, 200));
        dataSet.insert(1L, new Employee(21, 101, 200));
        dataSet.insert(1L, new Employee(22, 103, 200));
        dataSet.insert(1L, new Employee(23, 100, 201));
        dataSet.insert(1L, new Employee(24, 100, 202));
        dataSet.insert(1L, new Employee(20, 100, 204));

        CompiledPredicate<Employee> compiledPredicate = dataSet.compile(new SqlPredicate("age==$age or iq=20 or salary=50"));
        Map<String, Object> bindings = new HashMap<String, Object>();
        bindings.put("age", 20);
        assertEquals(2, compiledPredicate.execute(bindings).size());
    }

    @Test
    public void noResults() {
        Config config = new Config();
        config.setProperty(GroupProperty.PARTITION_COUNT.getName(), "10");
        config.addDataSetConfig(new DataSetConfig("employees").setKeyClass(Long.class).setValueClass(Employee.class));

        HazelcastInstance[] cluster = createHazelcastInstanceFactory(2).newInstances(config);

        DataSet<Long, Employee> dataSet = cluster[0].getDataSet("employees");
        dataSet.insert(1L, new Employee(20, 100, 200));
        dataSet.insert(1L, new Employee(21, 101, 200));
        dataSet.insert(1L, new Employee(22, 103, 200));
        dataSet.insert(1L, new Employee(23, 100, 201));
        dataSet.insert(1L, new Employee(24, 100, 202));
        dataSet.insert(1L, new Employee(20, 100, 204));

        CompiledPredicate<Employee> compiledPredicate = dataSet.compile(new SqlPredicate("age==$age"));
        Map<String, Object> bindings = new HashMap<String, Object>();
        bindings.put("age", 2000);
        assertEquals(0, compiledPredicate.execute(bindings).size());
    }

    @Test
    public void compileQueryAll() {
        Config config = new Config();
        config.setProperty(GroupProperty.PARTITION_COUNT.getName(), "10");
        config.addDataSetConfig(new DataSetConfig("employees").setKeyClass(Long.class).setValueClass(Employee.class));

        HazelcastInstance[] cluster = createHazelcastInstanceFactory(2).newInstances(config);

        DataSet<Long, Employee> dataSet = cluster[0].getDataSet("employees");
        dataSet.insert(1L, new Employee(20, 100, 200));
        dataSet.insert(1L, new Employee(21, 101, 200));
        dataSet.insert(1L, new Employee(22, 103, 200));
        dataSet.insert(1L, new Employee(23, 100, 201));
        dataSet.insert(1L, new Employee(24, 100, 202));
        dataSet.insert(1L, new Employee(20, 100, 204));

        CompiledPredicate<Employee> compiledPredicate = dataSet.compile(new SqlPredicate("true"));
        Map<String, Object> bindings = new HashMap<String, Object>();
        assertEquals(6, compiledPredicate.execute(bindings).size());
    }

    @Test
    public void queryWithIndex_andNoBindParameter() {
        Config config = new Config();
        config.setProperty(GroupProperty.PARTITION_COUNT.getName(), "1");
        config.addDataSetConfig(new DataSetConfig("employees")
                .setKeyClass(Long.class)
                .setValueClass(Employee.class)
                .addIndexField("age"));

        HazelcastInstance[] cluster = createHazelcastInstanceFactory(2).newInstances(config);

        DataSet<Long, Employee> dataSet = cluster[0].getDataSet("employees");
        dataSet.insert(1L, new Employee(20, 100, 200));

        CompiledPredicate<Employee> compiledPredicate = dataSet.compile(new SqlPredicate("age==10"));
        Map<String, Object> bindings = new HashMap<String, Object>();
        assertEquals(0, compiledPredicate.execute(bindings).size());
    }

    @Test
    public void queryWithIndex_andBindParameter() {
        Config config = new Config();
        config.setProperty(GroupProperty.PARTITION_COUNT.getName(), "1");
        config.addDataSetConfig(new DataSetConfig("employees")
                .setKeyClass(Long.class)
                .setValueClass(Employee.class)
                .addIndexField("age"));

        HazelcastInstance[] cluster = createHazelcastInstanceFactory(2).newInstances(config);

        DataSet<Long, Employee> dataSet = cluster[0].getDataSet("employees");
        dataSet.insert(1L, new Employee(20, 100, 200));

        CompiledPredicate<Employee> compiledPredicate = dataSet.compile(new SqlPredicate("age==$age"));
        Map<String, Object> bindings = new HashMap<String, Object>();
        bindings.put("age", 50);
        assertEquals(0, compiledPredicate.execute(bindings).size());
    }
}
