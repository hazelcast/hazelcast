package com.hazelcast.dataset;

import com.hazelcast.config.Config;
import com.hazelcast.config.DataSetConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.query.SqlPredicate;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;

import static com.hazelcast.spi.properties.GroupProperty.PARTITION_COUNT;
import static org.junit.Assert.assertEquals;

public class EntryProcessorTest extends HazelcastTestSupport {

    @Test
    public void testFieldMutator() {
        Config config = new Config()
                .setProperty(PARTITION_COUNT.getName(), "10")
                .addDataSetConfig(
                        new DataSetConfig("employees")
                                .setKeyClass(Long.class)
                                .setValueClass(Employee.class));

        HazelcastInstance[] cluster = createHazelcastInstanceFactory(2).newInstances(config);

        DataSet<Long, Employee> employees = cluster[0].getDataSet("employees");
        int initialAge = 20;
        for (int k = 0; k < 50; k++) {
            employees.insert((long) k, new Employee(20, k, k));
        }
        System.out.println("employees consumed memory:" + employees.memoryInfo().consumedBytes());

        PreparedEntryProcessor<AgeSalary> preparedEntryProcessor = employees.prepare(
                new EntryProcessorRecipe(new SqlPredicate("age=20"), new MultiplyMutator("age", 10)));

        preparedEntryProcessor.execute(new HashMap<>());

        PreparedQuery<Employee> preparedQuery = employees.prepare(new SqlPredicate("true"));
        List<Employee> employeeList = preparedQuery.execute(new HashMap<>());
        for (Employee employee : employeeList) {
            assertEquals(initialAge * 2, employee.age);
        }
    }

    @Test
    public void testRecordMutator() {
        Config config = new Config()
                .setProperty(PARTITION_COUNT.getName(), "10")
                .addDataSetConfig(
                        new DataSetConfig("employees")
                                .setKeyClass(Long.class)
                                .setValueClass(Employee.class));

        HazelcastInstance[] cluster = createHazelcastInstanceFactory(2).newInstances(config);

        DataSet<Long, Employee> employees = cluster[0].getDataSet("employees");
        int initialAge = 20;
        for (int k = 0; k < 50; k++) {
            employees.insert((long) k, new Employee(20, k, k));
        }

        PreparedEntryProcessor<AgeSalary> preparedEntryProcessor = employees.prepare(
                new EntryProcessorRecipe(new SqlPredicate("true"), new IncreaseAgeSalary()));

        preparedEntryProcessor.execute(new HashMap<>());

        PreparedQuery<Employee> preparedQuery = employees.prepare(new SqlPredicate("true"));
        List<Employee> employeeList = preparedQuery.execute(new HashMap<>());
        for (Employee employee : employeeList) {
            assertEquals(initialAge * 2, employee.age);
        }
    }

}
