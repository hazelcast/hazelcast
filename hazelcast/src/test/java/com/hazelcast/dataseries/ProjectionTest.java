package com.hazelcast.dataseries;

import com.hazelcast.config.Config;
import com.hazelcast.config.DataSeriesConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.query.SqlPredicate;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static com.hazelcast.nio.Bits.INT_SIZE_IN_BYTES;
import static com.hazelcast.spi.properties.GroupProperty.PARTITION_COUNT;
import static org.junit.Assert.assertEquals;

public class ProjectionTest extends HazelcastTestSupport {

    @Test
    public void compileIdentityProjection() {
        Config config = new Config()
                .setProperty(PARTITION_COUNT.getName(), "1")
                .addDataSeriesConfig(
                        new DataSeriesConfig("employees")
                                .setKeyClass(Long.class)
                                .setValueClass(Employee.class));

        HazelcastInstance[] cluster = createHazelcastInstanceFactory(2).newInstances(config);

        DataSeries<Long, Employee> dataSeries = cluster[0].getDataSeries("employees");
        dataSeries.append((long) 0, new Employee(19, 10, 100));
        dataSeries.append((long) 1, new Employee(20, 20, 200));
        dataSeries.append((long) 2, new Employee(21, 30, 300));
        dataSeries.append((long) 3, new Employee(22, 40, 400));
        dataSeries.append((long) 3, new Employee(23, 50, 500));


        PreparedProjection<Long, Employee> compiledPredicate = dataSeries.prepare(
                new ProjectionRecipe<Employee>(Employee.class, false, new SqlPredicate("age<=$age")));
        Map<String, Object> bindings = new HashMap<String, Object>();
        bindings.put("age", 20);

        Set<Employee> result = compiledPredicate.executePartitionThread(bindings, HashSet.class);
        System.out.println(result);
        assertEquals(2, result.size());
    }

    @Test
    public void compileProjectionAgeSalary() {
        Config config = new Config()
                .setProperty(PARTITION_COUNT.getName(), "10")
                .addDataSeriesConfig(
                        new DataSeriesConfig("employees")
                                .setKeyClass(Long.class)
                                .setValueClass(Employee.class));

        HazelcastInstance[] cluster = createHazelcastInstanceFactory(2).newInstances(config);

        DataSeries<Long, Employee> dataSeries = cluster[0].getDataSeries("employees");
        for (int k = 0; k < 1000; k++) {
            dataSeries.append((long) k, new Employee(k, k, k));
        }

        PreparedProjection<Long, AgeSalary> preparedProjection = dataSeries.prepare(
                new ProjectionRecipe<AgeSalary>(AgeSalary.class, true, new SqlPredicate("age==$age and iq==$iq and height>10")));
//        Map<String, Object> bindings = new HashMap<String, Object>();
//        bindings.put("age", 100);
//        bindings.put("iq", 100l);
//        compiledPredicate.execute(bindings);
    }

    @Test
    public void newDataSeries() {
        Config config = new Config()
                .setProperty(PARTITION_COUNT.getName(), "10")
                .addDataSeriesConfig(
                        new DataSeriesConfig("employees")
                                .setKeyClass(Long.class)
                                .setValueClass(Employee.class));

        HazelcastInstance[] cluster = createHazelcastInstanceFactory(2).newInstances(config);

        DataSeries<Long, Employee> employees = cluster[0].getDataSeries("employees");
        for (int k = 0; k < 100; k++) {
            employees.append((long) k, new Employee(k, k, k));
        }
        System.out.println("employees consumed memory:" + employees.memoryInfo().consumedBytes());

        PreparedProjection<Long, AgeSalary> preparedProjection = employees.prepare(
                new ProjectionRecipe<AgeSalary>(AgeSalary.class, true, new SqlPredicate("age<$age")));
        Map<String, Object> bindings = new HashMap<String, Object>();
        bindings.put("age", 50);

        DataSeries ageSalaries = preparedProjection.newDataSeries("ageSalary", bindings);
        assertEquals(employees.count() / 2, ageSalaries.count());
        System.out.println("agesalary memory:" + ageSalaries.memoryInfo().consumedBytes());
    }

    @Test
    public void newDataSetFromLargeInitialSet() {
        Config config = new Config()
                .setProperty(PARTITION_COUNT.getName(), "1")
                .addDataSeriesConfig(
                        new DataSeriesConfig("employees")
                                .setSegmentsPerPartition(Integer.MAX_VALUE)
                                .setKeyClass(Long.class)
                                .setValueClass(Employee.class));

        HazelcastInstance hz = createHazelcastInstance(config);

        DataSeries<Long, Employee> employees = hz.getDataSeries("employees");
        int count = 50 * 1000 * 1000;
        employees.fill(count, new EmployeeSupplier() );

        PreparedProjection<Long, AgeSalary> preparedProjection = employees.prepare(
                new ProjectionRecipe<AgeSalary>(AgeSalary.class, true, new SqlPredicate("true")));
        Map<String, Object> bindings = new HashMap<String, Object>();

        DataSeries ageSalaries = preparedProjection.newDataSeries("ageSalary", bindings);
        MemoryInfo memoryInfo = ageSalaries.memoryInfo();
        System.out.println(memoryInfo);

        assertEquals(count * (INT_SIZE_IN_BYTES+ INT_SIZE_IN_BYTES), memoryInfo.consumedBytes());
        assertEquals(count, ageSalaries.count());
    }

}
