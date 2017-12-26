package com.hazelcast.dataseries;

import com.hazelcast.config.Config;
import com.hazelcast.config.DataSeriesConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.dataseries.impl.DataSeriesProxy;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class IteratorTest extends HazelcastTestSupport {

//    @Test
//    public void iterator() {
//        Config config = new Config()
//                .setProperty(GroupProperty.PARTITION_COUNT.getName(), "1")
//                .addDataSeriesConfig(
//                        new DataSeriesConfig("employees")
//                                .setInitialSegmentSize(256)
//                                .setMaxSegmentSize(256)
//                                .setSegmentsPerPartition(Integer.MAX_VALUE)
//                                .setKeyClass(Long.class)
//                                .setValueClass(Employee.class));
//
//        HazelcastInstance hz = createHazelcastInstance(config);
//
//        List<Employee> employeeList = new ArrayList<>();
//        EmployeeSupplier supplier = new EmployeeSupplier();
//        DataSeriesProxy<Long, Employee> dataSeries = (DataSeriesProxy) hz.getDataSeries("employees");
//        for(long k=0;k<1000;k++){
//            Employee employee = supplier.get();
//            employeeList.add(employee);
//            dataSeries.append(0l, employee);
//        }
//
//        dataSeries.freeze();
//
//        Iterator<Employee> it = dataSeries.iterator(0);
//
//        List actual = new LinkedList();
//        it.forEachRemaining(actual::add);
//
//        assertEquals(employeeList.size(), actual.size());
//        for(int k=0;k<employeeList.size();k++){
//            assertEquals("at index:"+k, employeeList.get(k), actual.get(k));
//        }
//    }
}
