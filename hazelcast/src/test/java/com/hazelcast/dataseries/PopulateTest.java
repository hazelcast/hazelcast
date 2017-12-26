package com.hazelcast.dataseries;

import com.hazelcast.config.Config;
import com.hazelcast.config.DataSeriesConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.Test;

public class PopulateTest extends HazelcastTestSupport {

    @Test
    public void test() {
        Config config = new Config()
                .addDataSeriesConfig(
                        new DataSeriesConfig("employeesDataset")
                                .setKeyClass(Long.class)
                                .setValueClass(Employee.class));

        HazelcastInstance hz = createHazelcastInstance(config);

        IMap<Long, Employee> employeesMap = hz.getMap("employeesMap");
        int itemCount = 10000;
        for (int k = 0; k < itemCount; k++) {
            employeesMap.put((long) k, new Employee(k, k, k));
        }

        DataSeries<Long, Employee> dataSeries = hz.getDataSeries("employeesDataset");
        dataSeries.populate(employeesMap);

        //assertEquals(itemCount, dataSeries.count());

        System.out.println("dataseries consumed memory:" + dataSeries.memoryInfo().consumedBytes());
        System.out.println("imap consumed memory:" + employeesMap.getLocalMapStats().getHeapCost());
    }
}
