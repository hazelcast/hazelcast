package com.hazelcast.dataset;

import com.hazelcast.config.Config;
import com.hazelcast.config.DataSetConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.Test;

public class PopulateTest extends HazelcastTestSupport {

    @Test
    public void test() {
        Config config = new Config()
                .addDataSetConfig(
                        new DataSetConfig("employeesDataset")
                                .setKeyClass(Long.class)
                                .setValueClass(Employee.class));

        HazelcastInstance hz = createHazelcastInstance(config);

        IMap<Long, Employee> employeesMap = hz.getMap("employeesMap");
        int itemCount = 10000;
        for (int k = 0; k < itemCount; k++) {
            employeesMap.put((long) k, new Employee(k, k, k));
        }

        DataSet<Long, Employee> dataSet = hz.getDataSet("employeesDataset");
        dataSet.populate(employeesMap);

        //assertEquals(itemCount, dataSet.count());

        System.out.println("dataset consumed memory:" + dataSet.memoryInfo().consumedBytes());
        System.out.println("imap consumed memory:" + employeesMap.getLocalMapStats().getHeapCost());
    }
}
