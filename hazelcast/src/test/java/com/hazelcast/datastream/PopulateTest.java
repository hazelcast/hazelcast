package com.hazelcast.datastream;

import com.hazelcast.config.Config;
import com.hazelcast.config.DataStreamConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.Test;

public class PopulateTest extends HazelcastTestSupport {

    @Test
    public void test() {
        Config config = new Config()
                .addDataStreamConfig(
                        new DataStreamConfig("employeesDataset")
                                .setValueClass(Employee.class));

        HazelcastInstance hz = createHazelcastInstance(config);

        IMap<Long, Employee> employeesMap = hz.getMap("employeesMap");
        int itemCount = 10000;
        for (int k = 0; k < itemCount; k++) {
            employeesMap.put((long) k, new Employee(k, k, k));
        }

        DataStream<Employee> stream = hz.getDataStream("employeesDataset");
        stream.populate(employeesMap);

        //assertEquals(itemCount, stream.count());

        System.out.println("datastream consumed memory:" + stream.asFrame().memoryInfo().consumedBytes());
        System.out.println("imap consumed memory:" + employeesMap.getLocalMapStats().getHeapCost());
    }
}
