package com.hazelcast.datastream.impl;

import com.hazelcast.config.DataStreamConfig;
import com.hazelcast.datastream.Employee;
import com.hazelcast.internal.codeneneration.Compiler;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.Before;
import org.junit.Test;

import static java.lang.Math.min;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

public class DSPartition_TrimTest extends HazelcastTestSupport {

    public static final int EMPLOYEE_SIZE = 20;
    private InternalSerializationService serializationService;

    @Before
    public void before() {
        serializationService = new DefaultSerializationServiceBuilder().build();
    }

    @Test
    public void headTail() {
        DataStreamConfig config = new DataStreamConfig()
                .setValueClass(Employee.class)
                .setInitialRegionSize(EMPLOYEE_SIZE)
                .setMaxRegionSize(EMPLOYEE_SIZE)
                .setMaxRegionsPerPartition(10);
        DSPartition partition = newPartition(config);

        assertEquals(0, partition.head());
        assertEquals(0, partition.tail());
        long expectedHead = 0;

        for (int k = 1; k < 100; k++) {
            Employee e = new Employee();
            partition.append(e);
            System.out.println(partition.head());

            if (k <= config.getMaxRegionsPerPartition()) {
                assertEquals(0, partition.head());
            } else {
                System.out.println((k-12)*EMPLOYEE_SIZE+" "+partition.head());
                //assertEquals((k-10)*EMPLOYEE_SIZE, partition.head());
                //assertEquals(config.getMaxRegionsPerPartition(), partition.regionCount());
            }

            assertEquals(k * EMPLOYEE_SIZE, partition.tail());
        }
    }

    // a test that verifies that we remain within the maximum number of regions per partition.
    @Test
    public void testRegionCount() {
        DataStreamConfig config = new DataStreamConfig()
                .setValueClass(Employee.class)
                .setInitialRegionSize(EMPLOYEE_SIZE)
                .setMaxRegionSize(EMPLOYEE_SIZE)
                .setMaxRegionsPerPartition(10);
        DSPartition partition = newPartition(config);

        for (int k = 1; k < 100; k++) {
            Employee e = new Employee();
            partition.append(e);
            assertEquals(min(k,config.getMaxRegionsPerPartition()), partition.regionCount());
        }
    }

    @Test
    public void testCount() {
        DataStreamConfig config = new DataStreamConfig()
                .setValueClass(Employee.class)
                .setInitialRegionSize(EMPLOYEE_SIZE)
                .setMaxRegionSize(EMPLOYEE_SIZE)
                .setMaxRegionsPerPartition(10);

        DSPartition partition = newPartition(config);

        for (int k = 1; k < 100; k++) {
            Employee e = new Employee();
            partition.append(e);
            assertEquals(min(k,config.getMaxRegionsPerPartition()), partition.count());
        }
    }

    private DSPartition newPartition(DataStreamConfig config) {
        DSService mock = mock(DSService.class);
        //when(mock.getOrCreatePartitionListeners(any(),any(),any())).thenReturn(new DSPartitionListeners(mock,"foo",null,serializationService));
        return new DSPartition(mock, 0, config, serializationService, new Compiler("datastream-src/"));
    }
}
