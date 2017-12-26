package com.hazelcast.datastream.impl;

import com.hazelcast.config.DataStreamConfig;
import com.hazelcast.datastream.Employee;
import com.hazelcast.internal.codeneneration.Compiler;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.internal.serialization.impl.HeapData;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.Before;
import org.junit.Test;

import java.util.LinkedList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

public class DSPartition_AppendSingleRegion extends HazelcastTestSupport {

    public static final int EMPLOYEE_SIZE = 20;
    private InternalSerializationService serializationService;

    @Before
    public void before() {
        serializationService = new DefaultSerializationServiceBuilder().build();
    }

    @Test
    public void withValueClass() {
        DataStreamConfig config = new DataStreamConfig()
                .setValueClass(Employee.class);
        DSPartition partition = newPartition(config);

        for (int k = 1; k <= 100; k++) {
            Employee e = new Employee(k, k, k);
            partition.append(e);
            assertEquals(k, partition.count());
        }

        assertEquals(100, partition.count());
        for (int k = 1; k <= 100; k++) {
            HeapData found = partition.load((k - 1) * EMPLOYEE_SIZE);
            assertEquals(new Employee(k, k, k), serializationService.toObject(found));
        }
    }

    @Test
    public void whenNotEnoughSpace() {
        DataStreamConfig config = new DataStreamConfig()
                .setInitialRegionSize(3 * EMPLOYEE_SIZE)
                .setMaxRegionSize(3 * EMPLOYEE_SIZE)
                .setMaxRegionsPerPartition(Integer.MAX_VALUE)
                .setValueClass(Employee.class);
        DSPartition partition = newPartition(config);

        int count = 100;
        for (int k = 1; k <= count; k++) {
            Employee e = new Employee(k, k, k);
            partition.append(e);
            assertEquals(k, partition.count());
        }

        assertEquals(count, partition.count());
        for (int k = 1; k <= count; k++) {
            HeapData found = partition.load((k - 1) * EMPLOYEE_SIZE);
            assertEquals(new Employee(k, k, k), serializationService.toObject(found));
        }
        assertEquals(0, partition.head());
        assertEquals(count * EMPLOYEE_SIZE, partition.tail());
    }

    @Test
    public void whenGrowingOfRegionIsNeeded() {
        DataStreamConfig config = new DataStreamConfig()
                .setInitialRegionSize(1)
                .setMaxRegionSize(64 * 1024)
                .setValueClass(Employee.class);
        DSPartition partition = newPartition(config);

        for (int k = 1; k <= 100; k++) {
            Employee e = new Employee(k, k, k);
            partition.append(e);
            assertEquals(k, partition.count());
        }

        assertEquals(100, partition.count());
        for (int k = 1; k <= 100; k++) {
            HeapData found = partition.load((k - 1) * EMPLOYEE_SIZE);
            assertEquals(new Employee(k, k, k), serializationService.toObject(found));
        }
    }

    @Test
    public void withoutValueClass() {
        DataStreamConfig config = new DataStreamConfig();
        DSPartition partition = newPartition(config);

        List<Long> offsets = new LinkedList<>();
        for (int k = 1; k <= 100; k++) {
            Employee e = new Employee(k, k, k);
            offsets.add(partition.append(e));
            assertEquals(k, partition.count());
        }

        assertEquals(100, partition.count());
        for (int k = 1; k <= 100; k++) {
            long offset = offsets.get(k - 1);
            HeapData found = partition.load(offset);
            assertEquals(new Employee(k, k, k), serializationService.toObject(found));
        }
    }

    private DSPartition newPartition(DataStreamConfig config) {
        DSService mock = mock(DSService.class);
        //when(mock.getOrCreatePartitionListeners(any(),any(),any())).thenReturn(new DSPartitionListeners(mock,"foo",null,serializationService));
        return new DSPartition(mock, 0, config, serializationService, new Compiler("datastream-src/"));
    }
}