package com.hazelcast.internal.commitlog;

import com.hazelcast.datastream.Employee;
import org.junit.Test;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class TrimTest extends CommitLogAbstractTest {

    @Test
    public void whenEdenRemains() {
        CommitLog.Context context = new CommitLog.Context()
                .encoder(newEncoder())
                .initialRegionSize(employeeBlobSize)
                .maxRegionSize(employeeBlobSize)
                .maxRegions(2);
        commitLog = new CommitLog(context);

        Employee e1 = new Employee(1, 1, 1);
        Employee e2 = new Employee(1, 1, 1);
        Employee e3 = new Employee(1, 1, 1);
        Employee e4 = new Employee(1, 1, 1);
        Employee e5 = new Employee(1, 1, 1);
        Employee e6 = new Employee(1, 1, 1);

        long offset1 = commitLog.append(e1);
        commitLog.tick();
        assertEquals(0, offset1);
        assertEquals(1, commitLog.count());
        assertEquals(1, commitLog.regionCount());
        assertEquals(0, commitLog.head());
        assertEquals(employeeBlobSize, commitLog.tail());

        long offset2 = commitLog.append(e2);
        commitLog.tick();
        assertEquals(employeeBlobSize, offset2);
        assertEquals(2, commitLog.count());
        assertEquals(2, commitLog.regionCount());
        assertEquals(0, commitLog.head());
        assertEquals(employeeBlobSize * 2, commitLog.tail());

        long offset3 = commitLog.append(e3);
        commitLog.tick();
        assertEquals(employeeBlobSize * 2, offset3);
        assertEquals(2, commitLog.count());
        assertEquals(2, commitLog.regionCount());
        assertEquals(employeeBlobSize, commitLog.head());
        assertEquals(employeeBlobSize * 3, commitLog.tail());

        long offset4 = commitLog.append(e4);
        commitLog.tick();
        assertEquals(employeeBlobSize * 3, offset4);
        assertEquals(2, commitLog.count());
        assertEquals(2, commitLog.regionCount());
        assertEquals(employeeBlobSize * 2, commitLog.head());
        assertEquals(employeeBlobSize * 4, commitLog.tail());

        long offset5 = commitLog.append(e5);
        commitLog.tick();
        assertEquals(employeeBlobSize * 4, offset5);
        assertEquals(2, commitLog.count());
        assertEquals(2, commitLog.regionCount());
        assertEquals(employeeBlobSize * 3, commitLog.head());
        assertEquals(employeeBlobSize * 5, commitLog.tail());

        long offset6 = commitLog.append(e6);
        commitLog.tick();
        assertEquals(employeeBlobSize * 5, offset6);
        assertEquals(2, commitLog.count());
        assertEquals(2, commitLog.regionCount());
        assertEquals(employeeBlobSize * 4, commitLog.head());
        assertEquals(employeeBlobSize * 6, commitLog.tail());
    }

    @Test
    public void whenAllRegionsGetEvicted(){
        CommitLog.Context context = new CommitLog.Context()
                .encoder(newEncoder())
                .retentionPeriod(1, TimeUnit.SECONDS)
                .initialRegionSize(employeeBlobSize)
                .maxRegionSize(employeeBlobSize)
                .maxRegions(100);
        commitLog = new CommitLog(context);
        int count = 20;
        for(int k = 0; k< count; k++){
            commitLog.append(new Employee(1,1,1));
        }

        // give time for everything to expire.
        sleepSeconds(2);

        commitLog.tick();

        //assertNull(commitLog.eden());
        assertNull(commitLog.oldestTenured());
        assertNull(commitLog.youngestTenured());
        assertEquals(0, commitLog.count());
        assertEquals(0, commitLog.regionCount());
        assertEquals(employeeBlobSize * 20, commitLog.head());
        assertEquals(employeeBlobSize * 20, commitLog.tail());
    }

    @Test
    public void test() {
        CommitLog.Context context = new CommitLog.Context()
                .encoder(newEncoder())
                .initialRegionSize(employeeBlobSize * 10)
                .maxRegionSize(employeeBlobSize * 10)
                .maxRegions(100);
        commitLog = new CommitLog(context);

        List<Long> offsets = new LinkedList<>();
        Map<Long, Employee> employees = new HashMap<>();
        int appendCount = 100 * 1000;
        for (int k = 1; k <= appendCount; k++) {
            Employee e = new Employee(k, k, k);
            long offset = commitLog.append(e);
            offsets.add(offset);
            employees.put(offset, e);
        }

        assertEquals(1000, commitLog.count());
        assertEquals(appendCount * employeeBlobSize, commitLog.tail());
        //assertEquals(commitLog.tail()-(1000*employeeBlobSize), commitLog.head());

        // and verify the last 1000 items.
        for (int k = 0; k < 1000; k++) {
            long offset = offsets.get(appendCount - 1 - k);
            Employee found = serializationService.toObject(commitLog.load(offset));
            Employee expected = employees.get(offset);
            assertEquals(expected, serializationService.toObject(found));
        }

        System.out.println(commitLog.count());
    }
}
