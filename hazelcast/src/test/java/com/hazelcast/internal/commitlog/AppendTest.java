package com.hazelcast.internal.commitlog;

import com.hazelcast.datastream.Employee;
import com.hazelcast.internal.serialization.impl.HeapData;
import org.junit.Test;

import java.util.LinkedList;
import java.util.List;

import static com.hazelcast.memory.MemoryUnit.MEGABYTES;
import static org.junit.Assert.assertEquals;

public class AppendTest extends CommitLogAbstractTest {

    @Test
    public void singleAppend() {
        CommitLog.Context context = new CommitLog.Context()
                .encoder(newEncoder());
        commitLog = new CommitLog(context);

        Employee e = new Employee(1, 1, 1);
        long offset = commitLog.append(e);

        assertEquals(0, offset);
        assertEquals(1, commitLog.count());
        assertEquals(0, commitLog.head());
        assertEquals(employeeBlobSize, commitLog.tail());

        HeapData found = commitLog.load(commitLog.head());
        assertEquals(e, serializationService.toObject(found));
    }

    @Test
    public void multipleAppendsInSingleRegion() {
        test(new CommitLog.Context()
                .initialRegionSize(1, MEGABYTES)
                .maxRegionSize(1, MEGABYTES)
                .encoder(newEncoder()));
    }

    @Test
    public void whenMultipleRegionsNeeded() {
        test(new CommitLog.Context()
                .encoder(newEncoder())
                .initialRegionSize(1024)
                .maxRegionSize(1024));
    }

    @Test
    public void whenGrowingOfRegionIsNeeded() {
        test(new CommitLog.Context()
                .encoder(newEncoder())
                .initialRegionSize(2)
                .maxRegionSize(1, MEGABYTES));
    }

    private void test(CommitLog.Context context) {
        commitLog = new CommitLog(context);

        List<Long> offsets = new LinkedList<>();
        int count = 100;
        for (int k = 1; k <= count; k++) {
            Employee e = new Employee(k, k, k);
            offsets.add(commitLog.append(e));
            assertEquals(k, commitLog.count());
        }

        assertEquals(count, commitLog.count());
        assertEquals(100, commitLog.count());
        for (int k = 1; k <= 100; k++) {
            HeapData found = commitLog.load(offsets.get(k - 1));
            assertEquals(new Employee(k, k, k), serializationService.toObject(found));
        }
        assertEquals(0, commitLog.head());
    }
}