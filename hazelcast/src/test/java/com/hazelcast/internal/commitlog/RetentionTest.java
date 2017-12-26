package com.hazelcast.internal.commitlog;

import com.hazelcast.datastream.Employee;
import org.junit.Test;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;

public class RetentionTest extends CommitLogAbstractTest {

    @Test
    public void retention() {
        CommitLog.Context context = new CommitLog.Context()
                .initialRegionSize(employeeBlobSize)
                .maxRegionSize(employeeBlobSize)
                .retentionPeriod(1, SECONDS)
                .encoder(newEncoder());
        commitLog = new CommitLog(context);

        Employee e1 = new Employee(1, 1, 1);
        commitLog.append(e1);
        Employee e2 = new Employee(2, 2, 2);
        commitLog.append(e2);

        assertEquals(2, commitLog.count());
        assertEquals(2, commitLog.regionCount());

        sleepSeconds(2);
        commitLog.tick();

        assertEquals(1, commitLog.count());
        assertEquals(1, commitLog.regionCount());
        assertEquals(employeeBlobSize, commitLog.head());
        assertEquals(2*employeeBlobSize, commitLog.tail());
    }


}
