package com.hazelcast.internal.commitlog;

import com.hazelcast.datastream.Employee;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.After;
import org.junit.Before;

import static com.hazelcast.nio.Bits.INT_SIZE_IN_BYTES;

public class CommitLogAbstractTest extends HazelcastTestSupport {

    public InternalSerializationService serializationService;
    public CommitLog commitLog;
    public int employeeBlobSize;

    @Before
    public void before() {
        serializationService = new DefaultSerializationServiceBuilder().build();
        employeeBlobSize = serializationService.toData(new Employee()).totalSize() + INT_SIZE_IN_BYTES;
    }

    @After
    public void after() {
        if (commitLog != null) {
            commitLog.destroy();
        }
    }

    public HeapDataEncoder newEncoder() {
        return new HeapDataEncoder(serializationService);
    }

}
