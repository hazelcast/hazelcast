package com.hazelcast.mapreduce.impl.operation;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class GetResultOperationFactoryTest {

    private GetResultOperationFactory factory;

    @Before
    public void setUp() {
        factory = new GetResultOperationFactory("name", "jobId");
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testWriteData() throws Exception {
        factory.writeData(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testReadData() throws Exception {
        factory.readData(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetFactoryId() {
        factory.getFactoryId();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetId() {
        factory.getId();
    }
}
