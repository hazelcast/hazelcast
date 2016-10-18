package com.hazelcast.spi;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class AbstractLocalOperationTest {

    private AbstractLocalOperation operation = new AbstractLocalOperation() {
        @Override
        public void run() throws Exception {
        }
    };

    @Test(expected = UnsupportedOperationException.class)
    public void testWriteInternal() throws Exception {
        operation.writeInternal(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testReadInternal() throws Exception {
        operation.readInternal(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetFactoryId() {
        operation.getFactoryId();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetId() {
        operation.getId();
    }
}
