package com.hazelcast.spi.exception;

import com.hazelcast.nio.Address;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class WrongTargetExceptionTest {

    @Test
    public void testConstructor_withTarget() {
        Address address = new Address();
        Address target = new Address();

        WrongTargetException exception = new WrongTargetException(address, target, 23, 42, "WrongTargetExceptionTest");

        assertEquals(target, exception.getTarget());
    }
}
