package com.hazelcast.spi.exception;

import com.hazelcast.nio.Address;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class WrongTargetExceptionTest {

    @Test
    public void testConstructor_withTarget() {
        Address address = new Address();
        Address target = new Address();

        WrongTargetException exception = new WrongTargetException(address, target, 23, 42, "WrongTargetExceptionTest");

        assertEquals(target, exception.getTarget());
    }
}
