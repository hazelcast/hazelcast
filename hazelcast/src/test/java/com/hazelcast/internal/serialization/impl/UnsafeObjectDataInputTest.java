package com.hazelcast.internal.serialization.impl;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.nio.ByteOrder;

import static org.junit.Assert.assertEquals;

/**
 * UnsafeObjectDataInput Tester.
 *
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class UnsafeObjectDataInputTest extends ByteArrayObjectDataInputTest {

    @Override
    protected void initDataInput() {
        byteOrder = ByteOrder.nativeOrder();
        in = createDataInput(byteOrder);
    }

    @Override
    protected ByteArrayObjectDataInput createDataInput(ByteOrder bo) {
        return new UnsafeObjectDataInput(INIT_DATA, 0, mockSerializationService);
    }

    @Test
    public void testGetByteOrder() throws Exception {
        assertEquals(ByteOrder.nativeOrder(), in.getByteOrder());
    }

}
