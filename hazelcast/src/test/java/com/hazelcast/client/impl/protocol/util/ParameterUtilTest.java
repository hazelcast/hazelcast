package com.hazelcast.client.impl.protocol.util;

import com.hazelcast.internal.serialization.impl.HeapData;
import com.hazelcast.map.impl.MapEntrySimple;
import com.hazelcast.nio.Bits;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.client.impl.protocol.util.ParameterUtil.UTF8_MAX_BYTES_PER_CHAR;
import static com.hazelcast.client.impl.protocol.util.ParameterUtil.calculateDataSize;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ParameterUtilTest extends HazelcastTestSupport {

    private static final Data DATA = new HeapData(new byte[]{0, 1, 2, 3, 4, 5, 6, 7});

    @Test
    public void testConstructor() {
        assertUtilityConstructor(ParameterUtil.class);
    }

    @Test
    public void testCalculateDataSize_withString() {
        int size = calculateDataSize("test");

        assertEquals(Bits.INT_SIZE_IN_BYTES + UTF8_MAX_BYTES_PER_CHAR * 4, size);
    }

    @Test
    public void testCalculateDataSize_withData() {
        int size = calculateDataSize(DATA);

        assertEquals(Bits.INT_SIZE_IN_BYTES + 8, size);
    }

    @Test
    public void testCalculateDataSize_withMapEntry() {
        int size = calculateDataSize(new MapEntrySimple<Data, Data>(DATA, DATA));

        assertEquals(calculateDataSize(DATA) * 2, size);
    }

    @Test
    public void testCalculateDataSize_withInteger() {
        int size = calculateDataSize(Integer.MAX_VALUE);

        assertEquals(Bits.INT_SIZE_IN_BYTES, size);
    }

    @Test
    public void testCalculateDataSize_withBoolean() {
        int size = calculateDataSize(Boolean.TRUE);

        assertEquals(Bits.BOOLEAN_SIZE_IN_BYTES, size);
    }

    @Test
    public void testCalculateDataSize_withLong() {
        int size = calculateDataSize(Long.MAX_VALUE);

        assertEquals(Bits.LONG_SIZE_IN_BYTES, size);
    }
}
