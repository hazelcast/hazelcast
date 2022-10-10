package com.hazelcast.alto.offheapmap;

import com.hazelcast.internal.alto.offheapmap.Bin;
import com.hazelcast.internal.alto.offheapmap.Bout;
import com.hazelcast.internal.alto.offheapmap.OffheapMap;
import com.hazelcast.internal.tpc.iobuffer.IOBuffer;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.internal.alto.OffheapAllocator;
import com.hazelcast.internal.alto.FrameCodec;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.internal.nio.Bits.INT_SIZE_IN_BYTES;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class OffheapMapTest {

    private OffheapMap map;

    @Before
    public void before() {
        map = new OffheapMap(16, new OffheapAllocator());
    }

    @After
    public void after() {
        if (map != null) {
            map.clear();
        }
    }

    public void assertMapSize(int expected) {
        assertEquals("map size should be the same", expected, map.size());
    }

    public void assertGet(String key, String expectedValue) {
        String actual = get(key);
        assertEquals("values do not match for key " + key, expectedValue, actual);
    }

    public void put(String key, String value) {
        byte[] keyBytes = key.getBytes();
        byte[] valueBytes = value.getBytes();

        IOBuffer buf = new IOBuffer(32);
        buf.writeInt(-1);
        buf.writeSizedBytes(keyBytes);
        buf.writeSizedBytes(valueBytes);
        FrameCodec.setSize(buf);


        buf.position(INT_SIZE_IN_BYTES);

        Bin keyBin = new Bin();
        keyBin.init(buf);

        Bin valueBin = new Bin();
        valueBin.init(buf);

        map.set(keyBin, valueBin);
    }

    public String get(String key) {
        byte[] keyBytes = key.getBytes();
        IOBuffer buf = new IOBuffer(32);
        buf.writeInt(-1);
        buf.writeSizedBytes(keyBytes);
        FrameCodec.setSize(buf);

        buf.position(INT_SIZE_IN_BYTES);

        Bin keyBin = new Bin();
        keyBin.init(buf);
        IOBuffer response = new IOBuffer(32);
        Bout valueBout = new Bout();
        valueBout.init(response);
        map.get(keyBin, valueBout);

        byte[] bytes = valueBout.bytes();
        return bytes == null ? null : new String(bytes);
    }

    @Test
    public void test_get_when_null() {
        assertGet("1", null);
    }

    @Test
    public void test_put_first() {
        put("1", "a");

        assertGet("1", "a");
        assertMapSize(1);
    }

    @Test
    public void test_put_overwrite_same_size() {
        put("1", "a");
        put("1", "b");

        assertGet("1", "b");
        assertMapSize(1);
    }

    @Test
    public void test_put_overwrite_smaller_size() {
        put("1", "aa");
        put("1", "b");

        assertGet("1", "b");
        assertMapSize(1);
    }

    @Test
    public void test_put_overwrite_larger_size() {
        put("1", "a");
        put("1", "bb");

        assertGet("1", "bb");
        assertMapSize(1);
    }

    @Test
    public void testManyItems() {
        int itemCount = 1_000_000;

        for (int k = 0; k < itemCount; k++) {
            put("" + k, "value-" + k);
        }

        assertMapSize(itemCount);

        System.out.println("verify content");

        for (int k = 0; k < itemCount; k++) {
            assertGet("" + k, "value-" + k);
        }
    }

    @Test
    public void testLoadFactor() {
        int itemCount = 1_000;

        for (int k = 0; k < itemCount; k++) {
            put("" + k, "value-" + k);
        }

        assertEquals("load factor not the same", (1.0f * itemCount / map.tableSize()), map.loadFactor(), 0.1);
    }

    @Test
    public void testClear() {
        int itemCount = 1_000;

        for (int k = 0; k < itemCount; k++) {
            put("" + k, "value-" + k);
        }

        map.clear();
        assertMapSize(0);

        for (int k = 0; k < itemCount; k++) {
            put("" + k, "value-" + k);
        }

        assertMapSize(itemCount);
        for (int k = 0; k < itemCount; k++) {
            assertGet("" + k, "value-" + k);
        }
        assertMapSize(itemCount);
    }
}
