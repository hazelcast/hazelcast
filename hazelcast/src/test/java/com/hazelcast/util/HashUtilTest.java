package com.hazelcast.util;

import com.hazelcast.internal.memory.impl.EndiannessUtil;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.RequireAssertEnabled;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.nio.ByteBuffer;
import java.util.Random;

import static com.hazelcast.util.HashUtil.MurmurHash3_x86_32;
import static com.hazelcast.util.HashUtil.MurmurHash3_x64_64;
import static com.hazelcast.util.HashUtil.hashToIndex;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class HashUtilTest extends HazelcastTestSupport {

    @Test
    public void testConstructor() {
        assertUtilityConstructor(HashUtil.class);
    }

    @Test
    public void testMurmurHash3_x64_64() {
        final int LENGTH = 16;

        byte[] data = new byte[LENGTH];
        new Random().nextBytes(data);
        ByteBuffer dataBuffer = ByteBuffer.wrap(data);

        HashUtil.LoadStrategy<byte[]> byteArrayLoadStrategy = new HashUtil.LoadStrategy<byte[]>() {

            @Override
            public int getInt(byte[] buf, long offset) {
                return EndiannessUtil.readIntL(this, buf, offset);
            }

            @Override
            public long getLong(byte[] buf, long offset) {
                return EndiannessUtil.readLongL(this, buf, offset);
            }

            @Override
            public byte getByte(byte[] buf, long offset) {
                return buf[(int) offset];
            }

        };
        HashUtil.LoadStrategy<ByteBuffer> byteBufferLoadStrategy = new HashUtil.LoadStrategy<ByteBuffer>() {

            @Override
            public int getInt(ByteBuffer buf, long offset) {
                return EndiannessUtil.readIntL(this, buf, offset);
            }

            @Override
            public long getLong(ByteBuffer buf, long offset) {
                return EndiannessUtil.readLongL(this, buf, offset);
            }

            @Override
            public byte getByte(ByteBuffer buf, long offset) {
                return buf.get((int) offset);
            }

        };

        long hash1 = MurmurHash3_x64_64(byteArrayLoadStrategy, data, 0, LENGTH);
        long hash2 = MurmurHash3_x64_64(byteBufferLoadStrategy, dataBuffer, 0, LENGTH);

        assertEquals(hash1, hash2);
    }

    @Test(expected = AssertionError.class)
    @RequireAssertEnabled
    public void testMurmurHash3_x86_32_withIntOverflow() {
        MurmurHash3_x86_32(null, Integer.MAX_VALUE, Integer.MAX_VALUE);
    }

    @Test
    public void hashToIndex_whenHashPositive() {
        assertEquals(hashToIndex(20, 100), 20);
        assertEquals(hashToIndex(420, 100), 20);
    }

    @Test
    public void hashToIndex_whenHashZero() {
        int result = hashToIndex(420, 100);
        assertEquals(20, result);
    }

    @Test
    public void hashToIndex_whenHashNegative() {
        int result = hashToIndex(-420, 100);
        assertEquals(20, result);
    }

    @Test
    public void hashToIndex_whenHashIntegerMinValue() {
        int result = hashToIndex(Integer.MIN_VALUE, 100);
        assertEquals(0, result);
    }

    @Test(expected = IllegalArgumentException.class)
    public void hashToIndex_whenItemCountZero() {
        hashToIndex(Integer.MIN_VALUE, 0);
    }
}
