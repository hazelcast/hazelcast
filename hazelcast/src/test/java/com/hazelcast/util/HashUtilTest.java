package com.hazelcast.util;

import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.RequireAssertEnabled;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.util.HashUtil.MurmurHash3_x86_32;
import static com.hazelcast.util.HashUtil.hashToIndex;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class HashUtilTest extends HazelcastTestSupport {

    @Test
    public void testConstructor() {
        assertUtilityConstructor(HashUtil.class);
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
