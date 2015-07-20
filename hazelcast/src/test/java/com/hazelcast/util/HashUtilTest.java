package com.hazelcast.util;

import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.util.HashUtil.hashToIndex;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class HashUtilTest {

    @Test
    public void hashToIndex_whenHashPositive(){
        assertEquals(hashToIndex(20, 100), 20);
        assertEquals(hashToIndex(420, 100), 20);
    }

    @Test
    public void hashToIndex_whenHashZero(){
        int result = hashToIndex(420, 100);
        assertEquals(20, result);
    }

    @Test
    public void hashToIndex_whenHashNegative(){
        int result = hashToIndex(-420, 100);
        assertEquals(20, result);
    }

    @Test
    public void hashToIndex_whenHashIntegerMinValue(){
        int result = hashToIndex(Integer.MIN_VALUE, 100);
        assertEquals(0, result);
    }

    @Test(expected = IllegalArgumentException.class)
    public void hashToIndex_whenItemCountZero(){
        hashToIndex(Integer.MIN_VALUE, 0);
    }
}
