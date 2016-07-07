package com.hazelcast.util;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.jruby.ir.operands.Hash;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class MapUtilTest {

    @Test
    public void isConstructorPrivate() {
        HazelcastTestSupport.assertUtilityConstructor(MapUtil.class);
    }

    @Test
    public void isNullOrEmpty_whenNull() {
        assertTrue(MapUtil.isNullOrEmpty(null));
    }

    @Test
    public void isNullOrEmpty_whenEmpty() {
        assertTrue(MapUtil.isNullOrEmpty(new HashMap()));
    }

    @Test
    public void isNullOrEmpty_whenNotEmpty() {
        Map<String, String> map = new HashMap();
        map.put("a", "b");
        assertFalse(MapUtil.isNullOrEmpty(map));
    }

}
