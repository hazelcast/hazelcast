package com.hazelcast.config;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.config.MapIndexConfig.validateIndexAttribute;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class MapIndexConfigTest {

    @Test(expected = IllegalArgumentException.class)
    public void testValidation() {
        validateIndexAttribute(null);
    }

    @Test
    public void testValidation_withKeyNonKeyword() {
        assertEquals("__key.this", validateIndexAttribute("__key.this"));
    }

    @Test
    public void testValidation_withKeyKeyword() {
        assertEquals("__key#value", validateIndexAttribute("__key#value"));
    }

}