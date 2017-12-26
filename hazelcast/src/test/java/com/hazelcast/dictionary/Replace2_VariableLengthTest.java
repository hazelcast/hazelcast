package com.hazelcast.dictionary;

import com.hazelcast.config.Config;
import com.hazelcast.config.DictionaryConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class Replace2_VariableLengthTest extends HazelcastTestSupport {

    private Dictionary<Long, byte[]> createDictionary() {
        Config config = new Config()
                .setProperty(GroupProperty.PARTITION_COUNT.getName(),"1");
        config.addDictionaryConfig(
                new DictionaryConfig("foo")
                        .setSegmentsPerPartition(1)
                        .setKeyClass(Long.class)
                        .setValueClass(byte[].class));

        HazelcastInstance hz = createHazelcastInstance(config);

        return hz.getDictionary("foo");
    }

    @Test
    public void whenUntouched() {
        Dictionary<Long, byte[]> dictionary = createDictionary();

        boolean result = dictionary.replace(1L,new byte[]{1,2,3});

        assertFalse(result);
        assertNull(dictionary.get(1L));
        assertEquals(0,dictionary.size());
    }

    @Test
    public void whenTouched_butNoExistingValue() {
        Dictionary<Long, byte[]> dictionary = createDictionary();
        dictionary.put(100L,new byte[]{1,2,3});

        boolean result = dictionary.replace(1L,new byte[]{3,4,5});

        assertFalse(result);
        assertNull(dictionary.get(1L));
        assertEquals(1,dictionary.size());
    }

    @Test
    public void whenExistingValue() {
        Dictionary<Long, byte[]> dictionary = createDictionary();

        dictionary.put(1L,new byte[]{1,2,3});

        boolean result = dictionary.replace(1L,new byte[]{3,4,5,6});

        assertTrue(result);
        assertArrayEquals(new byte[]{3,4,5,6}, dictionary.get(1L));
        assertEquals(1,dictionary.size());
    }
}

