package com.hazelcast.dictionary;

import com.hazelcast.config.Config;
import com.hazelcast.config.DictionaryConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class Remove1Test extends HazelcastTestSupport {

    private Dictionary<Long, Long> newDictionary() {
        Config config = new Config()
                .setProperty(GroupProperty.PARTITION_COUNT.getName(), "1");
        config.addDictionaryConfig(
                new DictionaryConfig("foo")
                        .setSegmentsPerPartition(1)
                        .setKeyClass(Long.class)
                        .setValueClass(Long.class));

        HazelcastInstance hz = createHazelcastInstance(config);
        return hz.getDictionary("foo");
    }

    @Test(expected = NullPointerException.class)
    public void whenNullKey() {
        Dictionary<Long, Long> dictionary = newDictionary();
        dictionary.remove(null);
    }

    @Test
    public void whenNotAllocated() {
        Dictionary<Long, Long> dictionary = newDictionary();

        assertFalse(dictionary.remove(1L));
    }

    @Test
    public void whenNotExisting() {
        Dictionary<Long, Long> dictionary = newDictionary();
        dictionary.put(1L, 1L);
        dictionary.put(2L, 2L);

        assertFalse(dictionary.remove(3L));
        assertEquals(2, dictionary.size());
    }

    @Test
    public void whenExisting() {
        Dictionary<Long, Long> dictionary = newDictionary();
        dictionary.put(1L, 1L);
        dictionary.put(2L, 2L);
        dictionary.put(3L, 2L);

        assertTrue(dictionary.remove(2L));
        assertFalse(dictionary.containsKey(2L));
        assertEquals(2, dictionary.size());
    }

}
