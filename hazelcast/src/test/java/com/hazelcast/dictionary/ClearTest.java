package com.hazelcast.dictionary;

import com.hazelcast.config.Config;
import com.hazelcast.config.DictionaryConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class ClearTest extends HazelcastTestSupport {

    @Test
    public void test_whenUnused(){
        Dictionary<Long, Long> dictionary = newDictionary();
        dictionary.clear();

        assertEquals(0, dictionary.size());
    }

    @Test
    public void test_whenNotEmpty() {
        Dictionary<Long, Long> dictionary = newDictionary();
        dictionary.put(1L, 2L);
        dictionary.clear();

        assertEquals(0, dictionary.size());
        assertNull(dictionary.get(1L));
    }

    private Dictionary<Long, Long> newDictionary() {
        Config config = new Config();
        config.addDictionaryConfig(
                new DictionaryConfig("foo")
                        .setKeyClass(Long.class)
                        .setValueClass(Long.class));

        HazelcastInstance hz = createHazelcastInstance(config);
        return hz.getDictionary("foo");
    }
}
