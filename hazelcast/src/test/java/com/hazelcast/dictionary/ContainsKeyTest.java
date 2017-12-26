package com.hazelcast.dictionary;

import com.hazelcast.config.Config;
import com.hazelcast.config.DictionaryConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ContainsKeyTest extends HazelcastTestSupport {

    private Dictionary<Long, Long> newDictionary() {
        Config config = new Config();
        config.addDictionaryConfig(
                new DictionaryConfig("foo")
                        .setKeyClass(Long.class)
                        .setValueClass(Long.class));

        HazelcastInstance hz = createHazelcastInstance(config);
        return hz.getDictionary("foo");
    }

    @Test(expected = NullPointerException.class)
    public void whenNullKey(){
        Dictionary<Long, Long> dictionary = newDictionary();

        dictionary.containsKey(null);
    }

    @Test
    public void whenNonExisting() {
        Dictionary<Long, Long> dictionary = newDictionary();

        assertFalse(dictionary.containsKey(1L));
    }

    @Test
    public void whenExisting() {
        Dictionary<Long, Long> dictionary = newDictionary();
        dictionary.put(1L, 2L);

        assertTrue(dictionary.containsKey(1L));
    }
}