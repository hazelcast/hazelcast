package com.hazelcast.dictionary;

import com.hazelcast.config.Config;
import com.hazelcast.config.DictionaryConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class PutTest extends HazelcastTestSupport {

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
    public void whenKeyNull() {
        Dictionary<Long, Long> dictionary = newDictionary();
        dictionary.put(null, 1L);
    }

    @Test(expected = NullPointerException.class)
    public void whenValueNull() {
        Dictionary<Long, Long> dictionary = newDictionary();
        dictionary.put(1L, null);
    }

    @Test
    public void whenKeyWrongType() {

    }

    @Test
    public void whenValueWrongType() {

    }

    @Test
    public void whenFirstTimePut() {
        Dictionary<Long, Long> dictionary = newDictionary();

        Long key = 1L;
        Long value = 123L;
        dictionary.put(key, value);

        assertEquals(1, dictionary.size());
        assertEquals(value, dictionary.get(key));
    }

    @Test
    public void whenOverwriteExistingPut() {
        Dictionary<Long, Long> dictionary = newDictionary();

        Long key = 1L;
        Long oldValue = 2L;
        Long newValue = 3L;
        dictionary.put(key, oldValue);
        dictionary.put(key, newValue);

        assertEquals(1, dictionary.size());
        assertEquals(newValue, dictionary.get(key));
    }
}
