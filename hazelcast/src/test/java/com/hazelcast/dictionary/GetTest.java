package com.hazelcast.dictionary;

import com.hazelcast.config.Config;
import com.hazelcast.config.DictionaryConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class GetTest extends HazelcastTestSupport {

    private Dictionary<Long, Long> newDictionary() {
        Config config = new Config();
        config.addDictionaryConfig(
                new DictionaryConfig("foo")
                        .setKeyClass(Long.class)
                        .setValueClass(Long.class));

        HazelcastInstance hz = createHazelcastInstance(config);
        return hz.getDictionary("foo");
    }

    @Test
    public void getWhenNonExisting() {
        Dictionary<Long, Long> dictionary = newDictionary();

        assertNull(dictionary.get(1L));
    }

    @Test
    public void getWhenExisting() {
        Dictionary<Long, Long> dictionary = newDictionary();
        dictionary.put(1L, 2L);

        assertEquals(new Long(2), dictionary.get(1L));
    }
}
