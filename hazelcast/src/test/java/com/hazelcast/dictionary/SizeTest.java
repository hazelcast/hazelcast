package com.hazelcast.dictionary;

import com.hazelcast.config.Config;
import com.hazelcast.config.DictionaryConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class SizeTest extends HazelcastTestSupport {

    private Dictionary<Long, Long> createDictionary() {
        Config config = new Config();
        config.addDictionaryConfig(
                new DictionaryConfig("foo")
                        .setKeyClass(Long.class)
                        .setValueClass(Long.class));

        HazelcastInstance hz = createHazelcastInstance(config);

        return hz.getDictionary("foo");
    }

    @Test
    public void whenUntouched() {
        Dictionary<Long, Long> dictionary = createDictionary();
        assertEquals(0, dictionary.size());
    }


    @Test
    public void whenManyItems() {
        Dictionary<Long, Long> dictionary = createDictionary();

        for (long k = 0; k < 100; k++) {
            dictionary.put(k, 10L);
        }

        assertEquals(100, dictionary.size());
    }
}
