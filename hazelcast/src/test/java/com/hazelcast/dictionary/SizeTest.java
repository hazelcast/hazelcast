package com.hazelcast.dictionary;

import com.hazelcast.config.Config;
import com.hazelcast.config.DictionaryConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class SizeTest extends HazelcastTestSupport {

    @Test
    public void whenUntouched() {
        Config config = new Config();
        config.addDictionaryConfig(
                new DictionaryConfig("foo")
                        .setKeyClass(Long.class)
                        .setValueClass(Long.class));

        HazelcastInstance hz = createHazelcastInstance(config);

        Dictionary<Long, Long> dictionary = hz.getDictionary("foo");
        assertEquals(0, dictionary.size());
    }

    @Test
    public void whenManyItems() {
        Config config = new Config();
        config.addDictionaryConfig(
                new DictionaryConfig("foo")
                        .setKeyClass(Long.class)
                        .setValueClass(Long.class));

        HazelcastInstance hz = createHazelcastInstance(config);

        Dictionary<Long, Long> dictionary = hz.getDictionary("foo");

        for (long k = 0; k < 100; k++) {
            dictionary.put(k, 10L);
        }

        assertEquals(100, dictionary.size());
    }
}
