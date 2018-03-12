package com.hazelcast.dictionary;

import com.hazelcast.config.Config;
import com.hazelcast.config.DictionaryConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ClearTest extends HazelcastTestSupport {

    @Test
    public void clear() {
        Dictionary<Long, Long> dictionary = newDictionary();
        dictionary.put(1L, 2L);
        dictionary.clear();

        assertEquals(0, dictionary.size());
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
