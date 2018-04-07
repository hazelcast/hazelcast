package com.hazelcast.dictionary;

import com.hazelcast.config.Config;
import com.hazelcast.config.DictionaryConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.dictionary.examples.EmptyObject;
import com.hazelcast.dictionary.examples.ListReference;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.Test;

import java.util.List;

public class InvalidKeyTest extends HazelcastTestSupport {

    @Test(expected = RuntimeException.class)
    public void whenInterface() {
        newDictionary(List.class);
    }

    @Test(expected = RuntimeException.class)
    public void whenEmptyObject() {
        newDictionary(EmptyObject.class);
    }

    @Test(expected = RuntimeException.class)
    public void whenUnrecognizedFIeld() {
        newDictionary(ListReference.class);
    }

    private Dictionary<Long, Long> newDictionary(Class keyClazz) {
        Config config = new Config();
        config.addDictionaryConfig(
                new DictionaryConfig("foo")
                        .setKeyClass(keyClazz)
                        .setValueClass(Long.class));

        HazelcastInstance hz = createHazelcastInstance(config);
        return hz.getDictionary("foo");
    }
}

