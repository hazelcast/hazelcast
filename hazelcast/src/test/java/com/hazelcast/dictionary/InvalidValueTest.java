package com.hazelcast.dictionary;

import com.hazelcast.config.Config;
import com.hazelcast.config.DictionaryConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.dictionary.examples.EmptyObject;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.Test;

import java.io.Serializable;
import java.util.List;

public class InvalidValueTest extends HazelcastTestSupport {

    @Test(expected = RuntimeException.class)
    public void whenInterface() {
        newDictionary(List.class).size();
    }

    @Test(expected = RuntimeException.class)
    public void whenEmptyObject() {
        newDictionary(EmptyObject.class).size();
    }

    @Test(expected = RuntimeException.class)
    public void whenUnrecognizedFIeld() {
        newDictionary(ListField.class).size();
    }

    private Dictionary<Long, Long> newDictionary(Class valueClass) {
        Config config = new Config();
        config.addDictionaryConfig(
                new DictionaryConfig("foo")
                        .setKeyClass(Long.class)
                        .setValueClass(valueClass));

        HazelcastInstance hz = createHazelcastInstance(config);
        return hz.getDictionary("foo");
    }

    public static class ListField implements Serializable {
        public List list;
    }
}
