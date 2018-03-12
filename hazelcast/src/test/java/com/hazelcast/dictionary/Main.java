package com.hazelcast.dictionary;

import com.hazelcast.config.Config;
import com.hazelcast.config.DictionaryConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

public class Main {

    public static void main(String[] arg) {
        Config config = new Config();
        config.addDictionaryConfig(
                new DictionaryConfig("foo")
                        .setKeyClass(Long.class)
                        .setValueClass(Long.class));

        HazelcastInstance hz = Hazelcast.newHazelcastInstance(config);
        Dictionary<Long, Long> dictionary = hz.getDictionary("foo");
        dictionary.put(1L, 2L);
        System.out.println("done");
        System.exit(0);
    }
}
