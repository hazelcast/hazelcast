package com.hazelcast.dictionary;

import com.hazelcast.config.Config;
import com.hazelcast.config.DictionaryConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.spi.properties.GroupProperty;

public class Main2 {

    public static void main(String[] arg) {
        Config config = new Config()
                .setProperty(GroupProperty.PARTITION_COUNT.getName(),"1");
        config.addDictionaryConfig(
                new DictionaryConfig("foo")
                        .setSegmentsPerPartition(1)
                        .setKeyClass(Long.class)
                        .setValueClass(SomeRecord.class));

        HazelcastInstance hz = Hazelcast.newHazelcastInstance(config);
        Dictionary<Long, SomeRecord> dictionary = hz.getDictionary("foo");

        for (long k = 0; k < 10000; k++) {
            dictionary.put(k, new SomeRecord());
        }

        System.out.println("size:" + dictionary.size());
        System.out.println("done");
        System.exit(0);
    }
}
