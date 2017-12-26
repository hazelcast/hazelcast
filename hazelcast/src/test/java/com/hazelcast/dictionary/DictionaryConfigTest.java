package com.hazelcast.dictionary;

import com.hazelcast.config.Config;
import com.hazelcast.config.DictionaryConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.Test;

import static com.hazelcast.spi.properties.GroupProperty.PARTITION_COUNT;

public class DictionaryConfigTest extends HazelcastTestSupport {

    @Test(expected = IllegalStateException.class)
    public void whenSegmentCountEqualsPartitionCount() {
        Config config = new Config()
                .setProperty(PARTITION_COUNT.getName(), "271");
        config.addDictionaryConfig(
                new DictionaryConfig("foo")
                        .setSegmentsPerPartition(271)
                        .setKeyClass(Long.class)
                        .setValueClass(Long.class));

        HazelcastInstance hz = createHazelcastInstance(config);
        Dictionary<Long, Long> dictionary = hz.getDictionary("foo");
        dictionary.get(1L);
    }
}
