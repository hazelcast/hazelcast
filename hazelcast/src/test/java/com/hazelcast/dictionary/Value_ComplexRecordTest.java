package com.hazelcast.dictionary;

import com.hazelcast.config.Config;
import com.hazelcast.config.DictionaryConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.dictionary.examples.ComplexPrimitiveRecord;
import com.hazelcast.dictionary.examples.ComplexRecord;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class Value_ComplexRecordTest extends HazelcastTestSupport {

    @Test
    public void whenComplexPrimitiveRecord() {
        Dictionary<Long, ComplexPrimitiveRecord> dictionary = newDictionary(ComplexPrimitiveRecord.class);
        ComplexPrimitiveRecord value = new ComplexPrimitiveRecord();
        value._boolean = true;
        value._byte = 10;
        value._short = 10;
        value._char = 10;
        value._int = 10;
        value._long = 10;
        value._float = 10f;
        value._double = 10d;

        dictionary.put(1L, value);

        assertEquals(value, dictionary.get(1L));
    }

    @Test
    public void whenComplexRecord() {
        Dictionary<Long, ComplexRecord> dictionary = newDictionary(ComplexRecord.class);
        ComplexRecord value = new ComplexRecord();
        value._boolean = true;
        value._byte = 10;
        value._short = 10;
        value._char = 10;
        value._int = 10;
        value._long = 10;
        value._float = 10f;
        value._double = 10d;
        value._Boolean = true;
        value._Byte = 10;
        value._Short = 10;
        value._Character = 10;
        value._Integer = 10;
        value._Long = 10L;
        value._Float = 10f;
        value._Double = 10d;

        dictionary.put(1L, value);

        assertEquals(value, dictionary.get(1L));
    }

    private <C> Dictionary<Long, C> newDictionary(Class<C> valueClass) {
        Config config = new Config()
                .setProperty(GroupProperty.PARTITION_COUNT.getName(), "1");
        config.addDictionaryConfig(
                new DictionaryConfig("foo")
                        .setSegmentsPerPartition(1)
                        .setKeyClass(Long.class)
                        .setValueClass(valueClass));

        HazelcastInstance hz = createHazelcastInstance(config);
        return hz.getDictionary("foo");
    }

}
