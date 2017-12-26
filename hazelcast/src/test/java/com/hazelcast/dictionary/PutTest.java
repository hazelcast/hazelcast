package com.hazelcast.dictionary;

import com.hazelcast.config.Config;
import com.hazelcast.config.DictionaryConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class PutTest extends HazelcastTestSupport {

    private Dictionary<Long, Long> newDictionary() {
        Config config = new Config();
        config.addDictionaryConfig(
                new DictionaryConfig("foo")
                        .setKeyClass(Long.class)
                        .setValueClass(Long.class));

        HazelcastInstance hz = createHazelcastInstance(config);
        return hz.getDictionary("foo");
    }

    @Test(expected = NullPointerException.class)
    public void whenKeyNull() {
        Dictionary<Long, Long> dictionary = newDictionary();
        dictionary.put(null, 1L);
    }

    @Test(expected = NullPointerException.class)
    public void whenValueNull() {
        Dictionary<Long, Long> dictionary = newDictionary();
        dictionary.put(1L, null);
    }

    @Test
    public void whenKeyWrongType() {

    }

    @Test
    public void whenValueWrongType() {

    }

    @Test
    public void whenFirstTimePut() {
        Dictionary<Long, Long> dictionary = newDictionary();

        Long key = 1L;
        Long value = 123L;
        dictionary.put(key, value);

        assertEquals(1, dictionary.size());
        assertEquals(value, dictionary.get(key));
    }

    @Test
    public void whenOverwriteExistingPut() {
        Dictionary<Long, Long> dictionary = newDictionary();

        Long key = 1L;
        Long oldValue = 2L;
        Long newValue = 3L;
        dictionary.put(key, oldValue);
        dictionary.put(key, newValue);

        assertEquals(1, dictionary.size());
        assertEquals(newValue, dictionary.get(key));
    }


    @Test
    public void whenOverwritingValueManyTimes() {
        Dictionary<Long, Long> dictionary = newDictionary();

        Long key = 1L;
        for (long k = 100000; k > 0; k--) {
            dictionary.put(key, k);
        }

        assertEquals(1, dictionary.size());
        assertEquals(new Long(1), dictionary.get(key));
    }

    @Test
    public void whenPuttingMany() {
        Config config = new Config()
                .setProperty(GroupProperty.PARTITION_COUNT.getName(), "1");

        config.addDictionaryConfig(
                new DictionaryConfig("foo")
                        .setSegmentsPerPartition(1)
                        .setInitialSegmentSize(1024)
                        .setKeyClass(Long.class)
                        .setValueClass(Long.class));

        HazelcastInstance hz = createHazelcastInstance(config);
        Dictionary<Long, Long> dictionary = hz.getDictionary("foo");

        int count = 10 * 1000;
        for (long k = 0; k < count; k++) {
            Long value = 10 * k;
            dictionary.put(k, value);
            assertEquals(value, dictionary.get(k));
            //System.out.println(" at : "+k);
        }
        System.out.println("Insert complete");
        assertEquals(count, dictionary.size());

        int errors = 0;
        for (long k = 0; k < count; k++) {
            Long actual = dictionary.get(k);
            Long expected = 10 * k;
            if (!expected.equals(actual)) {
                System.out.println("expected:" + expected + " actual:" + actual);
                errors++;
            }
        }
        System.out.println("errors:" + errors);
        assertEquals(0, errors);
        System.out.println(dictionary.memoryInfo());
    }
}
