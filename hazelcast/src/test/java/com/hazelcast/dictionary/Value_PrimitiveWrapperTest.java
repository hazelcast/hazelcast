package com.hazelcast.dictionary;

import com.hazelcast.config.Config;
import com.hazelcast.config.DictionaryConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class Value_PrimitiveWrapperTest extends HazelcastTestSupport {

    @Test
    public void test_Boolean_whenTrue() {
        Dictionary<Long, Boolean> dictionary = newDictionary(Boolean.class);
        Boolean p = true;

        dictionary.put(1L, p);

        assertEquals(p, dictionary.get(1L));
    }

    @Test
    public void test_Boolean_whenFalse() {
        Dictionary<Long, Boolean> dictionary = newDictionary(Boolean.class);
        Boolean p = false;

        dictionary.put(1L, p);

        assertEquals(p, dictionary.get(1L));
    }

    @Test
    public void test_Byte() {
        Dictionary<Long, Byte> dictionary = newDictionary(Byte.class);
        Byte p = 10;

        dictionary.put(1L, p);

        assertEquals(p, dictionary.get(1L));
    }

    @Test
    public void test_Character() {
        Dictionary<Long, Character> dictionary = newDictionary(Character.class);
        Character p = 10;

        dictionary.put(1L, p);

        assertEquals(p, dictionary.get(1L));
    }

    @Test
    public void test_Short() {
        Dictionary<Long, Short> dictionary = newDictionary(Short.class);
        Short p = 10;

        dictionary.put(1L, p);

        assertEquals(p, dictionary.get(1L));
    }

    @Test
    public void test_Integer() {
        Dictionary<Long, Integer> dictionary = newDictionary(Integer.class);
        Integer p = 10;

        dictionary.put(1L, p);

        assertEquals(p, dictionary.get(1L));
    }

    @Test
    public void test_Long() {
        Dictionary<Long, Long> dictionary = newDictionary(Long.class);
        Long p = 10L;

        dictionary.put(1L, p);

        assertEquals(p, dictionary.get(1L));
    }

    @Test
    public void test_Float() {
        Dictionary<Long, Float> dictionary = newDictionary(Float.class);
        Float p = 10f;

        dictionary.put(1L, p);

        assertEquals(p, dictionary.get(1L));
    }

    @Test
    public void test_Double() {
        Dictionary<Long, Double> dictionary = newDictionary(Double.class);
        Double p = 10d;

        dictionary.put(1L, p);

        assertEquals(p, dictionary.get(1L));
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
