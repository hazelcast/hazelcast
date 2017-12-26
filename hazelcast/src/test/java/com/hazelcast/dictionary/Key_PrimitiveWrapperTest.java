package com.hazelcast.dictionary;

import com.hazelcast.config.Config;
import com.hazelcast.config.DictionaryConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class Key_PrimitiveWrapperTest extends HazelcastTestSupport{

    @Test
    public void test_Boolean_whenTrue() {
        Dictionary<Boolean, Long> dictionary = newDictionary(Boolean.class);
        Boolean key = true;

        dictionary.put(key, 1L);

        assertEquals(new Long(1L), dictionary.get(key));
    }

    @Test
    public void test_Boolean_whenFalse() {
        Dictionary<Boolean, Long> dictionary = newDictionary(Boolean.class);
        Boolean key = false;

        dictionary.put(key, 1L);

        assertEquals(new Long(1L), dictionary.get(key));
    }

    @Test
    public void test_Byte() {
        Dictionary<Byte, Long> dictionary = newDictionary(Byte.class);
        Byte key = 100;

        dictionary.put(key, 1L);

        assertEquals(new Long(1L), dictionary.get(key));
    }

    @Test
    public void test_Character() {
        Dictionary<Character, Long> dictionary = newDictionary(Character.class);
        Character key = 100;

        dictionary.put(key, 1L);

        assertEquals(new Long(1L), dictionary.get(key));
    }

    @Test
    public void test_Short() {
        Dictionary<Short, Long> dictionary = newDictionary(Short.class);
        Short key = 100;

        dictionary.put(key, 1L);

        assertEquals(new Long(1L), dictionary.get(key));
    }

    @Test
    public void test_Integer() {
        Dictionary<Integer, Long> dictionary = newDictionary(Integer.class);
        Integer key = 100;

        dictionary.put(key, 1L);

        assertEquals(new Long(1L), dictionary.get(key));
    }

    @Test
    public void test_Long() {
        Dictionary<Long, Long> dictionary = newDictionary(Long.class);
        Long key = 100L;

        dictionary.put(key, 1L);

        assertEquals(new Long(1L), dictionary.get(key));
    }

    @Test
    public void test_Float() {
        Dictionary<Float, Long> dictionary = newDictionary(Float.class);
        Float key = 100f;

        dictionary.put(key, 1L);

        assertEquals(new Long(1L), dictionary.get(key));
    }

    @Test
    public void test_Double() {
        Dictionary<Double, Long> dictionary = newDictionary(Double.class);
        Double key = 100d;

        dictionary.put(key, 1L);

        assertEquals(new Long(1L), dictionary.get(key));
    }

    private <C> Dictionary<C, Long> newDictionary(Class<C> keyClass) {
        Config config = new Config()
                .setProperty(GroupProperty.PARTITION_COUNT.getName(), "1");
        config.addDictionaryConfig(
                new DictionaryConfig("foo")
                        .setSegmentsPerPartition(1)
                        .setKeyClass(keyClass)
                        .setValueClass(Long.class));

        HazelcastInstance hz = createHazelcastInstance(config);
        return hz.getDictionary("foo");
    }
}
