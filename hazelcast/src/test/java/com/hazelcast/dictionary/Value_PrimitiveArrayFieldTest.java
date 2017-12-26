package com.hazelcast.dictionary;

import com.hazelcast.config.Config;
import com.hazelcast.config.DictionaryConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.dictionary.examples.PByteArray;
import com.hazelcast.dictionary.examples.PLongArray;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class Value_PrimitiveArrayFieldTest extends HazelcastTestSupport {

    @Test
    public void test_byteArray_whenNull() {
        Dictionary<Long, PByteArray> dictionary = newDictionary(PByteArray.class);
        PByteArray p = new PByteArray();

        dictionary.put(1L, p);

        assertEquals(1, dictionary.size());
        assertEquals(p, dictionary.get(1L));
    }

    @Test
    public void test_byteArray_whenEmpty() {
        Dictionary<Long, PByteArray> dictionary = newDictionary(PByteArray.class);
        PByteArray p = new PByteArray();
        p.field = new byte[0];

        dictionary.put(1L, p);

        assertEquals(1, dictionary.size());
        assertEquals(p, dictionary.get(1L));
    }

    @Test
    public void test_byteArray_whenNotEmpty() {
        Dictionary<Long, PByteArray> dictionary = newDictionary(PByteArray.class);
        PByteArray p = new PByteArray();
        p.field = new byte[]{1, 2, 3, 4};

        dictionary.put(1L, p);

        assertEquals(1, dictionary.size());
        assertEquals(p, dictionary.get(1L));
    }

    @Test
    public void test_longArray_whenNull() {
        Dictionary<Long, PLongArray> dictionary = newDictionary(PLongArray.class);
        PLongArray p = new PLongArray();

        dictionary.put(1L, p);

        assertEquals(1, dictionary.size());
        assertEquals(p, dictionary.get(1L));
    }

    @Test
    public void test_longArray_whenEmpty() {
        Dictionary<Long, PLongArray> dictionary = newDictionary(PLongArray.class);
        PLongArray p = new PLongArray();
        p.field = new long[0];

        dictionary.put(1L, p);

        assertEquals(1, dictionary.size());
        assertEquals(p, dictionary.get(1L));
    }

    @Test
    public void test_longArray_whenNotEmpty() {
        Dictionary<Long, PLongArray> dictionary = newDictionary(PLongArray.class);
        PLongArray p = new PLongArray();
        p.field = new long[]{1, 2, 3, 4};

        dictionary.put(1L, p);

        assertEquals(1, dictionary.size());
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
