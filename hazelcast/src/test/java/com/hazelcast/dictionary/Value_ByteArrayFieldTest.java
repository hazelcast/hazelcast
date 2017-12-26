package com.hazelcast.dictionary;

import com.hazelcast.config.Config;
import com.hazelcast.config.DictionaryConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.dictionary.examples.PByteArray;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class Value_ByteArrayFieldTest extends HazelcastTestSupport {

    @Test
    public void whenNullBytes() {
        Dictionary<Long, PByteArray> dictionary = newDictionary();
        PByteArray value = new PByteArray();
        dictionary.put(1L, value);

        assertEquals(1, dictionary.size());
        assertEquals(value, dictionary.get(1L));
    }

    @Test
    public void whenEmptyArray() {
        Dictionary<Long, PByteArray> dictionary = newDictionary();
        PByteArray value = new PByteArray(new byte[0]);
        dictionary.put(1L, value);

        assertEquals(1, dictionary.size());
        assertEquals(value, dictionary.get(1L));
    }

    @Test
    public void whenNonEmpty() {
        Dictionary<Long, PByteArray> dictionary = newDictionary();
        PByteArray value = new PByteArray(new byte[]{1, 2, 3, 4, 5});
        dictionary.put(1L, value);

        assertEquals(1, dictionary.size());
        assertEquals(value, dictionary.get(1L));
    }

    private Dictionary<Long, PByteArray> newDictionary() {
        Config config = new Config();
        config.addDictionaryConfig(
                new DictionaryConfig("foo")
                        .setKeyClass(Long.class)
                        .setValueClass(PByteArray.class));

        HazelcastInstance hz = createHazelcastInstance(config);
        return hz.getDictionary("foo");
    }

}
