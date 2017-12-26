package com.hazelcast.dictionary;

import com.hazelcast.config.Config;
import com.hazelcast.config.DictionaryConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class PutIfAbsentTest  extends HazelcastTestSupport {

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
    public void whenNullKey(){
        Dictionary<Long, Long> dictionary = newDictionary();

        dictionary.putIfAbsent(null,1L);
    }

    @Test(expected = NullPointerException.class)
    public void whenNullValue(){
        Dictionary<Long, Long> dictionary = newDictionary();

        dictionary.putIfAbsent(1L,null);
    }

    @Test
    public void whenNonExisting_thenInsert() {
        Dictionary<Long, Long> dictionary = newDictionary();

        boolean result  = dictionary.putIfAbsent(1L,2L);

        assertTrue(result);
        assertEquals(1,dictionary.size());
        assertEquals(new Long(2L), dictionary.get(1L));
    }

    @Test
    public void whenExisting_thenNotInsert() {
        Dictionary<Long, Long> dictionary = newDictionary();
        dictionary.put(1L, 2L);

        boolean result  = dictionary.putIfAbsent(1L,3L);

        assertFalse(result);
        assertEquals(1,dictionary.size());
        assertEquals(new Long(2L), dictionary.get(1L));
    }
}