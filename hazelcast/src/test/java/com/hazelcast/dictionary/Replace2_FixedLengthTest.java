package com.hazelcast.dictionary;

import com.hazelcast.config.Config;
import com.hazelcast.config.DictionaryConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class Replace2_FixedLengthTest extends HazelcastTestSupport {

    private Dictionary<Long, Long> createDictionary() {
        Config config = new Config()
                .setProperty(GroupProperty.PARTITION_COUNT.getName(),"1");
        config.addDictionaryConfig(
                new DictionaryConfig("foo")
                        .setSegmentsPerPartition(1)
                        .setKeyClass(Long.class)
                        .setValueClass(Long.class));

        HazelcastInstance hz = createHazelcastInstance(config);

        return hz.getDictionary("foo");
    }

    @Test(expected = NullPointerException.class)
    public void whenKeyNull(){
        Dictionary<Long, Long> dictionary = createDictionary();
        dictionary.replace(null,1L);
    }

    @Test(expected = NullPointerException.class)
    public void whenValueNull(){
        Dictionary<Long, Long> dictionary = createDictionary();
        dictionary.replace(1L,null);
    }

    @Test
    public void whenUntouched() {
        Dictionary<Long, Long> dictionary = createDictionary();

        boolean result = dictionary.replace(1L,2L);

        assertFalse(result);
        assertNull(dictionary.get(1L));
        assertEquals(0,dictionary.size());
    }
    @Test
    public void whenTouched_butNoExistingValue() {
        Dictionary<Long, Long> dictionary = createDictionary();
        dictionary.put(100L,200L);

        boolean result = dictionary.replace(1L,2L);

        assertFalse(result);
        assertNull(dictionary.get(1L));
        assertEquals(1,dictionary.size());
    }

    @Test
    public void whenExistingValue() {
        Dictionary<Long, Long> dictionary = createDictionary();

        dictionary.put(1L,10L);
        boolean result = dictionary.replace(1L,20L);

        assertTrue(result);
        assertEquals(new Long(20L), dictionary.get(1L));
        assertEquals(1,dictionary.size());
    }
}
