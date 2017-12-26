package com.hazelcast.dictionary;

import com.hazelcast.config.Config;
import com.hazelcast.config.DictionaryConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.Test;

import java.io.Serializable;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class SomeTest extends HazelcastTestSupport {


    @Test
    public void whenMultiFields() {
        Dictionary<Long, MultiField> dictionary = newDictionary(MultiField.class);
        dictionary.put(1L, new MultiField());

        assertEquals(1, dictionary.size());
    }

    @Test
    public void whenMultiNotNullFields() {
        Dictionary<Long, MultiField> dictionary = newDictionary(MultiField.class);
        MultiField value = new MultiField();
        value.field1 = 1L;
        value.field2 = 2L;
        value.field3 = 3L;
        value.field4 = 4L;
        dictionary.put(1L, value);

        assertEquals(1, dictionary.size());

        MultiField found = dictionary.get(1L);
        assertNotNull(found);
        assertEquals(value.field1, found.field1);
        assertEquals(value.field2, found.field2);
        assertEquals(value.field3, found.field3);
        assertEquals(value.field4, found.field4);
    }

    private <C> Dictionary<Long, C> newDictionary(Class<C> valueClass) {
        Config config = new Config();
        config.addDictionaryConfig(
                new DictionaryConfig("foo")
                        .setKeyClass(Long.class)
                        .setValueClass(valueClass));

        HazelcastInstance hz = createHazelcastInstance(config);
        return hz.getDictionary("foo");
    }

    public static class MultiField implements Serializable {

        public Long field1;
        public Long field2;
        public Long field3;
        public Long field4;


    }
}
