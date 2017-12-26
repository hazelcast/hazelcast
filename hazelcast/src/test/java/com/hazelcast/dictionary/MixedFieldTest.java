package com.hazelcast.dictionary;

import com.hazelcast.config.Config;
import com.hazelcast.config.DictionaryConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.Test;

import java.io.Serializable;

import static org.junit.Assert.assertEquals;

public class MixedFieldTest extends HazelcastTestSupport {


    @Test
    public void test() {
        Dictionary<Long, MixedFields> dictionary = newDictionary(MixedFields.class);
        MixedFields value = new MixedFields();
        value.longField1 = 1L;
        value.integerFIeld = 2;
        dictionary.put(1L, value);

        assertEquals(1, dictionary.size());
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


    public static class MixedFields implements Serializable {
        public long longField1;
        public int intFIeld2;
        public boolean booleanField;
        public byte byteField;
        public float floatField;
        public char charField;
        public double doubleField;
        public short shortField;

        public Long LongFIeld;
        public Integer integerFIeld;

    }
}