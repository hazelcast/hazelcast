package com.hazelcast.dictionary;

import com.hazelcast.config.Config;
import com.hazelcast.config.DictionaryConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.dictionary.examples.PBoolean;
import com.hazelcast.dictionary.examples.PByte;
import com.hazelcast.dictionary.examples.PChar;
import com.hazelcast.dictionary.examples.PDouble;
import com.hazelcast.dictionary.examples.PFloat;
import com.hazelcast.dictionary.examples.PInt;
import com.hazelcast.dictionary.examples.PLong;
import com.hazelcast.dictionary.examples.PShort;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class Value_PrimitiveFieldTest extends HazelcastTestSupport {

    @Test
    public void test_boolean_true() {
        Dictionary<Long, PBoolean> dictionary = newDictionary(PBoolean.class);
        PBoolean p = new PBoolean();
        p.field = true;

        dictionary.put(1L, p);

        assertEquals(1, dictionary.size());
        assertEquals(p, dictionary.get(1L));
    }

    @Test
    public void test_boolean_false() {
        Dictionary<Long, PBoolean> dictionary = newDictionary(PBoolean.class);
        PBoolean p = new PBoolean();
        p.field = false;

        dictionary.put(1L, p);

        assertEquals(1, dictionary.size());
        assertEquals(p, dictionary.get(1L));
    }

    @Test
    public void test_byte() {
        Dictionary<Long, PByte> dictionary = newDictionary(PByte.class);
        PByte p = new PByte();
        p.field = 1;

        dictionary.put(1L, p);

        assertEquals(1, dictionary.size());
        assertEquals(p, dictionary.get(1L));
    }

    @Test
    public void test_char() {
        Dictionary<Long, PChar> dictionary = newDictionary(PChar.class);
        PChar p = new PChar();
        p.field = 1;

        dictionary.put(1L, p);

        assertEquals(1, dictionary.size());
        assertEquals(p, dictionary.get(1L));
    }


    @Test
    public void test_short() {
        Dictionary<Long, PShort> dictionary = newDictionary(PShort.class);
        PShort p = new PShort();
        p.field = 1;

        dictionary.put(1L, p);

        assertEquals(1, dictionary.size());
        assertEquals(p, dictionary.get(1L));
    }

    @Test
    public void test_int() {
        Dictionary<Long, PInt> dictionary = newDictionary(PInt.class);
        PInt p = new PInt();
        p.field = 1;

        dictionary.put(1L, p);

        assertEquals(1, dictionary.size());
        assertEquals(p, dictionary.get(1L));
    }

    @Test
    public void test_long() {
        Dictionary<Long, PLong> dictionary = newDictionary(PLong.class);
        PLong p = new PLong();
        p.field = 1L;

        dictionary.put(1L, p);

        assertEquals(1, dictionary.size());
        assertEquals(p, dictionary.get(1L));
    }

    @Test
    public void test_float() {
        Dictionary<Long, PFloat> dictionary = newDictionary(PFloat.class);
        PFloat p = new PFloat();
        p.field = 1;

        dictionary.put(1L, p);

        assertEquals(1, dictionary.size());
        assertEquals(p, dictionary.get(1L));
    }

    @Test
    public void test_double() {
        Dictionary<Long, PDouble> dictionary = newDictionary(PDouble.class);
        PDouble p = new PDouble();
        p.field = 1;

        dictionary.put(1L, p);

        assertEquals(1, dictionary.size());
        assertEquals(p, dictionary.get(1L));
    }

    private <C> Dictionary<Long, C> newDictionary(Class<C> valueClass) {
        Config config = new Config()
                .setProperty(GroupProperty.PARTITION_COUNT.getName(),"1");
        config.addDictionaryConfig(
                new DictionaryConfig("foo")
                        .setSegmentsPerPartition(1)
                        .setKeyClass(Long.class)
                        .setValueClass(valueClass));

        HazelcastInstance hz = createHazelcastInstance(config);
        return hz.getDictionary("foo");
    }

}
