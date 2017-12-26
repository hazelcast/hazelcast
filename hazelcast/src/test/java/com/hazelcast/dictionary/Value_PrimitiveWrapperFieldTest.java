package com.hazelcast.dictionary;

import com.hazelcast.config.Config;
import com.hazelcast.config.DictionaryConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.dictionary.examples.BooleanReference;
import com.hazelcast.dictionary.examples.ByteReference;
import com.hazelcast.dictionary.examples.CharacterReference;
import com.hazelcast.dictionary.examples.DoubleReference;
import com.hazelcast.dictionary.examples.FloatReference;
import com.hazelcast.dictionary.examples.IntegerReference;
import com.hazelcast.dictionary.examples.LongReference;
import com.hazelcast.dictionary.examples.ShortReference;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class Value_PrimitiveWrapperFieldTest extends HazelcastTestSupport {

    @Test
    public void test_Boolean_whenTrue() {
        Dictionary<Long, BooleanReference> dictionary = newDictionary(BooleanReference.class);
        BooleanReference p = new BooleanReference();
        p.field = true;

        dictionary.put(1L, p);

        assertEquals(p, dictionary.get(1L));
    }

    @Test
    public void test_Boolean_whenFalse() {
        Dictionary<Long, BooleanReference> dictionary = newDictionary(BooleanReference.class);
        BooleanReference p = new BooleanReference();
        p.field = false;

        dictionary.put(1L, p);

        BooleanReference found = dictionary.get(1L);
        assertEquals(p, found);
    }

    @Test
    public void test_Boolean_whenNull() {
        Dictionary<Long, BooleanReference> dictionary = newDictionary(BooleanReference.class);
        BooleanReference p = new BooleanReference();

        dictionary.put(1L, p);

        assertEquals(p, dictionary.get(1L));
    }

    @Test
    public void test_Byte_whenNotNull() {
        Dictionary<Long, ByteReference> dictionary = newDictionary(ByteReference.class);
        ByteReference p = new ByteReference();
        p.field = 1;

        dictionary.put(1L, p);

        assertEquals(p, dictionary.get(1L));
    }

    @Test
    public void test_Byte_whenNull() {
        Dictionary<Long, ByteReference> dictionary = newDictionary(ByteReference.class);
        ByteReference p = new ByteReference();

        dictionary.put(1L, p);

        assertEquals(p, dictionary.get(1L));
    }

    @Test
    public void test_Character_whenNotNull() {
        Dictionary<Long, CharacterReference> dictionary = newDictionary(CharacterReference.class);
        CharacterReference p = new CharacterReference();
        p.field = 1;

        dictionary.put(1L, p);

        assertEquals(p, dictionary.get(1L));
    }

    @Test
    public void test_Character_whenNull() {
        Dictionary<Long, CharacterReference> dictionary = newDictionary(CharacterReference.class);
        CharacterReference p = new CharacterReference();

        dictionary.put(1L, p);

        assertEquals(p, dictionary.get(1L));
    }

    @Test
    public void test_Short_whenNotNull() {
        Dictionary<Long, ShortReference> dictionary = newDictionary(ShortReference.class);
        ShortReference p = new ShortReference();
        p.field = 1;

        dictionary.put(1L, p);

        assertEquals(p, dictionary.get(1L));
    }

    @Test
    public void test_Short_whenNull() {
        Dictionary<Long, ShortReference> dictionary = newDictionary(ShortReference.class);
        ShortReference p = new ShortReference();

        dictionary.put(1L, p);

        assertEquals(p, dictionary.get(1L));
    }

    @Test
    public void test_Integer_whenNotNull() {
        Dictionary<Long, IntegerReference> dictionary = newDictionary(IntegerReference.class);
        IntegerReference p = new IntegerReference();
        p.field = 1;

        dictionary.put(1L, p);

        assertEquals(p, dictionary.get(1L));
    }

    @Test
    public void test_Integer_whenNull() {
        Dictionary<Long, IntegerReference> dictionary = newDictionary(IntegerReference.class);
        IntegerReference p = new IntegerReference();

        dictionary.put(1L, p);

        assertEquals(p, dictionary.get(1L));
    }

    @Test
    public void test_Long_whenNotNull() {
        Dictionary<Long, LongReference> dictionary = newDictionary(LongReference.class);
        LongReference p = new LongReference();
        p.field = 1L;

        dictionary.put(1L, p);

        assertEquals(p, dictionary.get(1L));
    }

    @Test
    public void test_Long_whenNull() {
        Dictionary<Long, LongReference> dictionary = newDictionary(LongReference.class);
        LongReference p = new LongReference();

        dictionary.put(1L, p);

        assertEquals(p, dictionary.get(1L));
    }

    @Test
    public void test_Float_whenNotNull() {
        Dictionary<Long, FloatReference> dictionary = newDictionary(FloatReference.class);
        FloatReference p = new FloatReference();
        p.field = 1f;

        dictionary.put(1L, p);

        assertEquals(p, dictionary.get(1L));
    }

    @Test
    public void test_Float_whenNull() {
        Dictionary<Long, FloatReference> dictionary = newDictionary(FloatReference.class);
        FloatReference p = new FloatReference();

        dictionary.put(1L, p);

        assertEquals(p, dictionary.get(1L));
    }

    @Test
    public void test_Double_whenNotNull() {
        Dictionary<Long, DoubleReference> dictionary = newDictionary(DoubleReference.class);
        DoubleReference p = new DoubleReference();
        p.field = 1d;

        dictionary.put(1L, p);

        assertEquals(p, dictionary.get(1L));
    }

    @Test
    public void test_Double_whenNull() {
        Dictionary<Long, DoubleReference> dictionary = newDictionary(DoubleReference.class);
        DoubleReference p = new DoubleReference();

        dictionary.put(1L, p);

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
