package com.hazelcast.dictionary;

import com.hazelcast.config.Config;
import com.hazelcast.config.DictionaryConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class Value_PrimitiveArrayTest extends HazelcastTestSupport {

    @Test
    public void test_booleanArray_whenEmpty() {
        Dictionary<Long, boolean[]> dictionary = newDictionary(boolean[].class);
        boolean[] value = new boolean[0];

        dictionary.put(1L, value);

        assertEquals(1, dictionary.size());
        assertArrayEquals(value, dictionary.get(1L));
    }

    @Test
    public void test_booleanArray_whenNotEmpty() {
        Dictionary<Long, boolean[]> dictionary = newDictionary(boolean[].class);
        boolean[] value = new boolean[]{true, true, true, false, true, false};

        dictionary.put(1L, value);

        assertEquals(1, dictionary.size());
        boolean[] actual = dictionary.get(1L);
        assertArrayEquals(value, actual);
    }

    @Test
    public void test_byteArray_whenEmpty() {
        Dictionary<Long, byte[]> dictionary = newDictionary(byte[].class);
        byte[] value = new byte[0];

        dictionary.put(1L, value);

        assertEquals(1, dictionary.size());
        assertArrayEquals(value, dictionary.get(1L));
    }

    @Test
    public void test_byteArray_whenNotEmpty() {
        Dictionary<Long, byte[]> dictionary = newDictionary(byte[].class);
        byte[] value = new byte[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10};

        dictionary.put(1L, value);

        assertEquals(1, dictionary.size());
        byte[] actual = dictionary.get(1L);
        assertArrayEquals(value, actual);
    }

    @Test
    public void test_shortArray_whenEmpty() {
        Dictionary<Long, short[]> dictionary = newDictionary(short[].class);
        short[] value = new short[0];

        dictionary.put(1L, value);

        assertEquals(1, dictionary.size());
        assertArrayEquals(value, dictionary.get(1L));
    }

    @Test
    public void test_shortArray_whenNotEmpty() {
        Dictionary<Long, short[]> dictionary = newDictionary(short[].class);
        short[] value = new short[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10};

        dictionary.put(1L, value);

        assertEquals(1, dictionary.size());
        short[] actual = dictionary.get(1L);
        assertArrayEquals(value, actual);
    }

    @Test
    public void test_charArray_whenEmpty() {
        Dictionary<Long, char[]> dictionary = newDictionary(char[].class);
        char[] value = new char[0];

        dictionary.put(1L, value);

        assertEquals(1, dictionary.size());
        assertArrayEquals(value, dictionary.get(1L));
    }

    @Test
    public void test_charArray_whenNotEmpty() {
        Dictionary<Long, char[]> dictionary = newDictionary(char[].class);
        char[] value = new char[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10};

        dictionary.put(1L, value);

        assertEquals(1, dictionary.size());
        char[] actual = dictionary.get(1L);
        assertArrayEquals(value, actual);
    }

    @Test
    public void test_intArray_whenEmpty() {
        Dictionary<Long, int[]> dictionary = newDictionary(int[].class);
        int[] value = new int[0];

        dictionary.put(1L, value);

        assertEquals(1, dictionary.size());
        assertArrayEquals(value, dictionary.get(1L));
    }

    @Test
    public void test_intArray_whenNotEmpty() {
        Dictionary<Long, int[]> dictionary = newDictionary(int[].class);
        int[] value = new int[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10};

        dictionary.put(1L, value);

        assertEquals(1, dictionary.size());
        int[] actual = dictionary.get(1L);
        assertArrayEquals(value, actual);
    }


    @Test
    public void test_longArray_whenEmpty() {
        Dictionary<Long, long[]> dictionary = newDictionary(long[].class);
        long[] value = new long[0];

        dictionary.put(1L, value);

        assertEquals(1, dictionary.size());
        assertArrayEquals(value, dictionary.get(1L));
    }

    @Test
    public void test_longArray_whenNotEmpty() {
        Dictionary<Long, long[]> dictionary = newDictionary(long[].class);
        long[] value = new long[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10};

        dictionary.put(1L, value);

        assertEquals(1, dictionary.size());
        long[] actual = dictionary.get(1L);
        assertArrayEquals(value, actual);
    }

    @Test
    public void test_floatArray_whenEmpty() {
        Dictionary<Long, float[]> dictionary = newDictionary(float[].class);
        float[] value = new float[0];

        dictionary.put(1L, value);

        assertEquals(1, dictionary.size());
        assertArrayEquals(value, dictionary.get(1L), 0.01f);
    }

    @Test
    public void test_floatArray_whenNotEmpty() {
        Dictionary<Long, float[]> dictionary = newDictionary(float[].class);
        float[] value = new float[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10};

        dictionary.put(1L, value);

        assertEquals(1, dictionary.size());
        float[] actual = dictionary.get(1L);
        assertArrayEquals(value, actual, 0.01f);
    }

    @Test
    public void test_doubleArray_whenEmpty() {
        Dictionary<Long, double[]> dictionary = newDictionary(double[].class);
        double[] value = new double[0];

        dictionary.put(1L, value);

        assertEquals(1, dictionary.size());
        assertArrayEquals(value, dictionary.get(1L), 0.01f);
    }

    @Test
    public void test_doubleArray_whenNotEmpty() {
        Dictionary<Long, double[]> dictionary = newDictionary(double[].class);
        double[] value = new double[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10};

        dictionary.put(1L, value);

        assertEquals(1, dictionary.size());
        double[] actual = dictionary.get(1L);
        assertArrayEquals(value, actual, 0.01f);
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
