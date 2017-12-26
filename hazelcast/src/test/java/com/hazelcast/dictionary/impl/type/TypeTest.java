package com.hazelcast.dictionary.impl.type;

import com.hazelcast.dictionary.examples.BooleanReference;
import com.hazelcast.dictionary.examples.ByteReference;
import com.hazelcast.dictionary.examples.CharacterReference;
import com.hazelcast.dictionary.examples.ComplexPrimitiveRecord;
import com.hazelcast.dictionary.examples.DoubleReference;
import com.hazelcast.dictionary.examples.FloatReference;
import com.hazelcast.dictionary.examples.IntegerReference;
import com.hazelcast.dictionary.examples.LongReference;
import com.hazelcast.dictionary.examples.PBoolean;
import com.hazelcast.dictionary.examples.PBooleanArray;
import com.hazelcast.dictionary.examples.PByte;
import com.hazelcast.dictionary.examples.PByteArray;
import com.hazelcast.dictionary.examples.PChar;
import com.hazelcast.dictionary.examples.PCharArray;
import com.hazelcast.dictionary.examples.PDouble;
import com.hazelcast.dictionary.examples.PDoubleArray;
import com.hazelcast.dictionary.examples.PFloat;
import com.hazelcast.dictionary.examples.PFloatArray;
import com.hazelcast.dictionary.examples.PInt;
import com.hazelcast.dictionary.examples.PIntArray;
import com.hazelcast.dictionary.examples.PLong;
import com.hazelcast.dictionary.examples.PLongArray;
import com.hazelcast.dictionary.examples.PShort;
import com.hazelcast.dictionary.examples.PShortArray;
import com.hazelcast.dictionary.examples.ShortReference;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.Test;

import java.io.Serializable;

import static com.hazelcast.dictionary.impl.type.Kind.FIXED_LENGTH_RECORD;
import static com.hazelcast.dictionary.impl.type.Kind.PRIMITIVE_ARRAY;
import static com.hazelcast.dictionary.impl.type.Kind.STRING;
import static com.hazelcast.dictionary.impl.type.Kind.VARIABLE_LENGTH_RECORD;
import static com.hazelcast.nio.Bits.BOOLEAN_SIZE_IN_BYTES;
import static com.hazelcast.nio.Bits.LONG_SIZE_IN_BYTES;
import static org.junit.Assert.assertEquals;

public class TypeTest extends HazelcastTestSupport {

    @Test
    public void fixedLengthValue() {
        assertIsFixedLenth(true, Boolean.class);
        assertIsFixedLenth(true, Byte.class);
        assertIsFixedLenth(true, Character.class);
        assertIsFixedLenth(true, Short.class);
        assertIsFixedLenth(true, Integer.class);
        assertIsFixedLenth(true, Long.class);
        assertIsFixedLenth(true, Float.class);
        assertIsFixedLenth(true, Double.class);

        assertIsFixedLenth(true, PBoolean.class);
        assertIsFixedLenth(true, PByte.class);
        assertIsFixedLenth(true, PChar.class);
        assertIsFixedLenth(true, PShort.class);
        assertIsFixedLenth(true, PInt.class);
        assertIsFixedLenth(true, PLong.class);
        assertIsFixedLenth(true, PFloat.class);
        assertIsFixedLenth(true, PDouble.class);

        assertIsFixedLenth(true, BooleanReference.class);
        assertIsFixedLenth(true, ByteReference.class);
        assertIsFixedLenth(true, CharacterReference.class);
        assertIsFixedLenth(true, ShortReference.class);
        assertIsFixedLenth(true, IntegerReference.class);
        assertIsFixedLenth(true, LongReference.class);
        assertIsFixedLenth(true, FloatReference.class);
        assertIsFixedLenth(true, DoubleReference.class);

        assertIsFixedLenth(true, ComplexPrimitiveRecord.class);

        assertIsFixedLenth(false, PBooleanArray.class);
        assertIsFixedLenth(false, PByteArray.class);
        assertIsFixedLenth(false, PCharArray.class);
        assertIsFixedLenth(false, PShortArray.class);
        assertIsFixedLenth(false, PIntArray.class);
        assertIsFixedLenth(false, PLongArray.class);
        assertIsFixedLenth(false, PFloatArray.class);
        assertIsFixedLenth(false, PDoubleArray.class);
    }

    public void assertIsFixedLenth(boolean expected, Class clazz) {
        Type type = newType(clazz);
        assertEquals(expected, type.isFixedLength());
    }

    @Test
    public void testKind() {
        testKind(FIXED_LENGTH_RECORD, PInt.class);
        testKind(VARIABLE_LENGTH_RECORD, PIntArray.class);
        testKind(STRING, String.class);

        testKind(PRIMITIVE_ARRAY, boolean[].class);
        testKind(PRIMITIVE_ARRAY, boolean[].class);
        testKind(PRIMITIVE_ARRAY, short[].class);
        testKind(PRIMITIVE_ARRAY, char[].class);
        testKind(PRIMITIVE_ARRAY, int[].class);
        testKind(PRIMITIVE_ARRAY, long[].class);
        testKind(PRIMITIVE_ARRAY, float[].class);
        testKind(PRIMITIVE_ARRAY, double[].class);

        //testKind(Kind.);
    }

    public void testKind(Kind expectedKind, Class clazz) {
        Type type = newType(clazz);
        Kind actualKind = type.kind();
        assertEquals(expectedKind, actualKind);
    }

    @Test
    public void test_PLong() {
        Type type = newType(PLong.class);

        assertEquals(LONG_SIZE_IN_BYTES, type.fixedLength());
        assertEquals(FIXED_LENGTH_RECORD, type.kind());
    }

    @Test
    public void testTransientField() {
        Type type = newType(Transient.class);

        assertEquals(1, type.fixedLengthFields().size());
        assertEquals(LONG_SIZE_IN_BYTES, type.fixedLength());
    }

    @Test
    public void testStaticField() {
        Type type = newType(Static.class);

        assertEquals(1, type.fixedLengthFields().size());
        assertEquals(LONG_SIZE_IN_BYTES, type.fixedLength());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNoFields() {
        newType(NoFields.class);
    }

    @Test
    public void heapSize() {
        assertEquals(LONG_SIZE_IN_BYTES, newType(PLong.class).fixedLength());
        assertEquals(BOOLEAN_SIZE_IN_BYTES + LONG_SIZE_IN_BYTES, newType(LongReference.class).fixedLength());
    }

    private Type newType(Class valueType) {
        return new Type(valueType);
    }

    public static class Transient implements Serializable {
        public transient int transientField;
        public long regularField;
    }

    public static class Static implements Serializable {
        public static int staticField;
        public long regularField;
    }

    private static class NoFields implements Serializable {
    }
}
