package com.hazelcast.nio.serialization.impl;


import com.hazelcast.config.Config;
import com.hazelcast.core.IMap;
import com.hazelcast.instance.HazelcastInstanceProxy;
import com.hazelcast.internal.serialization.impl.SerializationServiceV1;
import com.hazelcast.map.AbstractEntryProcessor;
import com.hazelcast.map.impl.LazyMapEntry;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.HazelcastSerializationException;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableFactory;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

import static com.hazelcast.nio.serialization.impl.DefaultPortableReaderTest.WheelPortable.w;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class DefaultPortableReaderTest extends HazelcastTestSupport {

    private static final CarPortable NON_EMPTY_PORSCHE = new CarPortable("Porsche", new EnginePortable(300),
            w("front", true), w("rear", true));

    private static final CarPortable PORSCHE = new CarPortable("Porsche", new EnginePortable(300),
            w("front", false), w("rear", false));

    private static final PrimitivePortable P = new PrimitivePortable(1);

    //
    //
    // ----------------------------------------------------------------
    //
    //

    @Test
    public void primitive_byte() throws IOException {
        assertThat(reader(P).readByte("byte_"), equalTo(P.byte_));
    }

    @Test
    public void primitive_short() throws IOException {
        assertThat(reader(P).readShort("short_"), equalTo(P.short_));
    }

    @Test
    public void primitive_int() throws IOException {
        assertThat(reader(P).readInt("int_"), equalTo(P.int_));
    }

    @Test
    public void primitive_long() throws IOException {
        assertThat(reader(P).readLong("long_"), equalTo(P.long_));
    }

    @Test
    public void primitive_float() throws IOException {
        assertThat(reader(P).readFloat("float_"), equalTo(P.float_));
    }

    @Test
    public void primitive_double() throws IOException {
        assertThat(reader(P).readDouble("double_"), equalTo(P.double_));
    }

    @Test
    public void primitive_boolean() throws IOException {
        assertThat(reader(P).readBoolean("boolean_"), equalTo(P.boolean_));
    }

    @Test
    public void primitive_char() throws IOException {
        assertThat(reader(P).readChar("char_"), equalTo(P.char_));
    }

    @Test
    public void primitive_utf() throws IOException {
        assertThat(reader(P).readUTF("string_"), equalTo(P.string_));
    }

    //
    //
    // ----------------------------------------------------------------
    //
    //

    @Test
    public void primitive_byte_array() throws IOException {
        assertThat(reader(P).readByteArray("bytes"), equalTo(P.bytes));
    }

    @Test
    public void primitive_byte_array_any() throws IOException {
        assertThat(reader(P).readByteArray("bytes[any]"), equalTo(P.bytes));
    }

    @Test
    public void primitive_byte_array_each() throws IOException {
        PortableReader reader = reader(P);
        for (int i = 0; i < P.bytes.length; i++) {
            assertThat(reader.readByte("bytes[" + i + "]"), equalTo(P.bytes[i]));
        }
    }

    @Test
    public void primitive_short_array() throws IOException {
        assertThat(reader(P).readShortArray("shorts"), equalTo(P.shorts));
    }

    @Test
    public void primitive_short_array_any() throws IOException {
        assertThat(reader(P).readShortArray("shorts[any]"), equalTo(P.shorts));
    }

    @Test
    public void primitive_short_array_each() throws IOException {
        PortableReader reader = reader(P);
        for (int i = 0; i < P.shorts.length; i++) {
            assertThat(reader.readShort("shorts[" + i + "]"), equalTo(P.shorts[i]));
        }
    }

    @Test
    public void primitive_int_array() throws IOException {
        assertThat(reader(P).readIntArray("ints"), equalTo(P.ints));
    }

    @Test
    public void primitive_int_array_any() throws IOException {
        assertThat(reader(P).readIntArray("ints[any]"), equalTo(P.ints));
    }

    @Test
    public void primitive_int_array_each() throws IOException {
        PortableReader reader = reader(P);
        for (int i = 0; i < P.ints.length; i++) {
            assertThat(reader.readInt("ints[" + i + "]"), equalTo(P.ints[i]));
        }
    }

    @Test
    public void primitive_long_array() throws IOException {
        assertThat(reader(P).readLongArray("longs"), equalTo(P.longs));
    }

    @Test
    public void primitive_long_array_any() throws IOException {
        assertThat(reader(P).readLongArray("longs[any]"), equalTo(P.longs));
    }

    @Test
    public void primitive_long_array_each() throws IOException {
        PortableReader reader = reader(P);
        for (int i = 0; i < P.ints.length; i++) {
            assertThat(reader.readLong("longs[" + i + "]"), equalTo(P.longs[i]));
        }
    }

    @Test
    public void primitive_float_array() throws IOException {
        assertThat(reader(P).readFloatArray("floats"), equalTo(P.floats));
    }

    @Test
    public void primitive_float_array_any() throws IOException {
        assertThat(reader(P).readFloatArray("floats[any]"), equalTo(P.floats));
    }

    @Test
    public void primitive_float_array_each() throws IOException {
        PortableReader reader = reader(P);
        for (int i = 0; i < P.floats.length; i++) {
            assertThat(reader.readFloat("floats[" + i + "]"), equalTo(P.floats[i]));
        }
    }

    @Test
    public void primitive_double_array() throws IOException {
        assertThat(reader(P).readDoubleArray("doubles"), equalTo(P.doubles));
    }

    @Test
    public void primitive_double_array_any() throws IOException {
        assertThat(reader(P).readDoubleArray("doubles[any]"), equalTo(P.doubles));
    }

    @Test
    public void primitive_double_array_each() throws IOException {
        PortableReader reader = reader(P);
        for (int i = 0; i < P.doubles.length; i++) {
            assertThat(reader.readDouble("doubles[" + i + "]"), equalTo(P.doubles[i]));
        }
    }

    @Test
    public void primitive_boolean_array() throws IOException {
        assertThat(reader(P).readBooleanArray("booleans"), equalTo(P.booleans));
    }

    @Test
    public void primitive_boolean_array_any() throws IOException {
        assertThat(reader(P).readBooleanArray("booleans[any]"), equalTo(P.booleans));
    }

    @Test
    public void primitive_boolean_array_each() throws IOException {
        PortableReader reader = reader(P);
        for (int i = 0; i < P.booleans.length; i++) {
            assertThat(reader.readBoolean("booleans[" + i + "]"), equalTo(P.booleans[i]));
        }
    }

    @Test
    public void primitive_char_array() throws IOException {
        assertThat(reader(P).readCharArray("chars"), equalTo(P.chars));
    }

    @Test
    public void primitive_char_array_any() throws IOException {
        assertThat(reader(P).readCharArray("chars[any]"), equalTo(P.chars));
    }

    @Test
    public void primitive_char_array_each() throws IOException {
        PortableReader reader = reader(P);
        for (int i = 0; i < P.chars.length; i++) {
            assertThat(reader.readChar("chars[" + i + "]"), equalTo(P.chars[i]));
        }
    }

    @Test
    public void primitive_utf_array() throws IOException {
        assertThat(reader(P).readUTFArray("strings"), equalTo(P.strings));
    }

    @Test
    public void primitive_utf_array_any() throws IOException {
        assertThat(reader(P).readUTFArray("strings[any]"), equalTo(P.strings));
    }

    @Test
    public void primitive_utf_array_each() throws IOException {
        PortableReader reader = reader(P);
        for (int i = 0; i < P.strings.length; i++) {
            assertThat(reader.readUTF("strings[" + i + "]"), equalTo(P.strings[i]));
        }
    }

    //
    //
    // ----------------------------------------------------------------
    //
    //


    @Test
    public void primitive() throws IOException {
        String expected = "Porsche";
        assertEquals(expected, reader(PORSCHE).readUTF("name"));
    }

    @Test
    public void nestedprimitive() throws IOException {
        int expected = 300;
        assertEquals(expected, reader(PORSCHE).readInt("engine.power"));
    }

    @Test
    public void portableAttribute() throws IOException {
        EnginePortable expected = PORSCHE.engine;
        assertEquals(expected, reader(PORSCHE).readPortable("engine"));
    }

    @Test
    public void nestedPortableAttribute() throws IOException {
        ChipPortable expected = PORSCHE.engine.chip;
        assertEquals(expected, reader(PORSCHE).readPortable("engine.chip"));
    }

    @Test
    public void primitiveArrayAtTheEnd_wholeArrayFetched() throws IOException {
        String[] expected = {"911", "GT"};
        assertArrayEquals(expected, reader(PORSCHE).readUTFArray("model"));
    }

    @Test
    public void primitiveArrayAtTheEnd_wholeArrayFetched_withAny() throws IOException {
        String[] expected = {"911", "GT"};
        assertArrayEquals(expected, reader(PORSCHE).readUTFArray("model[any]"));
    }

    @Test
    public void primitiveArrayAtTheEnd_oneElementFetched() throws IOException {
        String expected = "911";
        assertEquals(expected, reader(PORSCHE).readUTF("model[0]"));
    }

    @Test
    public void primitiveArrayAtTheEnd_lastElementFetched() throws IOException {
        String expected = "GT";
        assertEquals(expected, reader(PORSCHE).readUTF("model[1]"));
    }

    @Test
    public void portableArray_wholeArrayFetched() throws IOException {
        Portable[] expected = PORSCHE.wheels;
        assertArrayEquals(expected, reader(PORSCHE).readPortableArray("wheels"));
    }

    @Test
    public void portableArray_wholeArrayFetched_withAny() throws IOException {
        Portable[] expected = PORSCHE.wheels;
        assertArrayEquals(expected, reader(PORSCHE).readPortableArray("wheels[any]"));
    }

    @Test
    public void portableArrayAtTheEnd_oneElementFetched() throws IOException {
        Portable expected = PORSCHE.wheels[0];
        assertEquals(expected, reader(PORSCHE).readPortable("wheels[0]"));
    }

    @Test
    public void portableArrayAtTheEnd_lastElementFetched() throws IOException {
        Portable expected = PORSCHE.wheels[1];
        assertEquals(expected, reader(PORSCHE).readPortable("wheels[1]"));
    }

    @Test
    public void portableArrayFirst_primitiveAtTheEnd() throws IOException {
        String expected = "rear";
        assertEquals(expected, reader(PORSCHE).readUTF("wheels[1].name"));
    }

    @Test
    public void portableArrayFirst_portableAtTheEnd() throws IOException {
        ChipPortable expected = ((WheelPortable) PORSCHE.wheels[1]).chip;
        assertEquals(expected, reader(PORSCHE).readPortable("wheels[1].chip"));
    }

    @Test
    public void portableArrayFirst_portableArrayAtTheEnd_oneElementFetched() throws IOException {
        Portable expected = ((WheelPortable) PORSCHE.wheels[0]).chips[1];
        assertEquals(expected, reader(PORSCHE).readPortable("wheels[0].chips[1]"));
    }

    @Test
    public void portableArrayFirst_portableArrayAtTheEnd_wholeArrayFetched() throws IOException {
        Portable[] expected = ((WheelPortable) PORSCHE.wheels[0]).chips;
        assertArrayEquals(expected, reader(PORSCHE).readPortableArray("wheels[0].chips"));
    }

    @Test
    public void portableArrayFirst_portableArrayAtTheEnd_wholeArrayFetched_withAny() throws IOException {
        Portable[] expected = ((WheelPortable) PORSCHE.wheels[0]).chips;
        assertArrayEquals(expected, reader(PORSCHE).readPortableArray("wheels[0].chips[any]"));
    }

    @Test
    public void portableArrayFirst_portableArrayInTheMiddle_primitiveAtTheEnd() throws IOException {
        int expected = 20;
        assertEquals(expected, reader(PORSCHE).readInt("wheels[0].chips[0].power"));
    }

    @Test
    public void portableArrayFirst_primitiveArrayAtTheEnd() throws IOException {
        int expected = 12;
        assertEquals(expected, reader(PORSCHE).readInt("wheels[0].serial[1]"));
    }

    @Test(expected = HazelcastSerializationException.class)
    public void portableArrayFirst_primitiveArrayAtTheEnd2() throws IOException {
        reader(PORSCHE).readInt("wheels[0].serial[1].x");
    }

    @Test
    public void portableArrayFirst_primitiveArrayAtTheEnd_wholeArrayFetched() throws IOException {
        int[] expected = ((WheelPortable) PORSCHE.wheels[0]).serial;
        assertArrayEquals(expected, reader(PORSCHE).readIntArray("wheels[0].serial"));
    }

    @Test
    public void portableArrayFirst_primitiveArrayAtTheEnd_wholeArrayFetched_withAny() throws IOException {
        // TODO cleanup
        int[] result = reader(PORSCHE).readIntArray("wheels[0].serial[any]");
        int[] expected = ((WheelPortable) PORSCHE.wheels[0]).serial.clone();
        Arrays.sort(result);
        Arrays.sort(expected);

        assertArrayEquals(expected, result);
    }

    @Test
    public void portableArrayFirst_withAny_primitiveArrayAtTheEnd() throws IOException {
        int[] expected = {12, 12};
        assertArrayEquals(expected, reader(PORSCHE).readIntArray("wheels[any].serial[1]"));
    }

    @Test
    public void portableArrayFirst_withAny_primitiveArrayAtTheEnd2() throws IOException {
        Portable[] expected = new Portable[]{((WheelPortable) PORSCHE.wheels[0]).chip, ((WheelPortable) PORSCHE.wheels[1]).chip};
        assertArrayEquals(expected, reader(PORSCHE).readPortableArray("wheels[any].chip"));
    }

    @Test
    public void portableArrayFirst_withAny_primitiveArrayAtTheEnd3() throws IOException {
        Portable[] expected = new Portable[]{((WheelPortable) PORSCHE.wheels[0]).chips[1], ((WheelPortable) PORSCHE.wheels[1]).chips[1]};
        assertArrayEquals(expected, reader(PORSCHE).readPortableArray("wheels[any].chips[1]"));
    }

    @Test
    public void portableArrayFirst_withAny_primitiveArrayAtTheEnd5() throws IOException {
        String[] expected = {"front", "rear"};
        assertArrayEquals(expected, reader(PORSCHE).readUTFArray("wheels[any].name"));
    }

    @Test
    public void portableArrayFirst_withAny_primitiveArrayAtTheEnd6() throws IOException {
        int[] expected = {};
        assertArrayEquals(expected, reader(PORSCHE).readIntArray("wheels[1].emptyChips[any].power"));
    }

//    @Test
//    public void portableArrayFirst_withAny_primitiveArrayAtTheEnd7() throws IOException {
//        assertArrayEquals(null, reader(PORSCHE).readIntArray("wheels[1].nullChips[any].power"));
//    }

    @Test
    public void portableArrayFirst_withAny_primitiveArrayAtTheEnd8() throws IOException {
        Portable[] expected = {};
        assertArrayEquals(expected, reader(PORSCHE).readPortableArray("wheels[1].emptyChips[any]"));
    }

    @Test
    public void portableArrayFirst_withAny_primitiveArrayAtTheEnd8a() throws IOException {
        Portable[] expected = {};
        assertArrayEquals(expected, reader(PORSCHE).readPortableArray("wheels[any].emptyChips[any]"));
    }

    @Test
    public void portableArrayFirst_withAny_primitiveArrayAtTheEnd9() throws IOException {
        Portable[] expected = {};
        assertArrayEquals(expected, reader(PORSCHE).readPortableArray("wheels[1].emptyChips"));
    }

//    @Test
//    public void portableArrayFirst_withAny_primitiveArrayAtTheEnd10() throws IOException {
//        assertArrayEquals(null, reader(PORSCHE).readPortableArray("wheels[1].nullChips[any]"));
//    }
//
//    @Test
//    public void portableArrayFirst_withAny_primitiveArrayAtTheEnd11() throws IOException {
//        assertArrayEquals(null, reader(PORSCHE).readPortableArray("wheels[1].nullChips"));
//    }

    //
    // Utilities
    //
    public PortableReader reader(Portable portable) throws IOException {
        Config config = new Config();
        config.getSerializationConfig().addPortableFactory(TestPortableFactory.ID,
                new TestPortableFactory());

        HazelcastInstanceProxy hz = (HazelcastInstanceProxy) createHazelcastInstance(config);
        IMap map = hz.getMap("stealingMap");

        if (portable == PORSCHE) {
            // makes sure that proper class definitions are registered
            map.put(portable.toString(), NON_EMPTY_PORSCHE);
        }

        map.put(portable.toString(), portable);
        EntryStealingProcessor processor = new EntryStealingProcessor();
        map.executeOnEntries(processor);

        SerializationServiceV1 ss = (SerializationServiceV1) hz.getSerializationService();
        return ss.createPortableReader(processor.stolenEntryData);
    }

    public static class EntryStealingProcessor extends AbstractEntryProcessor {
        private Data stolenEntryData;

        public EntryStealingProcessor() {
            super(false);
        }

        public Object process(Map.Entry entry) {
            // Hack to get rid of de-serialization cost.
            // And assuming in-memory-format is BINARY, if it is OBJECT you can replace
            // the null check below with entry.getValue() != null
            // Works only for versions >= 3.6
            stolenEntryData = (Data) ((LazyMapEntry) entry).getValueData();
            return null;
        }
    }

    //
    // Test portable data structures
    //
    static class PrimitivePortable implements Portable {
        final static int FACTORY_ID = 1;
        final static int ID = 10;

        public PrimitivePortable(int seed) {
            byte_ = (byte) (seed + 10);
            short_ = (short) (seed + 20);
            int_ = seed + 30;
            long_ = seed + 40;
            float_ = seed + 50.01f;
            double_ = seed + 60.01d;
            boolean_ = seed % 2 == 0;
            char_ = (char) (seed + 'a');
            string_ = seed + 80 + "text";

            bytes = new byte[]{(byte) (seed + 11), (byte) (seed + 12), (byte) (seed + 13)};
            shorts = new short[]{(short) (seed + 21), (short) (seed + 22), (short) (seed + 23)};
            ints = new int[]{seed + 31, seed + 32, seed + 33};
            longs = new long[]{seed + 41, seed + 42, seed + 43};
            floats = new float[]{seed + 51.01f, seed + 52.01f, seed + 53.01f};
            doubles = new double[]{seed + 61.01f, seed + 62.01f, seed + 63.01f};
            booleans = new boolean[]{seed % 2 == 0, seed % 2 == 1, seed % 2 == 0};
            chars = new char[]{(char) (seed + 'b'), (char) (seed + 'c'), (char) (seed + 'd')};
            strings = new String[]{seed + 81 + "text", seed + 82 + "text", seed + 83 + "text"};
        }

        byte byte_;
        short short_;
        int int_;
        long long_;
        float float_;
        double double_;
        boolean boolean_;
        char char_;
        String string_;

        byte[] bytes;
        short[] shorts;
        int[] ints;
        long[] longs;
        float[] floats;
        double[] doubles;
        boolean[] booleans;
        char[] chars;
        String[] strings;

        public PrimitivePortable() {
        }

        public int getFactoryId() {
            return FACTORY_ID;
        }

        public int getClassId() {
            return ID;
        }

        public void writePortable(PortableWriter writer) throws IOException {
            writer.writeByte("byte_", byte_);
            writer.writeShort("short_", short_);
            writer.writeInt("int_", int_);
            writer.writeLong("long_", long_);
            writer.writeFloat("float_", float_);
            writer.writeDouble("double_", double_);
            writer.writeBoolean("boolean_", boolean_);
            writer.writeChar("char_", char_);
            writer.writeUTF("string_", string_);

            writer.writeByteArray("bytes", bytes);
            writer.writeShortArray("shorts", shorts);
            writer.writeIntArray("ints", ints);
            writer.writeLongArray("longs", longs);
            writer.writeFloatArray("floats", floats);
            writer.writeDoubleArray("doubles", doubles);
            writer.writeBooleanArray("booleans", booleans);
            writer.writeCharArray("chars", chars);
            writer.writeUTFArray("strings", strings);
        }

        public void readPortable(PortableReader reader) throws IOException {
            byte_ = reader.readByte("byte_");
            short_ = reader.readShort("short_");
            int_ = reader.readInt("int_");
            long_ = reader.readLong("long_");
            float_ = reader.readFloat("float_");
            double_ = reader.readDouble("double_");
            boolean_ = reader.readBoolean("boolean_");
            char_ = reader.readChar("char_");
            string_ = reader.readUTF("string_");

            bytes = reader.readByteArray("bytes");
            shorts = reader.readShortArray("shorts");
            ints = reader.readIntArray("ints");
            longs = reader.readLongArray("longs");
            floats = reader.readFloatArray("floats");
            doubles = reader.readDoubleArray("doubles");
            booleans = reader.readBooleanArray("booleans");
            chars = reader.readCharArray("chars");
            strings = reader.readUTFArray("strings");
        }
    }

    static class PortableGroup implements Portable {
        final static int FACTORY_ID = 1;
        final static int ID = 10;

        Portable[] portables;

        @Override
        public int getFactoryId() {
            return FACTORY_ID;
        }

        @Override
        public int getClassId() {
            return ID;
        }

        @Override
        public void writePortable(PortableWriter writer) throws IOException {
            writer.writePortableArray("portables", portables);
        }

        @Override
        public void readPortable(PortableReader reader) throws IOException {
            portables = reader.readPortableArray("portables");
        }
    }

    static class CarPortable implements Portable {
        final static int FACTORY_ID = 1;
        final static int ID = 5;

        public String name;
        public EnginePortable engine;
        public Portable[] wheels;

        public String[] model;

        public CarPortable(String name, EnginePortable engine, WheelPortable... wheels) {
            this.name = name;
            this.engine = engine;
            this.wheels = wheels;
            this.model = new String[]{"911", "GT"};
        }

        public CarPortable() {
        }

        public int getFactoryId() {
            return FACTORY_ID;
        }

        public int getClassId() {
            return ID;
        }

        public void writePortable(PortableWriter writer) throws IOException {
            writer.writeUTF("name", name);
            writer.writePortable("engine", engine);
            writer.writePortableArray("wheels", wheels);
            writer.writeUTFArray("model", model);
        }

        public void readPortable(PortableReader reader) throws IOException {
            name = reader.readUTF("name");
            engine = reader.readPortable("engine");
            wheels = reader.readPortableArray("wheels");
            model = reader.readUTFArray("model");
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            CarPortable that = (CarPortable) o;
            if (name != null ? !name.equals(that.name) : that.name != null) return false;
            return engine != null ? engine.equals(that.engine) : that.engine == null;

        }

        @Override
        public int hashCode() {
            int result = name != null ? name.hashCode() : 0;
            result = 31 * result + (engine != null ? engine.hashCode() : 0);
            return result;
        }
    }

    static class EnginePortable implements Portable, Comparable<EnginePortable> {
        final static int FACTORY_ID = 1;
        final static int ID = 8;
        public Integer power;
        public ChipPortable chip;

        public EnginePortable(int power) {
            this.power = power;
            this.chip = new ChipPortable();
        }

        public EnginePortable() {
            this.chip = new ChipPortable();
        }

        public int getFactoryId() {
            return FACTORY_ID;
        }

        public int getClassId() {
            return ID;
        }

        public void writePortable(PortableWriter writer) throws IOException {
            writer.writeInt("power", power);
            writer.writePortable("chip", chip);
        }

        public void readPortable(PortableReader reader) throws IOException {
            power = reader.readInt("power");
            chip = reader.readPortable("chip");
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            EnginePortable that = (EnginePortable) o;
            return power.equals(that.power);

        }

        @Override
        public int hashCode() {
            return power;
        }

        @Override
        public int compareTo(EnginePortable o) {
            return this.power.compareTo(o.power);
        }
    }

    static class ChipPortable implements Portable, Comparable<ChipPortable> {
        final static int FACTORY_ID = 1;
        final static int ID = 6;
        public Integer power;

        public ChipPortable(int power) {
            this.power = power;
        }

        public ChipPortable() {
            this.power = 15;
        }

        public int getFactoryId() {
            return FACTORY_ID;
        }

        public int getClassId() {
            return ID;
        }

        public void writePortable(PortableWriter writer) throws IOException {
            writer.writeInt("power", power);
        }

        public void readPortable(PortableReader reader) throws IOException {
            power = reader.readInt("power");
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ChipPortable that = (ChipPortable) o;
            return power.equals(that.power);

        }

        @Override
        public int hashCode() {
            return power;
        }

        @Override
        public int compareTo(ChipPortable o) {
            return this.power.compareTo(o.power);
        }
    }

    static class WheelPortable implements Portable, Comparable<WheelPortable> {
        final static int FACTORY_ID = 1;
        final static int ID = 7;
        public String name;
        public ChipPortable chip;
        public Portable chips[];
        public Portable emptyChips[];
        public Portable nullChips[];
        public int serial[];

        public WheelPortable(String name, boolean nonNull) {
            this.name = name;
            this.chip = new ChipPortable(100);
            this.chips = new Portable[]{new ChipPortable(20), new ChipPortable(40)};
            if (nonNull) {
                this.emptyChips = new Portable[]{new ChipPortable(20)};
                this.nullChips = new Portable[]{new ChipPortable(20)};
            } else {
                this.emptyChips = new Portable[]{};
                this.nullChips = null;
            }
            this.serial = new int[]{41, 12, 79, 18, 102};
        }

        public WheelPortable() {
        }

        public int getFactoryId() {
            return FACTORY_ID;
        }

        public int getClassId() {
            return ID;
        }

        public void writePortable(PortableWriter writer) throws IOException {
            writer.writeUTF("name", name);
            writer.writePortable("chip", chip);
            writer.writePortableArray("chips", chips);
            writer.writePortableArray("emptyChips", emptyChips);
            writer.writePortableArray("nullChips", nullChips);
            writer.writeIntArray("serial", serial);
        }

        public void readPortable(PortableReader reader) throws IOException {
            name = reader.readUTF("name");
            chip = reader.readPortable("chip");
            chips = reader.readPortableArray("chips");
            emptyChips = reader.readPortableArray("emptyChips");
            nullChips = reader.readPortableArray("nullChips");
            serial = reader.readIntArray("serial");
        }

        public static WheelPortable w(String name, boolean nonNull) {
            return new WheelPortable(name, nonNull);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            WheelPortable that = (WheelPortable) o;
            return name != null ? name.equals(that.name) : that.name == null;
        }

        @Override
        public int hashCode() {
            return name != null ? name.hashCode() : 0;
        }

        @Override
        public int compareTo(WheelPortable o) {
            return this.name.compareTo(o.name);
        }
    }

    static class TestPortableFactory implements PortableFactory {
        final static int ID = 1;

        @Override
        public Portable create(int classId) {
            if (CarPortable.ID == classId)
                return new CarPortable();
            else if (EnginePortable.ID == classId)
                return new EnginePortable();
            else if (WheelPortable.ID == classId)
                return new WheelPortable();
            else if (ChipPortable.ID == classId)
                return new ChipPortable();
            else
                return null;
        }
    }


}
