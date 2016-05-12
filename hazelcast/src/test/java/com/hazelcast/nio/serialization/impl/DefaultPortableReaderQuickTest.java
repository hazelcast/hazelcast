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
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.Map;

import static com.hazelcast.nio.serialization.impl.DefaultPortableReaderQuickTest.WheelPortable.w;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class DefaultPortableReaderQuickTest extends HazelcastTestSupport {

    @Rule
    public ExpectedException expected = ExpectedException.none();

    public static final CarPortable NON_EMPTY_PORSCHE = new CarPortable("Porsche", new EnginePortable(300),
            w("front", true), w("rear", true));

    public static final CarPortable PORSCHE = new CarPortable("Porsche", new EnginePortable(300),
            w("front", false), w("rear", false));


    @Test(expected = IllegalArgumentException.class)
    public void nullAttributeName() throws IOException {
        reader(PORSCHE).readPortableArray(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void emptyAttributeName() throws IOException {
        reader(PORSCHE).readPortableArray("");
    }

    @Test(expected = HazelcastSerializationException.class)
    public void wrongAttributeName_specialCharsNotTreatedSpecially() throws IOException {
        reader(PORSCHE).readPortableArray("-;',;");
    }

    @Test(expected = HazelcastSerializationException.class)
    public void wrongAttributeName() throws IOException {
        reader(PORSCHE).readPortableArray("wheelsss");
    }

    @Test(expected = HazelcastSerializationException.class)
    public void wrongNestedAttributeName() throws IOException {
        reader(PORSCHE).readPortableArray("wheels[0].seriall");
    }

    @Test(expected = IllegalArgumentException.class)
    public void wrongDotsExpression_middle() throws IOException {
        reader(PORSCHE).readIntArray("wheels[0]..serial");
    }

    @Test(expected = IllegalArgumentException.class)
    public void wrongDotsExpression_end() throws IOException {
        Portable a = reader(PORSCHE).readPortable("wheels[0].");
    }

    @Test(expected = IllegalArgumentException.class)
    public void wrongDotsExpression_end_tooMany() throws IOException {
        reader(PORSCHE).readPortable("wheels[0]...");
    }

    @Test(expected = IllegalArgumentException.class)
    public void wrongDotsExpression_beg() throws IOException {
        reader(PORSCHE).readPortable(".wheels[0]");
    }

    @Test(expected = IllegalArgumentException.class)
    public void wrongDotsExpression_beg_tooMany() throws IOException {
        reader(PORSCHE).readPortable("...wheels[0]");
    }

    @Test(expected = IllegalArgumentException.class)
    public void malformedQuantifier_leading() throws IOException {
        reader(PORSCHE).readPortable("wheels[");
    }

    @Test(expected = IllegalArgumentException.class)
    public void malformedQuantifier_middle() throws IOException {
        reader(PORSCHE).readPortable("wheels[0");
    }

    @Test(expected = IllegalArgumentException.class)
    public void malformedQuantifier_trailing() throws IOException {
        reader(PORSCHE).readPortable("wheels0]");
    }

    @Test(expected = IllegalArgumentException.class)
    public void malformedQuantifier_trailingNoNumber() throws IOException {
        reader(PORSCHE).readPortable("wheels]");
    }

    @Test(expected = IllegalArgumentException.class)
    public void malformedQuantifierNested_leading() throws IOException {
        reader(PORSCHE).readPortable("wheels[0].chips[");
    }

    @Test(expected = IllegalArgumentException.class)
    public void malformedQuantifierNested_middle() throws IOException {
        reader(PORSCHE).readPortable("wheels[0].chips[0");
    }

    @Test(expected = IllegalArgumentException.class)
    public void malformedQuantifierNested_trailing() throws IOException {
        reader(PORSCHE).readPortable("wheels[0].chips0]");
    }

    @Test(expected = IllegalArgumentException.class)
    public void malformedQuantifierNested_trailingNoNumber() throws IOException {
        reader(PORSCHE).readPortable("wheels[0].chips]");
    }

    @Test(expected = IllegalArgumentException.class)
    public void wrongMethodType() throws IOException {
        reader(PORSCHE).readPortable("wheels");
    }

    @Test
    public void primitive() throws IOException {
        String expected = "Porsche";
        assertEquals(expected, reader(PORSCHE).readUTF("name"));
    }

    @Test
    public void nestedPrimitive() throws IOException {
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
        int expected = 12 + 5;
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
        int[] expected = ((WheelPortable) PORSCHE.wheels[0]).serial;
        assertArrayEquals(expected, reader(PORSCHE).readIntArray("wheels[0].serial[any]"));
    }

    @Test
    public void portableArrayFirst_withAny_primitiveArrayAtTheEnd() throws IOException {
        int[] expected = {17, 16};
        assertArrayEquals(expected, reader(PORSCHE).readIntArray("wheels[any].serial[1]"));
    }

    @Test
    public void portableArrayFirst_withAny_primitiveArrayAtTheEnd2() throws IOException {
        Portable[] expected = new Portable[]{((WheelPortable) PORSCHE.wheels[0]).chip,
                ((WheelPortable) PORSCHE.wheels[1]).chip};
        assertArrayEquals(expected, reader(PORSCHE).readPortableArray("wheels[any].chip"));
    }

    @Test
    public void portableArrayFirst_withAny_primitiveArrayAtTheEnd3() throws IOException {
        Portable[] expected = new Portable[]{((WheelPortable) PORSCHE.wheels[0]).chips[1],
                ((WheelPortable) PORSCHE.wheels[1]).chips[1]};
        assertArrayEquals(expected, reader(PORSCHE).readPortableArray("wheels[any].chips[1]"));
    }

    @Test
    public void portableArrayFirst_withAny_primitiveArrayAtTheEnd5() throws IOException {
        String[] expected = {"front", "rear"};
        assertArrayEquals(expected, reader(PORSCHE).readUTFArray("wheels[any].name"));
    }

    @Test
    public void portableArrayFirst_withAny_primitiveArrayAtTheEnd6() throws IOException {
        assertNull(reader(PORSCHE).readIntArray("wheels[1].emptyChips[any].power"));
    }

    @Test
    public void portableArrayFirst_withAny_primitiveArrayAtTheEnd7() throws IOException {
        assertArrayEquals(null, reader(PORSCHE).readIntArray("wheels[1].nullChips[any].power"));
    }

    @Test
    public void portableArrayFirst_withAny_primitiveArrayAtTheEnd8() throws IOException {
        assertNull(reader(PORSCHE).readPortableArray("wheels[1].emptyChips[any]"));
    }

    @Test
    public void portableArrayFirst_withAny_primitiveArrayAtTheEnd8a() throws IOException {
        Portable[] expected = {null, null};
        assertArrayEquals(expected, reader(PORSCHE).readPortableArray("wheels[any].emptyChips[any]"));
    }

    @Test
    public void portableArrayFirst_withAny_primitiveArrayAtTheEnd9() throws IOException {
        Portable[] expected = {};
        assertArrayEquals(expected, reader(PORSCHE).readPortableArray("wheels[1].emptyChips"));
    }

    @Test
    public void portableArrayFirst_withAny_primitiveArrayAtTheEnd10() throws IOException {
        assertArrayEquals(null, reader(PORSCHE).readPortableArray("wheels[1].nullChips[any]"));
    }

    @Test
    public void portableArrayFirst_withAny_primitiveArrayAtTheEnd11() throws IOException {
        assertArrayEquals(null, reader(PORSCHE).readPortableArray("wheels[1].nullChips"));
    }

    @Test
    public void reusingTheReader_multipleCalls_stateResetCorreclty() throws IOException {
        PortableReader reader = reader(PORSCHE);
        assertEquals("rear", reader.readUTF("wheels[1].name"));
        assertEquals(300, reader.readInt("engine.power"));
        assertEquals(46, reader.readInt("wheels[0].serial[0]"));

        try {
            reader.readFloat("wheels[0].serial[0]");
            fail();
        } catch (Exception ex) {
        }

        assertEquals("front", reader.readUTF("wheels[0].name"));
        assertEquals(45, reader.readInt("wheels[1].serial[0]"));

        try {
            reader.readIntArray("name");
            fail();
        } catch (Exception ex) {
        }

        assertEquals(15, reader.readInt("engine.chip.power"));
        assertEquals("Porsche", reader.readUTF("name"));

    }

    //
    // Utilities
    //
    public PortableReader reader(Portable portable) throws IOException {
        Config config = new Config();
        config.getSerializationConfig().addPortableFactory(TestPortableFactory.ID,
                new TestPortableFactory());

        HazelcastInstanceProxy hz = (HazelcastInstanceProxy) createHazelcastInstance(config);
        IMap map = hz.getMap("stealingMap");

        if (portable instanceof CarPortable) {
            // makes sure that proper class definitions are registered
            map.put(NON_EMPTY_PORSCHE.toString(), NON_EMPTY_PORSCHE);
        }

        map.put(portable.toString(), portable);

        EntryStealingProcessor processor = new EntryStealingProcessor(portable.toString());
        map.executeOnEntries(processor);

        SerializationServiceV1 ss = (SerializationServiceV1) hz.getSerializationService();
        return ss.createPortableReader(processor.stolenEntryData);
    }

    public static class EntryStealingProcessor extends AbstractEntryProcessor {
        private final Object key;
        private Data stolenEntryData;

        public EntryStealingProcessor(String key) {
            super(false);
            this.key = key;
        }

        public Object process(Map.Entry entry) {
            // Hack to get rid of de-serialization cost.
            // And assuming in-memory-format is BINARY, if it is OBJECT you can replace
            // the null check below with entry.getValue() != null
            // Works only for versions >= 3.6
            if (key.equals(entry.getKey())) {
                stolenEntryData = (Data) ((LazyMapEntry) entry).getValueData();
            }
            return null;
        }
    }

    static class CarPortable implements Portable {
        final static int FACTORY_ID = 1;
        final static int ID = 5;

        public int power;
        public String name;
        public EnginePortable engine;
        public Portable[] wheels;

        public String[] model;

        public CarPortable(String name, EnginePortable engine, WheelPortable... wheels) {
            this.power = 100;
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
            writer.writeInt("power", power);
            writer.writeUTF("name", name);
            writer.writePortable("engine", engine);
            writer.writePortableArray("wheels", wheels);
            writer.writeUTFArray("model", model);
        }

        public void readPortable(PortableReader reader) throws IOException {
            power = reader.readInt("power");
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
            int nameLength = name.length();
            this.serial = new int[]{41 + nameLength, 12 + nameLength, 79 + nameLength, 18 + nameLength, 102 + nameLength};
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

    public static class TestPortableFactory implements PortableFactory {
        public final static int ID = 1;

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
