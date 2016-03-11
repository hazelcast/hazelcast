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
import java.util.Map;

import static com.hazelcast.nio.serialization.impl.DefaultPortableReaderTest.WheelPortable.w;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class DefaultPortableReaderTest extends HazelcastTestSupport {

    private static final CarPortable PORSCHE = new CarPortable("Porsche", new EnginePortable(300), w("front"), w("rear"));

    @Test
    public void primitiveAttribute() throws IOException {
        assertEquals("Porsche", reader(PORSCHE).readUTF("name"));
    }

    @Test
    public void nestedPrimitiveAttribute() throws IOException {
        assertEquals(300, reader(PORSCHE).readInt("engine.power"));
    }

    @Test
    public void portableAttribute() throws IOException {
        assertEquals(PORSCHE.engine, reader(PORSCHE).readPortable("engine"));
    }

    @Test
    public void nestedPortableAttribute() throws IOException {
        assertEquals(PORSCHE.engine.chip, reader(PORSCHE).readPortable("engine.chip"));
    }

    @Test
    public void portableArrayInTheMiddle_primitiveAtTheEnd() throws IOException {
        assertEquals("front", reader(PORSCHE).readUTF("wheels[0].name"));
    }

    @Test
    public void portableArrayInTheMiddle_portableAtTheEnd() throws IOException {
        assertEquals(((WheelPortable) PORSCHE.wheels[0]).chip, reader(PORSCHE).readPortable("wheels[0].chip"));
    }

    @Test
    public void portableArrayInTheMiddle_portableArrayAtTheEnd_oneElementFetched() throws IOException {
        assertEquals(((WheelPortable) PORSCHE.wheels[0]).chips[1], reader(PORSCHE).readPortable("wheels[0].chips[1]"));
    }

    @Test
    public void portableArrayInTheMiddle_portableArrayAtTheEnd_wholeArrayFetched() throws IOException {
        assertArrayEquals(((WheelPortable) PORSCHE.wheels[0]).chips, reader(PORSCHE).readPortableArray("wheels[0].chips"));
    }

    @Test
    public void portableArrayFirst_portableArrayInTheMiddle_primitiveAttributeAtTheEnd() throws IOException {
        assertEquals(20, reader(PORSCHE).readInt("wheels[0].chips[0].power"));
    }

    @Test
    public void portableArrayFirst_primitiveArrayAtTheEnd() throws IOException {
        assertEquals(12, reader(PORSCHE).readInt("wheels[0].serial[1]"));
    }

    @Test(expected = HazelcastSerializationException.class)
    public void portableArrayFirst_primitiveArrayAtTheEnd2() throws IOException {
        reader(PORSCHE).readInt("wheels[0].serial[1].x");

    }

    @Test
    public void portableArrayFirst_primitiveArrayAtTheEnd_wholeArrayFetched() throws IOException {
        assertArrayEquals(((WheelPortable) PORSCHE.wheels[0]).serial, reader(PORSCHE).readIntArray("wheels[0].serial"));
    }

    @Test
    public void portableArrayAtTheEnd_oneElementFetched() throws IOException {
        assertEquals(PORSCHE.wheels[0], reader(PORSCHE).readPortable("wheels[0]"));
    }

    @Test
    public void portableArrayAtTheEnd_wholeArrayFetched() throws IOException {
        assertArrayEquals(PORSCHE.wheels, reader(PORSCHE).readPortableArray("wheels"));
    }

    @Test
    public void primitiveArrayAtTheEnd_oneElementFetched() throws IOException {
        assertEquals("911", reader(PORSCHE).readUTF("model[0]"));
    }

    @Test
    public void primitiveArrayAtTheEnd_wholeArrayFetched() throws IOException {
        assertArrayEquals(new String[]{"911", "GT"}, reader(PORSCHE).readUTFArray("model"));
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
    static class CarPortable implements Portable {
        final static int FACTORY_ID = 1;
        final static int ID = 5;

        public String name;
        public EnginePortable engine;
        public Portable[] wheels;
        public String[] model;

        public CarPortable(String name, EnginePortable engine) {
            this.name = name;
            this.engine = engine;
            this.wheels = new Portable[]{new WheelPortable("FL"), new WheelPortable("FR"),
                    new WheelPortable("RL"), new WheelPortable("RR")};
            this.model = new String[]{"911", "GT"};
        }

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
        public int serial[];

        public WheelPortable(String name) {
            this.name = name;
            this.chip = new ChipPortable(100);
            this.chips = new Portable[]{new ChipPortable(20), new ChipPortable(40)};
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
            writer.writeIntArray("serial", serial);
        }

        public void readPortable(PortableReader reader) throws IOException {
            name = reader.readUTF("name");
            chip = reader.readPortable("chip");
            chips = reader.readPortableArray("chips");
            serial = reader.readIntArray("serial");
        }

        public static WheelPortable w(String name) {
            return new WheelPortable(name);
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
