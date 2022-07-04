/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.internal.serialization.impl.portable.portablereader;

import com.hazelcast.config.SerializationConfig;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.internal.serialization.impl.GenericRecordQueryReader;
import com.hazelcast.nio.serialization.HazelcastSerializationException;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableFactory;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.query.impl.getters.MultiResult;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.hazelcast.internal.serialization.impl.portable.portablereader.DefaultPortableReaderQuickTest.WheelPortable.w;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class DefaultPortableReaderQuickTest extends HazelcastTestSupport {

    static final CarPortable NON_EMPTY_PORSCHE = new CarPortable("Porsche", new EnginePortable(300),
            w("front", true), w("rear", true));

    static final CarPortable PORSCHE = new CarPortable("Porsche", new EnginePortable(300),
            w("front", false), w("rear", false));

    @Test(expected = IllegalArgumentException.class)
    public void nullAttributeName() throws IOException {
        reader(PORSCHE).read(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void emptyAttributeName() throws IOException {
        reader(PORSCHE).read("");
    }

    @Test
    public void wrongAttributeName_specialCharsNotTreatedSpecially() throws IOException {
        assertNull(reader(PORSCHE).read("-;',;"));
    }

    @Test
    public void wrongAttributeName() throws IOException {
        assertNull(reader(PORSCHE).read("wheelsss"));
    }

    @Test
    public void wrongNestedAttributeName() throws IOException {
        assertNull(reader(PORSCHE).read("wheels[0].seriall"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void wrongDotsExpression_middle() throws IOException {
        reader(PORSCHE).read("wheels[0]..serial");
    }

    @Test(expected = IllegalArgumentException.class)
    public void wrongDotsExpression_end() throws IOException {
        reader(PORSCHE).read("wheels[0].");
    }

    @Test(expected = IllegalArgumentException.class)
    public void wrongDotsExpression_end_tooMany() throws IOException {
        reader(PORSCHE).read("wheels[0]...");
    }

    @Test(expected = IllegalArgumentException.class)
    public void wrongDotsExpression_beg() throws IOException {
        reader(PORSCHE).read(".wheels[0]");
    }

    @Test(expected = IllegalArgumentException.class)
    public void wrongDotsExpression_beg_tooMany() throws IOException {
        reader(PORSCHE).read("...wheels[0]");
    }

    @Test(expected = IllegalArgumentException.class)
    public void malformedQuantifier_leading() throws IOException {
        reader(PORSCHE).read("wheels[");
    }

    @Test(expected = IllegalArgumentException.class)
    public void malformedQuantifier_middle() throws IOException {
        reader(PORSCHE).read("wheels[0");
    }

    @Test(expected = IllegalArgumentException.class)
    public void malformedQuantifier_trailing() throws IOException {
        reader(PORSCHE).read("wheels0]");
    }

    @Test(expected = IllegalArgumentException.class)
    public void malformedQuantifier_trailingNoNumber() throws IOException {
        reader(PORSCHE).read("wheels]");
    }

    @Test(expected = IllegalArgumentException.class)
    public void malformedQuantifierNested_leading() throws IOException {
        reader(PORSCHE).read("wheels[0].chips[");
    }

    @Test(expected = IllegalArgumentException.class)
    public void malformedQuantifierNested_middle() throws IOException {
        reader(PORSCHE).read("wheels[0].chips[0");
    }

    @Test(expected = IllegalArgumentException.class)
    public void malformedQuantifierNested_trailing() throws IOException {
        reader(PORSCHE).read("wheels[0].chips0]");
    }

    @Test(expected = IllegalArgumentException.class)
    public void malformedQuantifierNested_trailingNoNumber() throws IOException {
        reader(PORSCHE).read("wheels[0].chips]");
    }

    @Test
    public void primitive() throws IOException {
        String expected = "Porsche";
        Assert.assertEquals(expected, reader(PORSCHE).read("name"));
    }

    @Test
    public void nestedPrimitive() throws IOException {
        int expected = 300;
        Assert.assertEquals(expected, reader(PORSCHE).read("engine.power"));
    }

    @Test
    public void nestedPortableAttribute() throws IOException {
        int expected = PORSCHE.engine.chip.power;
        assertEquals(expected, reader(PORSCHE).read("engine.chip.power"));
    }

    @Test
    public void primitiveArrayAtTheEnd_wholeArrayFetched() throws IOException {
        String[] expected = {"911", "GT"};
        assertArrayEquals(expected, (Object[]) reader(PORSCHE).read("model"));
    }

    @Test
    public void primitiveArrayAtTheEnd_wholeArrayFetched_withAny() throws IOException {
        String[] expected = {"911", "GT"};
        assertCollection(Arrays.asList(expected), ((MultiResult) reader(PORSCHE).read("model[any]")).getResults());
    }

    @Test
    public void primitiveArrayAtTheEnd_oneElementFetched() throws IOException {
        String expected = "911";
        Assert.assertEquals(expected, reader(PORSCHE).read("model[0]"));
    }

    @Test
    public void primitiveArrayAtTheEnd_lastElementFetched() throws IOException {
        String expected = "GT";
        Assert.assertEquals(expected, reader(PORSCHE).read("model[1]"));
    }

    @Test
    public void portableArray_wholeArrayFetched_withAny() throws IOException {
        WheelPortable[] wheels = (WheelPortable[]) PORSCHE.wheels;
        Object[] expected = Arrays.asList(wheels).stream().map(portable -> portable.name).toArray();
        assertCollection(Arrays.asList(expected), ((MultiResult) reader(PORSCHE).read("wheels[any].name")).getResults());
    }

    @Test
    public void portableArrayAtTheEnd_oneElementFetched() throws IOException {
        String expected = ((WheelPortable) PORSCHE.wheels[0]).name;
        assertEquals(expected, reader(PORSCHE).read("wheels[0].name"));
    }

    @Test
    public void portableArrayAtTheEnd_lastElementFetched() throws IOException {
        String expected = ((WheelPortable) PORSCHE.wheels[1]).name;
        assertEquals(expected, reader(PORSCHE).read("wheels[1].name"));
    }

    @Test
    public void portableArrayFirst_primitiveAtTheEnd() throws IOException {
        String expected = "rear";
        Assert.assertEquals(expected, reader(PORSCHE).read("wheels[1].name"));
    }

    @Test
    public void portableArrayFirst_portableAtTheEnd() throws IOException {
        int expected = ((WheelPortable) PORSCHE.wheels[1]).chip.power;
        assertEquals(expected, reader(PORSCHE).read("wheels[1].chip.power"));
    }

    @Test
    public void portableArrayFirst_portableArrayAtTheEnd_oneElementFetched() throws IOException {
        int expected = ((ChipPortable) ((WheelPortable) PORSCHE.wheels[0]).chips[1]).power;
        assertEquals(expected, reader(PORSCHE).read("wheels[0].chips[1].power"));
    }

    @Test
    public void portableArrayFirst_portableArrayAtTheEnd_wholeArrayFetched() throws IOException {
        Object[] expected = Arrays.asList((((WheelPortable) PORSCHE.wheels[0]).chips))
                .stream().map((Function<Portable, Object>) portable -> ((ChipPortable) portable).power).toArray();
        assertCollection(Arrays.asList(expected), ((MultiResult) reader(PORSCHE).read("wheels[0].chips[any].power")).getResults());
    }

    @Test
    public void portableArrayFirst_portableArrayAtTheEnd_wholeArrayFetched_withAny() throws IOException {
        assertTrue(((MultiResult) reader(PORSCHE).read("wheels[0].emptyChips[any]")).isNullEmptyTarget());
    }

    @Test
    public void portableArrayFirst_portableArrayInTheMiddle_primitiveAtTheEnd() throws IOException {
        int expected = 20;
        Assert.assertEquals(expected, reader(PORSCHE).read("wheels[0].chips[0].power"));
    }

    @Test
    public void portableArrayFirst_primitiveArrayAtTheEnd() throws IOException {
        int expected = 12 + 5;
        Assert.assertEquals(expected, reader(PORSCHE).read("wheels[0].serial[1]"));
    }

    @Test(expected = HazelcastSerializationException.class)
    public void portableArrayFirst_primitiveArrayAtTheEnd2() throws IOException {
        reader(PORSCHE).read("wheels[0].serial[1].x");
    }

    @Test
    public void portableArrayFirst_primitiveArrayAtTheEnd_wholeArrayFetched() throws IOException {
        int[] expected = ((WheelPortable) PORSCHE.wheels[0]).serial;
        assertArrayEquals(expected, (int[]) reader(PORSCHE).read("wheels[0].serial"));
    }

    @Test
    public void portableArrayFirst_primitiveArrayAtTheEnd_wholeArrayFetched_withAny() throws IOException {
        int[] expected = ((WheelPortable) PORSCHE.wheels[0]).serial;
        List<Integer> collect = Arrays.stream(expected).boxed().collect(Collectors.toList());
        assertCollection(collect, ((MultiResult) reader(PORSCHE).read("wheels[0].serial[any]")).getResults());
    }

    @Test
    public void portableArrayFirst_withAny_primitiveArrayAtTheEnd() throws IOException {
        assertCollection(Arrays.asList(17, 16), ((MultiResult) reader(PORSCHE).read("wheels[any].serial[1]")).getResults());
    }

    @Test
    public void portableArrayFirst_withAny_primitiveArrayAtTheEnd2() throws IOException {
        Integer[] expected = new Integer[]{
                ((WheelPortable) PORSCHE.wheels[0]).chip.power,
                ((WheelPortable) PORSCHE.wheels[1]).chip.power
        };
        assertCollection(Arrays.asList(expected), ((MultiResult) reader(PORSCHE).read("wheels[any].chip.power")).getResults());
    }

    @Test
    public void portableArrayFirst_withAny_primitiveArrayAtTheEnd3() throws IOException {
        Integer[] expected = new Integer[]{
                ((ChipPortable) ((WheelPortable) PORSCHE.wheels[0]).chips[1]).power,
                ((ChipPortable) ((WheelPortable) PORSCHE.wheels[1]).chips[1]).power,
        };
        assertCollection(Arrays.asList(expected), ((MultiResult) reader(PORSCHE).read("wheels[any].chips[1].power")).getResults());
    }

    @Test
    public void portableArrayFirst_withAny_primitiveArrayAtTheEnd5() throws IOException {
        String[] expected = {
                "front",
                "rear",
        };
        assertCollection(Arrays.asList(expected), ((MultiResult) reader(PORSCHE).read("wheels[any].name")).getResults());
    }

    @Test
    public void portableArrayFirst_withAny_primitiveArrayAtTheEnd6() throws IOException {
        assertTrue(((MultiResult) reader(PORSCHE).read("wheels[1].emptyChips[any].power")).isNullEmptyTarget());
    }

    @Test
    public void portableArrayFirst_withAny_primitiveArrayAtTheEnd7() throws IOException {
        assertTrue(((MultiResult) reader(PORSCHE).read("wheels[1].nullChips[any].power")).isNullEmptyTarget());
    }

    @Test
    public void portableArrayFirst_withAny_primitiveArrayAtTheEnd8() throws IOException {
        assertTrue(((MultiResult) reader(PORSCHE).read("wheels[1].emptyChips[any]")).isNullEmptyTarget());
    }

    @Test
    public void portableArrayFirst_withAny_primitiveArrayAtTheEnd8a() throws IOException {
        assertTrue(((MultiResult) reader(PORSCHE).read("wheels[any].emptyChips[any]")).isNullEmptyTarget());
    }

    @Test
    public void portableArrayFirst_withAny_primitiveArrayAtTheEnd9() throws IOException {
        Portable[] expected = {};
        assertArrayEquals(expected, (Object[]) reader(PORSCHE).read("wheels[1].emptyChips"));
    }

    @Test
    public void portableArrayFirst_withAny_primitiveArrayAtTheEnd10() throws IOException {
        assertTrue(((MultiResult) reader(PORSCHE).read("wheels[1].nullChips[any]")).isNullEmptyTarget());
    }

    @Test
    public void portableArrayFirst_withAny_primitiveArrayAtTheEnd11() throws IOException {
        assertArrayEquals(null, (boolean[]) reader(PORSCHE).read("wheels[1].nullChips"));
    }

    @Test
    public void reusingTheReader_multipleCalls_stateResetCorrectly() throws IOException {
        GenericRecordQueryReader reader = reader(PORSCHE);
        assertEquals("rear", reader.read("wheels[1].name"));
        assertEquals(300, reader.read("engine.power"));
        assertEquals(46, reader.read("wheels[0].serial[0]"));

        reader.read("wheels[0].serial[0]");

        assertEquals("front", reader.read("wheels[0].name"));
        assertEquals(45, reader.read("wheels[1].serial[0]"));

        reader.read("name");

        assertEquals(15, reader.read("engine.chip.power"));
        assertEquals("Porsche", reader.read("name"));
    }

    //
    // Utilities
    //
    public GenericRecordQueryReader reader(Portable portable) throws IOException {
        SerializationConfig serializationConfig = new SerializationConfig();
        serializationConfig.addPortableFactory(TestPortableFactory.ID, new TestPortableFactory());

        InternalSerializationService ss = new DefaultSerializationServiceBuilder().setConfig(serializationConfig).build();

        ss.toData(NON_EMPTY_PORSCHE);
        return new GenericRecordQueryReader(ss.readAsInternalGenericRecord(ss.toData(portable)));
    }

    static class CarPortable implements Portable {

        static final int FACTORY_ID = 1;
        static final int ID = 5;

        int power;
        String name;
        EnginePortable engine;
        Portable[] wheels;

        public String[] model;

        CarPortable() {
        }

        CarPortable(String name, EnginePortable engine, WheelPortable... wheels) {
            this.power = 100;
            this.name = name;
            this.engine = engine;
            this.wheels = wheels;
            this.model = new String[]{"911", "GT"};
        }

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
            writer.writeInt("power", power);
            writer.writeString("name", name);
            writer.writePortable("engine", engine);
            writer.writePortableArray("wheels", wheels);
            writer.writeStringArray("model", model);
        }

        @Override
        public void readPortable(PortableReader reader) throws IOException {
            power = reader.readInt("power");
            name = reader.readString("name");
            engine = reader.readPortable("engine");
            wheels = reader.readPortableArray("wheels");
            model = reader.readStringArray("model");
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            CarPortable that = (CarPortable) o;
            if (name != null ? !name.equals(that.name) : that.name != null) {
                return false;
            }
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

        static final int FACTORY_ID = 1;
        static final int ID = 8;

        Integer power;
        ChipPortable chip;

        EnginePortable() {
            this.chip = new ChipPortable();
        }

        EnginePortable(int power) {
            this.power = power;
            this.chip = new ChipPortable();
        }

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
            writer.writeInt("power", power);
            writer.writePortable("chip", chip);
        }

        @Override
        public void readPortable(PortableReader reader) throws IOException {
            power = reader.readInt("power");
            chip = reader.readPortable("chip");
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
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

        static final int FACTORY_ID = 1;
        static final int ID = 6;

        Integer power;

        ChipPortable() {
            this.power = 15;
        }

        ChipPortable(int power) {
            this.power = power;
        }

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
            writer.writeInt("power", power);
        }

        @Override
        public void readPortable(PortableReader reader) throws IOException {
            power = reader.readInt("power");
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
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

        static final int FACTORY_ID = 1;
        static final int ID = 7;

        String name;
        ChipPortable chip;
        Portable[] chips;
        Portable[] emptyChips;
        Portable[] nullChips;
        int[] serial;

        WheelPortable() {
        }

        WheelPortable(String name, boolean nonNull) {
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
            writer.writeString("name", name);
            writer.writePortable("chip", chip);
            writer.writePortableArray("chips", chips);
            writer.writePortableArray("emptyChips", emptyChips);
            writer.writePortableArray("nullChips", nullChips);
            writer.writeIntArray("serial", serial);
        }

        @Override
        public void readPortable(PortableReader reader) throws IOException {
            name = reader.readString("name");
            chip = reader.readPortable("chip");
            chips = reader.readPortableArray("chips");
            emptyChips = reader.readPortableArray("emptyChips");
            nullChips = reader.readPortableArray("nullChips");
            serial = reader.readIntArray("serial");
        }

        static WheelPortable w(String name, boolean nonNull) {
            return new WheelPortable(name, nonNull);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
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

        public static final int ID = 1;

        @Override
        public Portable create(int classId) {
            if (CarPortable.ID == classId) {
                return new CarPortable();
            } else if (EnginePortable.ID == classId) {
                return new EnginePortable();
            } else if (WheelPortable.ID == classId) {
                return new WheelPortable();
            } else if (ChipPortable.ID == classId) {
                return new ChipPortable();
            } else {
                return null;
            }
        }
    }
}
