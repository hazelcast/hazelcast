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

package com.hazelcast.internal.serialization.impl.compact.reader;

import com.hazelcast.config.CompactSerializationConfig;
import com.hazelcast.config.SerializationConfig;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.internal.serialization.impl.GenericRecordQueryReader;
import com.hazelcast.internal.serialization.impl.compact.CompactTestUtil;
import com.hazelcast.internal.serialization.impl.compact.SchemaService;
import com.hazelcast.nio.serialization.HazelcastSerializationException;
import com.hazelcast.query.impl.getters.MultiResult;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class CompactStreamSerializerValueReaderQuickTest extends HazelcastTestSupport {

    static final Car PORSCHE = new Car("Porsche", new Engine(300), null,
            Wheel.w("front", false), Wheel.w("rear", false));

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
    public void compactAttribute() throws IOException {
        Engine expected = PORSCHE.engine;
        assertEquals(expected, reader(PORSCHE).read("engine"));
    }

    @Test
    public void compactNullablePrimitive() throws IOException {
        assertEquals(PORSCHE.price, reader(PORSCHE).read("price"));
    }

    @Test
    public void nestedcompactAttribute() throws IOException {
        Chip expected = PORSCHE.engine.chip;
        assertEquals(expected, reader(PORSCHE).read("engine.chip"));
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
    public void compactArray_wholeArrayFetched() throws IOException {
        Wheel[] expected = PORSCHE.wheels;
        assertArrayEquals(expected, (Object[]) reader(PORSCHE).read("wheels"));
    }

    @Test
    public void compactArray_wholeArrayFetched_withAny() throws IOException {
        Wheel[] expected = PORSCHE.wheels;
        assertCollection(Arrays.asList(expected), ((MultiResult) reader(PORSCHE).read("wheels[any]")).getResults());
    }

    @Test
    public void compactArrayAtTheEnd_oneElementFetched() throws IOException {
        Wheel expected = PORSCHE.wheels[0];
        assertEquals(expected, reader(PORSCHE).read("wheels[0]"));
    }

    @Test
    public void compactArrayAtTheEnd_lastElementFetched() throws IOException {
        Wheel expected = PORSCHE.wheels[1];
        assertEquals(expected, reader(PORSCHE).read("wheels[1]"));
    }

    @Test
    public void compactArrayFirst_primitiveAtTheEnd() throws IOException {
        String expected = "rear";
        Assert.assertEquals(expected, reader(PORSCHE).read("wheels[1].name"));
    }

    @Test
    public void compactArrayFirst_compactAtTheEnd() throws IOException {
        Chip expected = ((Wheel) PORSCHE.wheels[1]).chip;
        assertEquals(expected, reader(PORSCHE).read("wheels[1].chip"));
    }

    @Test
    public void compactArrayFirst_compactArrayAtTheEnd_oneElementFetched() throws IOException {
        Chip expected = ((Wheel) PORSCHE.wheels[0]).chips[1];
        assertEquals(expected, reader(PORSCHE).read("wheels[0].chips[1]"));
    }

    @Test
    public void compactArrayFirst_compactArrayAtTheEnd_wholeArrayFetched() throws IOException {
        Chip[] expected = ((Wheel) PORSCHE.wheels[0]).chips;
        assertArrayEquals(expected, (Object[]) reader(PORSCHE).read("wheels[0].chips"));
    }

    @Test
    public void compactArrayFirst_compactArrayAtTheEnd_wholeArrayFetched_withAny() throws IOException {
        assertTrue(((MultiResult) reader(PORSCHE).read("wheels[0].emptyChips[any]")).isNullEmptyTarget());
    }

    @Test
    public void compactArrayFirst_compactArrayInTheMiddle_primitiveAtTheEnd() throws IOException {
        int expected = 20;
        Assert.assertEquals(expected, reader(PORSCHE).read("wheels[0].chips[0].power"));
    }

    @Test
    public void compactArrayFirst_primitiveArrayAtTheEnd() throws IOException {
        int expected = 12 + 5;
        Assert.assertEquals(expected, reader(PORSCHE).read("wheels[0].serial[1]"));
    }

    @Test(expected = HazelcastSerializationException.class)
    public void compactArrayFirst_primitiveArrayAtTheEnd2() throws IOException {
        reader(PORSCHE).read("wheels[0].serial[1].x");
    }

    @Test
    public void compactArrayFirst_primitiveArrayAtTheEnd_wholeArrayFetched() throws IOException {
        int[] expected = ((Wheel) PORSCHE.wheels[0]).serial;
        assertArrayEquals(expected, (int[]) reader(PORSCHE).read("wheels[0].serial"));
    }

    @Test
    public void compactArrayFirst_primitiveArrayAtTheEnd_wholeArrayFetched_withAny() throws IOException {
        int[] expected = ((Wheel) PORSCHE.wheels[0]).serial;
        List<Integer> collect = Arrays.stream(expected).boxed().collect(Collectors.toList());
        assertCollection(collect, ((MultiResult) reader(PORSCHE).read("wheels[0].serial[any]")).getResults());
    }

    @Test
    public void compactArrayFirst_withAny_primitiveArrayAtTheEnd() throws IOException {
        assertCollection(Arrays.asList(17, 16), ((MultiResult) reader(PORSCHE).read("wheels[any].serial[1]")).getResults());
    }

    @Test
    public void compactArrayFirst_withAny_ObjectArrayAtTheEnd2() throws IOException {
        Chip[] expected = new Chip[]{
                ((Wheel) PORSCHE.wheels[0]).chip,
                ((Wheel) PORSCHE.wheels[1]).chip,
        };
        assertCollection(Arrays.asList(expected), ((MultiResult) reader(PORSCHE).read("wheels[any].chip")).getResults());
    }

    @Test
    public void compactArrayFirst_withAny_primitiveArrayAtTheEnd3() throws IOException {
        Chip[] expected = new Chip[]{
                ((Wheel) PORSCHE.wheels[0]).chips[1],
                ((Wheel) PORSCHE.wheels[1]).chips[1],
        };
        assertCollection(Arrays.asList(expected), ((MultiResult) reader(PORSCHE).read("wheels[any].chips[1]")).getResults());
    }

    @Test
    public void compactArrayFirst_withAny_primitiveArrayAtTheEnd5() throws IOException {
        String[] expected = {
                "front",
                "rear",
        };
        assertCollection(Arrays.asList(expected), ((MultiResult) reader(PORSCHE).read("wheels[any].name")).getResults());
    }

    @Test
    public void compactArrayFirst_withAny_primitiveArrayAtTheEnd6() throws IOException {
        assertTrue(((MultiResult) reader(PORSCHE).read("wheels[1].emptyChips[any].power")).isNullEmptyTarget());
    }

    @Test
    public void compactArrayFirst_withAny_primitiveArrayAtTheEnd7() throws IOException {
        assertTrue(((MultiResult) reader(PORSCHE).read("wheels[1].nullChips[any].power")).isNullEmptyTarget());
    }

    @Test
    public void compactArrayFirst_withAny_primitiveArrayAtTheEnd8() throws IOException {
        assertTrue(((MultiResult) reader(PORSCHE).read("wheels[1].emptyChips[any]")).isNullEmptyTarget());
    }

    @Test
    public void compactArrayFirst_withAny_primitiveArrayAtTheEnd8a() throws IOException {
        assertTrue(((MultiResult) reader(PORSCHE).read("wheels[any].emptyChips[any]")).isNullEmptyTarget());
    }

    @Test
    public void compactArrayFirst_withAny_primitiveArrayAtTheEnd9() throws IOException {
        Object[] expected = {};
        assertArrayEquals(expected, (Object[]) reader(PORSCHE).read("wheels[1].emptyChips"));
    }

    @Test
    public void compactArrayFirst_withAny_primitiveArrayAtTheEnd10() throws IOException {
        assertTrue(((MultiResult) reader(PORSCHE).read("wheels[1].nullChips[any]")).isNullEmptyTarget());
    }

    @Test
    public void compactArrayFirst_withAny_primitiveArrayAtTheEnd11() throws IOException {
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

    public GenericRecordQueryReader reader(Car car) throws IOException {
        SchemaService schemaService = CompactTestUtil.createInMemorySchemaService();
        SerializationConfig serializationConfig = new SerializationConfig();
        serializationConfig.setCompactSerializationConfig(new CompactSerializationConfig().setEnabled(true));
        InternalSerializationService ss = new DefaultSerializationServiceBuilder()
                .setConfig(serializationConfig)
                .setSchemaService(schemaService).build();
        Data data = ss.toData(car);
        return new GenericRecordQueryReader(ss.readAsInternalGenericRecord(data));
    }

    static class Car {

        int power;
        String name;
        Engine engine;
        Wheel[] wheels;
        Integer price;

        public String[] model;

        Car() {
        }

        Car(String name, Engine engine, Integer price, Wheel... wheels) {
            this.power = 100;
            this.name = name;
            this.engine = engine;
            this.wheels = wheels;
            this.model = new String[]{"911", "GT"};
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            Car car = (Car) o;

            if (power != car.power) {
                return false;
            }
            if (!Objects.equals(name, car.name)) {
                return false;
            }
            if (!Objects.equals(engine, car.engine)) {
                return false;
            }
            if (!Arrays.equals(wheels, car.wheels)) {
                return false;
            }
            if (!Objects.equals(price, car.price)) {
                return false;
            }
            return Arrays.equals(model, car.model);
        }

        @Override
        public int hashCode() {
            int result = power;
            result = 31 * result + (name != null ? name.hashCode() : 0);
            result = 31 * result + (engine != null ? engine.hashCode() : 0);
            result = 31 * result + Arrays.hashCode(wheels);
            result = 31 * result + (price != null ? price.hashCode() : 0);
            result = 31 * result + Arrays.hashCode(model);
            return result;
        }
    }

    static class Engine implements Comparable<Engine> {

        int power;
        Chip chip;

        Engine() {
            this.chip = new Chip();
        }

        Engine(int power) {
            this.power = power;
            this.chip = new Chip();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Engine engine = (Engine) o;
            return power == engine.power && Objects.equals(chip, engine.chip);
        }

        @Override
        public int hashCode() {
            return Objects.hash(power, chip);
        }

        @Override
        public int compareTo(@Nonnull Engine o) {
            return power - o.power;
        }
    }

    static class Chip implements Comparable<Chip> {

        int power;

        Chip() {
            this.power = 15;
        }

        Chip(int power) {
            this.power = power;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Chip that = (Chip) o;
            return power == that.power;

        }

        @Override
        public int hashCode() {
            return power;
        }

        @Override
        public int compareTo(Chip o) {
            return power - o.power;
        }
    }

    static class Wheel implements Comparable<Wheel> {

        String name;
        Chip chip;
        Chip[] chips;
        Chip[] emptyChips;
        Chip[] nullChips;
        int[] serial;

        Wheel() {
        }

        Wheel(String name, boolean nonNull) {
            this.name = name;
            this.chip = new Chip(100);
            this.chips = new Chip[]{new Chip(20), new Chip(40)};
            if (nonNull) {
                this.emptyChips = new Chip[]{new Chip(20)};
                this.nullChips = new Chip[]{new Chip(20)};
            } else {
                this.emptyChips = new Chip[]{};
                this.nullChips = null;
            }
            int nameLength = name.length();
            this.serial = new int[]{41 + nameLength, 12 + nameLength, 79 + nameLength, 18 + nameLength, 102 + nameLength};
        }


        static Wheel w(String name, boolean nonNull) {
            return new Wheel(name, nonNull);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Wheel that = (Wheel) o;
            return Objects.equals(name, that.name);
        }

        @Override
        public int hashCode() {
            return name != null ? name.hashCode() : 0;
        }

        @Override
        public int compareTo(Wheel o) {
            return this.name.compareTo(o.name);
        }
    }

}
