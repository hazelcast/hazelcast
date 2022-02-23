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

package com.hazelcast.query.impl.extractor.specification;

import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableFactory;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;

import java.io.IOException;
import java.util.Arrays;

public class PortableDataStructure {

    static class CarPortable implements Portable {

        static final int FACTORY_ID = 1;
        static final int ID = 5;

        String name;
        EnginePortable engine;
        Portable[] wheels;
        String[] model;

        CarPortable(String name, EnginePortable engine) {
            this.name = name;
            this.engine = engine;
            this.wheels = new Portable[]{
                    new WheelPortable("FL"),
                    new WheelPortable("FR"),
                    new WheelPortable("RL"),
                    new WheelPortable("RR"),
            };
            this.model = new String[]{"911", "GT"};
        }

        CarPortable(String name, EnginePortable engine, WheelPortable... wheels) {
            this.name = name;
            this.engine = engine;
            this.wheels = wheels;
            this.model = new String[]{"911", "GT"};
        }

        CarPortable() {
        }

        public int getFactoryId() {
            return FACTORY_ID;
        }

        public int getClassId() {
            return ID;
        }

        public void writePortable(PortableWriter writer) throws IOException {
            writer.writeString("name", name);
            writer.writePortable("engine", engine);
            writer.writePortableArray("wheels", wheels);
            writer.writeStringArray("model", model);
        }

        public void readPortable(PortableReader reader) throws IOException {
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

        EnginePortable(int power) {
            this.power = power;
            this.chip = new ChipPortable();
        }

        EnginePortable() {
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

        ChipPortable(int power) {
            this.power = power;
        }

        ChipPortable() {
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
        int[] serial;

        WheelPortable(String name) {
            this.name = name;
            this.chip = new ChipPortable(100);
            this.chips = new Portable[]{new ChipPortable(20), new ChipPortable(40)};
            this.serial = new int[]{41, 12, 79, 18, 102};
        }

        WheelPortable() {
        }

        public int getFactoryId() {
            return FACTORY_ID;
        }

        public int getClassId() {
            return ID;
        }

        public void writePortable(PortableWriter writer) throws IOException {
            writer.writeString("name", name);
            writer.writePortable("chip", chip);
            writer.writePortableArray("chips", chips);
            writer.writeIntArray("serial", serial);
        }

        public void readPortable(PortableReader reader) throws IOException {
            name = reader.readString("name");
            chip = reader.readPortable("chip");
            chips = reader.readPortableArray("chips");
            serial = reader.readIntArray("serial");
        }

        public static WheelPortable w(String name) {
            return new WheelPortable(name);
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

    static class XPortable implements Portable, Comparable<XPortable> {

        static final int FACTORY_ID = 1;
        static final int ID = 10;

        Portable[] chips;

        XPortable() {
            this.chips = new Portable[]{new YPortable()};
        }

        public int getFactoryId() {
            return FACTORY_ID;
        }

        public int getClassId() {
            return ID;
        }

        public void writePortable(PortableWriter writer) throws IOException {
            writer.writePortableArray("chips", chips);
        }

        public void readPortable(PortableReader reader) throws IOException {
            chips = reader.readPortableArray("chips");
        }

        @Override
        public boolean equals(Object o) {
            return Arrays.equals(this.chips, ((XPortable) o).chips);
        }

        @Override
        public int hashCode() {
            return chips.hashCode();
        }

        @Override
        public int compareTo(XPortable o) {
            return this.equals(o) ? 0 : -1;
        }
    }

    static class YPortable implements Portable, Comparable<YPortable> {

        static final int FACTORY_ID = 1;
        static final int ID = 9;

        int[] serial;

        YPortable() {
            this.serial = new int[]{41};
        }

        public int getFactoryId() {
            return FACTORY_ID;
        }

        public int getClassId() {
            return ID;
        }

        public void writePortable(PortableWriter writer) throws IOException {
            writer.writeIntArray("serial", serial);
        }

        public void readPortable(PortableReader reader) throws IOException {
            serial = reader.readIntArray("serial");
        }

        @Override
        public boolean equals(Object o) {
            return Arrays.equals(this.serial, ((YPortable) o).serial);
        }

        @Override
        public int hashCode() {
            return serial.hashCode();
        }

        @Override
        public int compareTo(YPortable o) {
            return this.equals(o) ? 0 : -1;
        }
    }

    static class CarPortableFactory implements PortableFactory {

        static final int ID = 1;

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
            } else if (XPortable.ID == classId) {
                return new XPortable();
            } else if (YPortable.ID == classId) {
                return new YPortable();
            } else {
                return null;
            }
        }
    }
}
