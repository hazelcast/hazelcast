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
import com.hazelcast.test.ObjectTestUtils;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.hazelcast.query.impl.extractor.AbstractExtractionSpecification.PortableAware;

/**
 * Data structure used in the tests of extraction in multi-value attributes (in collections and arrays)
 * Each multi-value attribute is present as both an array and as a collection, for example:
 * limbs_list & limbs_array, so that both extraction in arrays and in collections may be tested.
 */
public class ComplexTestDataStructure {

    private ComplexTestDataStructure() {
    }

    public static class Person implements Serializable, PortableAware {
        String name;
        List<Limb> limbs_list = new ArrayList<Limb>();
        Limb[] limbs_array;
        Limb firstLimb;
        Limb secondLimb;

        transient PersonPortable portable;

        public String getName() {
            return name;
        }

        public Limb getFirstLimb() {
            return firstLimb;
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof Person)) {
                return false;
            }
            final Person other = (Person) o;
            return ObjectTestUtils.equals(this.name, other.name) && ObjectTestUtils.equals(this.limbs_list, other.limbs_list);
        }

        @Override
        public int hashCode() {
            return ObjectTestUtils.hash(name, limbs_list);
        }

        public PersonPortable getPortable() {
            return portable;
        }
    }

    public static class PersonPortable implements Serializable, Portable {

        static final int FACTORY_ID = 1;
        static final int ID = 10;

        String name;
        Portable[] limbs_portable;
        LimbPortable firstLimb;
        LimbPortable secondLimb;

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof PersonPortable)) {
                return false;
            }
            final PersonPortable other = (PersonPortable) o;
            return ObjectTestUtils.equals(this.name, other.name);
        }

        @Override
        public int hashCode() {
            return ObjectTestUtils.hash(name);
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
            writer.writePortableArray("limbs_portable", limbs_portable);
            writer.writePortable("firstLimb", firstLimb);
            writer.writePortable("secondLimb", secondLimb);
        }

        @Override
        public void readPortable(PortableReader reader) throws IOException {
            name = reader.readString("name");
            limbs_portable = reader.readPortableArray("limbs_portable");
            firstLimb = reader.readPortable("firstLimb");
            secondLimb = reader.readPortable("secondLimb");
        }
    }

    public static class Limb implements Serializable, PortableAware, Comparable<Limb> {
        String name;
        List<Finger> fingers_list = new ArrayList<Finger>();
        Finger[] fingers_array;
        List<String> tattoos_list = new ArrayList<String>();
        String[] tattoos_array;

        public String getName() {
            return name;
        }

        transient LimbPortable portable;

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof Limb)) {
                return false;
            }
            final Limb other = (Limb) o;
            return ObjectTestUtils.equals(this.name, other.name) && ObjectTestUtils.equals(this.fingers_list, other.fingers_list)
                    && ObjectTestUtils.equals(this.tattoos_list, other.tattoos_list);
        }

        @Override
        public int hashCode() {
            return ObjectTestUtils.hash(name, fingers_list, tattoos_list);
        }

        public LimbPortable getPortable() {
            return portable;
        }

        @Override
        public int compareTo(Limb other) {
            return this.name.compareTo(other.name);
        }
    }

    public static class LimbPortable implements Serializable, Portable, Comparable<LimbPortable> {
        static final int FACTORY_ID = 1;
        static final int ID = 11;

        String name;
        Portable[] fingers_portable;
        String[] tattoos_portable;

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof LimbPortable)) {
                return false;
            }
            final LimbPortable other = (LimbPortable) o;
            return ObjectTestUtils.equals(this.name, other.name);
        }

        @Override
        public int hashCode() {
            return ObjectTestUtils.hash(name);
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
            writer.writePortableArray("fingers_portable", fingers_portable);
            writer.writeStringArray("tattoos_portable", tattoos_portable);
        }

        @Override
        public void readPortable(PortableReader reader) throws IOException {
            name = reader.readString("name");
            fingers_portable = reader.readPortableArray("fingers_portable");
            tattoos_portable = reader.readStringArray("tattoos_portable");
        }

        @Override
        public int compareTo(LimbPortable other) {
            return this.name.compareTo(other.name);
        }
    }

    public static class Finger implements Serializable, Comparable<Finger>, PortableAware {
        String name;
        transient FingerPortable portable;

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof Finger)) {
                return false;
            }
            final Finger other = (Finger) o;
            return ObjectTestUtils.equals(this.name, other.name);
        }

        @Override
        public int hashCode() {
            return ObjectTestUtils.hashCode(name);
        }

        @Override
        public int compareTo(Finger o) {
            if (o == null || this.name == null || o.name == null) {
                return -1;
            }
            return this.name.compareTo(o.name);
        }

        public FingerPortable getPortable() {
            return portable;
        }
    }

    public static class FingerPortable implements Serializable, Comparable<FingerPortable>, Portable {
        static final int FACTORY_ID = 1;
        static final int ID = 12;

        String name;

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof FingerPortable)) {
                return false;
            }
            final FingerPortable other = (FingerPortable) o;
            return ObjectTestUtils.equals(this.name, other.name);
        }

        @Override
        public int hashCode() {
            return ObjectTestUtils.hashCode(name);
        }

        @Override
        public int compareTo(FingerPortable o) {
            if (o == null || this.name == null || o.name == null) {
                return -1;
            }
            return this.name.compareTo(o.name);
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
        }

        @Override
        public void readPortable(PortableReader reader) throws IOException {
            name = reader.readString("name");
        }
    }

    public static Finger finger(String name) {
        FingerPortable portable = new FingerPortable();
        portable.name = name;

        Finger finger = new Finger();
        finger.name = name;
        finger.portable = portable;
        return finger;
    }

    public static List<String> tattoos(String... tattoos) {
        return Arrays.asList(tattoos);
    }

    public static Limb limb(String name, List<String> tattoos, Finger... fingers) {
        Limb limb = new Limb();
        limb.name = name;
        if (tattoos == null) {
            limb.tattoos_list = null;
            limb.tattoos_array = null;
        } else {
            limb.tattoos_list.addAll(tattoos);
            limb.tattoos_array = new String[tattoos.size()];
            int i = 0;
            for (String tattoo : tattoos) {
                limb.tattoos_array[i++] = tattoo;
            }
        }
        if (fingers.length == 0) {
            limb.fingers_list = null;
            limb.fingers_array = null;
        } else {
            limb.fingers_list.addAll(Arrays.asList(fingers));
            limb.fingers_array = fingers;
        }

        setupLimbPortable(limb);
        return limb;
    }

    private static void setupLimbPortable(Limb limb) {
        LimbPortable portable = new LimbPortable();
        portable.name = limb.name;
        if (limb.fingers_array != null) {
            portable.fingers_portable = new Portable[limb.fingers_array.length];
            for (int i = 0; i < limb.fingers_array.length; i++) {
                if (limb.fingers_array[i] != null) {
                    portable.fingers_portable[i] = limb.fingers_array[i].getPortable();
                }
            }
        }
        portable.tattoos_portable = limb.tattoos_array;
        limb.portable = portable;
    }

    public static Person person(String name, Limb... limbs) {
        Person person = new Person();
        person.name = name;
        if (limbs.length > 0) {
            person.limbs_list.addAll(Arrays.asList(limbs));
        } else {
            person.limbs_list = null;
        }
        if (limbs.length > 0) {
            person.firstLimb = limbs[0];
        }
        if (limbs.length > 1) {
            person.secondLimb = limbs[1];
        }
        person.limbs_array = limbs;
        setupPersonPortable(person);
        return person;
    }

    private static void setupPersonPortable(Person person) {
        PersonPortable portable = new PersonPortable();
        portable.name = person.name;
        portable.firstLimb = person.firstLimb != null ? person.firstLimb.getPortable() : null;
        portable.secondLimb = person.secondLimb != null ? person.secondLimb.getPortable() : null;

        if (person.limbs_array != null) {
            portable.limbs_portable = new Portable[person.limbs_array.length];
            for (int i = 0; i < person.limbs_array.length; i++) {
                if (person.limbs_array[i] != null) {
                    portable.limbs_portable[i] = person.limbs_array[i].getPortable();
                }
            }
        }

        person.portable = portable;
    }

    static class PersonPortableFactory implements PortableFactory {
        static final int ID = 1;

        @Override
        public Portable create(int classId) {
            if (PersonPortable.ID == classId) {
                return new PersonPortable();
            } else if (LimbPortable.ID == classId) {
                return new LimbPortable();
            } else if (FingerPortable.ID == classId) {
                return new FingerPortable();
            } else {
                return null;
            }
        }
    }

}
