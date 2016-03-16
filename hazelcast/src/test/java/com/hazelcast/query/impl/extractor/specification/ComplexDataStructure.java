package com.hazelcast.query.impl.extractor.specification;

import com.hazelcast.test.ObjectTestUtils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Data structure used in the tests of extraction in multi-value attributes (in collections and arrays)
 * Each multi-value attribute is present as both an array and as a collection, for example:
 * limbs_list & limbs_array, so that both extraction in arrays and in collections may be tested.
 */
public class ComplexDataStructure {

    public static class Person implements Serializable {
        String name;
        List<Limb> limbs_list = new ArrayList<Limb>();
        Limb[] limbs_array = null;
        Limb firstLimb = null;
        Limb secondLimb = null;

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof Person)) return false;
            final Person other = (Person) o;
            return ObjectTestUtils.equals(this.name, other.name) && ObjectTestUtils.equals(this.limbs_list, other.limbs_list);
        }

        @Override
        public int hashCode() {
            return ObjectTestUtils.hash(name, limbs_list);
        }
    }

    public static class Limb implements Serializable {
        String name;
        List<Finger> fingers_list = new ArrayList<Finger>();
        Finger[] fingers_array;
        List<String> tattoos_list = new ArrayList<String>();
        String[] tattoos_array = null;

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof Limb)) return false;
            final Limb other = (Limb) o;
            return ObjectTestUtils.equals(this.name, other.name) && ObjectTestUtils.equals(this.fingers_list, other.fingers_list)
                    && ObjectTestUtils.equals(this.tattoos_list, other.tattoos_list);
        }

        @Override
        public int hashCode() {
            return ObjectTestUtils.hash(name, fingers_list, tattoos_list);
        }
    }

    public static class Finger implements Serializable, Comparable<Finger> {
        String name;

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof Finger)) return false;
            final Finger other = (Finger) o;
            return ObjectTestUtils.equals(this.name, other.name);
        }

        @Override
        public int hashCode() {
            return ObjectTestUtils.hashCode(name);
        }

        @Override
        public int compareTo(Finger o) {
            return this.name.compareTo(o.name);
        }
    }

    public static Finger finger(String name) {
        Finger finger = new Finger();
        finger.name = name;
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
        return limb;
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
        return person;
    }

}
