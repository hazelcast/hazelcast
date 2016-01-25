package com.hazelcast.query.impl.extractor.predicates;

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
public class CollectionDataStructure {

    public static class Person implements Serializable {
        public List<Limb> limbs_list = new ArrayList<Limb>();
        Limb[] limbs_array = null;

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof Person)) return false;
            final Person other = (Person) o;
            return ObjectTestUtils.equals(this.limbs_list, other.limbs_list);
        }

        @Override
        public int hashCode() {
            return ObjectTestUtils.hashCode(limbs_list);
        }
    }

    public static class Limb implements Serializable {
        String name;
        public Integer power;

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof Limb)) return false;
            final Limb other = (Limb) o;
            return ObjectTestUtils.equals(this.name, other.name) && ObjectTestUtils.equals(this.power, other.power);
        }

        @Override
        public int hashCode() {
            return ObjectTestUtils.hash(name, power);
        }
    }

    public static Limb limb(String name, Integer power) {
        Limb limb = new Limb();
        limb.name = name;
        limb.power = power;
        return limb;
    }

    public static Person person(Limb... limbs) {
        Person person = new Person();
        person.limbs_list.addAll(Arrays.asList(limbs));
        person.limbs_array = limbs;
        return person;
    }
}
