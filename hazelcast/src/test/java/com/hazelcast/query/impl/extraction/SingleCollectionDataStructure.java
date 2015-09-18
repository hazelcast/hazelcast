package com.hazelcast.query.impl.extraction;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

// PRD: CASE 2
public class SingleCollectionDataStructure {

    public static class Person implements Serializable {
        List<Limb> limbs_list = new ArrayList<Limb>();
        Limb[] limbs_array = null;

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof Person)) return false;
            final Person other = (Person) o;
            return Objects.equals(this.limbs_list, other.limbs_list);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(limbs_list);
        }
    }

    public static class Limb implements Serializable {
        String name;
        Integer power;

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof Limb)) return false;
            final Limb other = (Limb) o;
            return Objects.equals(this.name, other.name) && Objects.equals(this.power, other.power);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, power);
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
