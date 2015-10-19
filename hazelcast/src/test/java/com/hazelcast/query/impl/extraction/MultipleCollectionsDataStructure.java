package com.hazelcast.query.impl.extraction;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

// PRD: CASE 5
public class MultipleCollectionsDataStructure {

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
        List<String> fingers_list = new ArrayList<String>();
        String[] fingers_array;

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof Limb)) return false;
            final Limb other = (Limb) o;
            return Objects.equals(fingers_list, other.fingers_list);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(fingers_list);
        }
    }

    public static Limb limb(String... fingers) {
        Limb limb = new Limb();
        limb.fingers_list.addAll(Arrays.asList(fingers));
        limb.fingers_array = fingers;
        return limb;
    }

    public static Person person(Limb... limbs) {
        Person person = new Person();
        person.limbs_list.addAll(Arrays.asList(limbs));
        person.limbs_array = limbs;
        return person;
    }

}
