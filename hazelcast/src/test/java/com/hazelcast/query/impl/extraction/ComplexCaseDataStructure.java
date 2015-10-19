package com.hazelcast.query.impl.extraction;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public class ComplexCaseDataStructure {

    public static class Person implements Serializable {
        String name;
        List<Limb> limbs_list = new ArrayList<Limb>();
        Limb[] limbs_array = null;

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof Person)) return false;
            final Person other = (Person) o;
            return Objects.equals(this.name, other.name) && Objects.equals(this.limbs_list, other.limbs_list);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, limbs_list);
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
            return Objects.equals(this.name, other.name) && Objects.equals(this.fingers_list, other.fingers_list)
                    && Objects.equals(this.tattoos_list, other.tattoos_list);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, fingers_list, tattoos_list);
        }
    }

    public static class Finger implements Serializable, Comparable<Finger> {
        String name;

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof Finger)) return false;
            final Finger other = (Finger) o;
            return Objects.equals(this.name, other.name);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(name);
        }

        @Override
        public int compareTo(Finger o) {
            return equals(o) ? 0 : -1;
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
        person.limbs_list.addAll(Arrays.asList(limbs));
        person.limbs_array = limbs;
        return person;
    }


}
