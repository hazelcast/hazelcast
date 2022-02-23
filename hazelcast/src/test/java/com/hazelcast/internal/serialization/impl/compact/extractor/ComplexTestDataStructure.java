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

package com.hazelcast.internal.serialization.impl.compact.extractor;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * Data structure used in the tests of extraction in multi-value attributes (in collections and arrays)
 * Each multi-value attribute is present as both an array and as a collection, for example:
 * limbs_list & limbs_array, so that both extraction in arrays and in collections may be tested.
 */
public class ComplexTestDataStructure {

    private ComplexTestDataStructure() {
    }

    public static class Person {
        String name;
        Limb[] limbs_array;
        Limb firstLimb;
        Limb secondLimb;

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            Person person = (Person) o;

            if (!Objects.equals(name, person.name)) {
                return false;
            }
            // Probably incorrect - comparing Object[] arrays with Arrays.equals
            if (!Arrays.equals(limbs_array, person.limbs_array)) {
                return false;
            }
            if (!Objects.equals(firstLimb, person.firstLimb)) {
                return false;
            }
            return Objects.equals(secondLimb, person.secondLimb);
        }

        @Override
        public int hashCode() {
            int result = name != null ? name.hashCode() : 0;
            result = 31 * result + Arrays.hashCode(limbs_array);
            result = 31 * result + (firstLimb != null ? firstLimb.hashCode() : 0);
            result = 31 * result + (secondLimb != null ? secondLimb.hashCode() : 0);
            return result;
        }

        @Override
        public String toString() {
            return "Person{"
                    + "name='" + name + '\''
                    + ", limbs_array=" + Arrays.toString(limbs_array)
                    + ", firstLimb=" + firstLimb
                    + ", secondLimb=" + secondLimb
                    + '}';
        }
    }


    public static class Limb {
        String name;
        Finger[] fingers_array;
        String[] tattoos_array;

        public String getName() {
            return name;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            Limb limb = (Limb) o;

            if (!Objects.equals(name, limb.name)) {
                return false;
            }
            // Probably incorrect - comparing Object[] arrays with Arrays.equals
            if (!Arrays.equals(fingers_array, limb.fingers_array)) {
                return false;
            }
            // Probably incorrect - comparing Object[] arrays with Arrays.equals
            return Arrays.equals(tattoos_array, limb.tattoos_array);
        }

        @Override
        public int hashCode() {
            int result = name != null ? name.hashCode() : 0;
            result = 31 * result + Arrays.hashCode(fingers_array);
            result = 31 * result + Arrays.hashCode(tattoos_array);
            return result;
        }

    }


    public static class Finger {
        String name;

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            Finger finger = (Finger) o;

            return Objects.equals(name, finger.name);
        }

        @Override
        public int hashCode() {
            return name != null ? name.hashCode() : 0;
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
            limb.tattoos_array = null;
        } else {
            limb.tattoos_array = new String[tattoos.size()];
            int i = 0;
            for (String tattoo : tattoos) {
                limb.tattoos_array[i++] = tattoo;
            }
        }
        if (fingers.length == 0) {
            limb.fingers_array = null;
        } else {
            limb.fingers_array = fingers;
        }

        return limb;
    }


    public static Person person(String name, Limb... limbs) {
        Person person = new Person();
        person.name = name;
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
