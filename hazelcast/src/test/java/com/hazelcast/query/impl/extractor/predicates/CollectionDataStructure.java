/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.query.impl.extractor.predicates;

import com.hazelcast.test.ObjectTestUtils;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Data structure used in the tests of extraction in multi-value attributes (in collections, arrays and maps)
 * Each multi-value attribute is present as both an array and as a collection, for example:
 * limbs_list & limbs_array, so that both extraction in arrays and in collections may be tested.
 */
public class CollectionDataStructure {

    public static class Person implements Serializable {

        List<Limb> limbs_list = new ArrayList<Limb>();
        Limb[] limbs_array = null;

        private Map<String, Tatoo> tatoos = new HashMap<String, Tatoo>();
        private Map<Short, Achievement> achievementsPerYear = new HashMap<Short, Achievement>();

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Person person = (Person) o;
            return ObjectTestUtils.equals(limbs_list, person.limbs_list) &&
                    Arrays.equals(limbs_array, person.limbs_array) &&
                    ObjectTestUtils.equals(tatoos, person.tatoos);
        }

        @Override
        public int hashCode() {

            int result = ObjectTestUtils.hash(limbs_list, tatoos);
            result = 31 * result + Arrays.hashCode(limbs_array);
            return result;
        }

        @Override
        public String toString() {
            return "Person{" +
                    "limbs_list=" + limbs_list +
                    ", limbs_array=" + Arrays.toString(limbs_array) +
                    ", tatoos=" + tatoos +
                    '}';
        }

        public Person withTatoos(Tatoo... tatoos) {
            for (Tatoo tatoo : tatoos) {
                this.tatoos.put(tatoo.location, tatoo);
            }
            return this;
        }

        public Person withAchievements(Achievement... achievements) {
            for (Achievement achievement : achievements) {
                this.achievementsPerYear.put(achievement.year, achievement);
            }
            return this;
        }
    }

    public static class Limb implements Serializable {

        String name;
        public Integer power;

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof Limb)) {
                return false;
            }
            final Limb other = (Limb) o;
            return ObjectTestUtils.equals(this.name, other.name) && ObjectTestUtils.equals(this.power, other.power);
        }

        @Override
        public int hashCode() {
            return ObjectTestUtils.hash(name, power);
        }

        @Override
        public String toString() {
            return "Limb{" +
                    "name='" + name + '\'' +
                    ", power=" + power +
                    '}';
        }
    }

    public static class Tatoo implements Serializable {

        private final String location;
        private final int size;
        private final String image;

        public Tatoo(String location, int size, String image) {
            this.location = location;
            this.size = size;
            this.image = image;
        }

        public String getLocation() {
            return location;
        }

        public int getSize() {
            return size;
        }

        public String getImage() {
            return image;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Tatoo tatoo = (Tatoo) o;
            return size == tatoo.size &&
                    ObjectTestUtils.equals(location, tatoo.location) &&
                    ObjectTestUtils.equals(image, tatoo.image);
        }

        @Override
        public int hashCode() {
            return ObjectTestUtils.hash(location, size, image);
        }

        @Override
        public String toString() {
            return "Tatoo{" +
                    "location='" + location + '\'' +
                    ", size=" + size +
                    ", image='" + image + '\'' +
                    '}';
        }

    }

    public static Tatoo tatoo(String location, int size, String image) {
        return new Tatoo(location, size, image);
    }

    public static class Achievement implements Serializable {

        private final Short year;
        private final String description;

        public Achievement(Short year, String description) {
            this.year = year;
            this.description = description;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Achievement that = (Achievement) o;
            return ObjectTestUtils.equals(year, that.year) &&
                    ObjectTestUtils.equals(description, that.description);
        }

        @Override
        public int hashCode() {
            return ObjectTestUtils.hash(year, description);
        }

        @Override
        public String toString() {
            return "Achievement{" +
                    "year=" + year +
                    ", description='" + description + '\'' +
                    '}';
        }
    }

    public static Achievement achievement(Short year, String description) {
        return new Achievement(year, description);
    }


    public static Limb limb(String name, Integer power) {
        Limb limb = new Limb();
        limb.name = name;
        limb.power = power;
        return limb;
    }

    public static Person person() {
        return new Person();
    }

    public static Person person(Limb... limbs) {
        Person person = new Person();
        person.limbs_list.addAll(Arrays.asList(limbs));
        person.limbs_array = limbs;
        return person;
    }
}
