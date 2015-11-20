package com.hazelcast.query.impl.extractor.predicates;

import com.hazelcast.test.ObjectTestUtils;

import java.io.Serializable;

/**
 * Data structure used in the tests of extraction in single-value attributes (not in collections).
 */
public class SingleValueDataStructure {

    public static class Person implements Serializable {

        Brain brain;

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof Person)) {
                return false;
            }
            Person other = (Person) o;
            return ObjectTestUtils.equals(this.brain, other.brain);
        }

        @Override
        public int hashCode() {
            return ObjectTestUtils.hashCode(brain);
        }
    }

    public static class Brain implements Serializable {

        Integer iq;
        String name;

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof Brain)) {
                return false;
            }
            Brain other = (Brain) o;
            return ObjectTestUtils.equals(this.iq, other.iq);
        }

        @Override
        public int hashCode() {
            return ObjectTestUtils.hashCode(iq);
        }
    }

    public static Person person(Integer iq) {
        Brain brain = new Brain();
        brain.iq = iq;
        brain.name = "brain" + iq;
        Person person = new Person();
        person.brain = brain;
        return person;
    }
}
