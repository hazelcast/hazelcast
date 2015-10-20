package com.hazelcast.query.impl.extraction;

import java.io.Serializable;
import java.util.Objects;

// PRD: CASE 1
public class SingleValueDataStructure {

    public static class Person implements Serializable {
        Brain brain;

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof Person)) return false;
            final Person other = (Person) o;
            return Objects.equals(this.brain, other.brain);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(brain);
        }
    }

    public static class Brain implements Serializable {
        Integer iq;
        String name;

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof Brain)) return false;
            final Brain other = (Brain) o;
            return Objects.equals(this.iq, other.iq);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(iq);
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
