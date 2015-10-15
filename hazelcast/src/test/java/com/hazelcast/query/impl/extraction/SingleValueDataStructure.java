package com.hazelcast.query.impl.extraction;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapAttributeConfig;
import com.hazelcast.config.MapConfig;
import com.hazelcast.query.extractor.ValueExtractor;

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

    public static class IqExtractor extends ValueExtractor {
        @Override
        public Object extract(Object target) {
            return ((Person) target).brain.iq;
        }
    }

    public static class NameExtractor extends ValueExtractor {
        @Override
        public Object extract(Object target) {
            return ((Person) target).brain.name;
        }
    }

    public static class BrainExtractorsConfigurator extends AbstractExtractionTest.Configurator {
        @Override
        public void doWithConfig(Config config) {
            MapConfig mapConfig = config.getMapConfig("map");

            MapAttributeConfig iqConfig = new AbstractExtractionTest.TestMapAttributeIndexConfig();
            iqConfig.setName("brain.iq");
            iqConfig.setExtractor("com.hazelcast.query.impl.extraction.SingleValueDataStructure$IqExtractor");
            mapConfig.addMapAttributeConfig(iqConfig);

            MapAttributeConfig nameConfig = new AbstractExtractionTest.TestMapAttributeIndexConfig();
            nameConfig.setName("brain.name");
            nameConfig.setExtractor("com.hazelcast.query.impl.extraction.SingleValueDataStructure$NameExtractor");
            mapConfig.addMapAttributeConfig(nameConfig);

        }
    }

}
