package com.hazelcast.query.impl.extraction;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapAttributeConfig;
import com.hazelcast.config.MapConfig;
import com.hazelcast.query.extractor.MultiResult;
import com.hazelcast.query.extractor.ValueExtractor;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

// PRD: CASE 5
public class MultipleCollectionsDataStructure {

    public static class Person implements Serializable {
        List<Limb> limbs = new ArrayList<Limb>();

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof Person)) return false;
            final Person other = (Person) o;
            return Objects.equals(this.limbs, other.limbs);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(limbs);
        }
    }

    public static class Limb implements Serializable {
        String[] fingers;

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof Limb)) return false;
            final Limb other = (Limb) o;
            return Arrays.equals(fingers, other.fingers);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(fingers);
        }
    }

    public static Limb limb(String... fingers) {
        Limb limb = new Limb();
        limb.fingers = new String[fingers.length];
        int i = 0;
        for (String finger : fingers) {
            limb.fingers[i++] = finger;
        }
        return limb;
    }

    public static Person person(Limb... limbs) {
        Person person = new Person();
        person.limbs.addAll(Arrays.asList(limbs));
        return person;
    }

    public static class BothIndexOneExtractor extends ValueExtractor {
        @Override
        public Object extract(Object target) {
            return ((Person) target).limbs.get(1).fingers[1];
        }
    }

    public static class BothReducedExtractor extends ValueExtractor {
        @Override
        public Object extract(Object target) {
            MultiResult multiResult = new MultiResult();
            for (Limb limb : ((Person) target).limbs) {
                for (String finger : limb.fingers) {
                    multiResult.add(finger);
                }
            }
            return multiResult;
        }
    }

    public static class FirstReducedExtractor extends ValueExtractor {
        @Override
        public Object extract(Object target) {
            MultiResult multiResult = new MultiResult();
            for (Limb limb : ((Person) target).limbs) {
                multiResult.add(limb.fingers[1]);
            }
            return multiResult;
        }
    }

    public static class LastReducedExtractor extends ValueExtractor {
        @Override
        public Object extract(Object target) {
            MultiResult multiResult = new MultiResult();
            Limb limb = ((Person) target).limbs.get(1);
            for (String finger : limb.fingers) {
                multiResult.add(finger);
            }
            return multiResult;
        }
    }

    public static class FingerExtractorsConfigurator extends AbstractExtractionTest.Configurator {
        @Override
        public void doWithConfig(Config config) {
            MapConfig mapConfig = config.getMapConfig("map");

            MapAttributeConfig reducedNameAttribute = new AbstractExtractionTest.TestMapAttributeIndexConfig();
            reducedNameAttribute.setName("limbs[1].fingers[1]");
            reducedNameAttribute.setExtractor(
                    "com.hazelcast.query.impl.extraction.MultipleCollectionsDataStructure$BothIndexOneExtractor");
            mapConfig.addMapAttributeConfig(reducedNameAttribute);

            MapAttributeConfig indexOneNameAttribute = new AbstractExtractionTest.TestMapAttributeIndexConfig();
            indexOneNameAttribute.setName("limbs[any].fingers[any]");
            indexOneNameAttribute.setExtractor(
                    "com.hazelcast.query.impl.extraction.MultipleCollectionsDataStructure$BothReducedExtractor");
            mapConfig.addMapAttributeConfig(indexOneNameAttribute);

            MapAttributeConfig reducedPowerAttribute = new AbstractExtractionTest.TestMapAttributeIndexConfig();
            reducedPowerAttribute.setName("limbs[any].fingers[1]");
            reducedPowerAttribute.setExtractor(
                    "com.hazelcast.query.impl.extraction.MultipleCollectionsDataStructure$FirstReducedExtractor");
            mapConfig.addMapAttributeConfig(reducedPowerAttribute);

            MapAttributeConfig indexOnePowerAttribute = new AbstractExtractionTest.TestMapAttributeIndexConfig();
            indexOnePowerAttribute.setName("limbs[1].fingers[any]");
            indexOnePowerAttribute.setExtractor(
                    "com.hazelcast.query.impl.extraction.MultipleCollectionsDataStructure$LastReducedExtractor");
            mapConfig.addMapAttributeConfig(indexOnePowerAttribute);
        }
    }

}
