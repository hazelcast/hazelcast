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

// PRD: CASE 2
public class SingleCollectionDataStructure {

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
        person.limbs.addAll(Arrays.asList(limbs));
        return person;
    }

    public static class IndexOneLimbPowerExtractor extends ValueExtractor {
        @Override
        public Object extract(Object target) {
            return ((Person) target).limbs.get(1).power;
        }
    }

    public static class IndexOneLimbNameExtractor extends ValueExtractor {
        @Override
        public Object extract(Object target) {
            return ((Person) target).limbs.get(1).name;
        }
    }

    public static class ReducedLimbPowerExtractor extends ValueExtractor {
        @Override
        public Object extract(Object target) {
            MultiResult multiResult = new MultiResult();
            for (Limb limb : ((Person) target).limbs) {
                multiResult.add(limb.power);
            }
            return multiResult;
        }
    }

    public static class ReducedLimbNameExtractor extends ValueExtractor {
        @Override
        public Object extract(Object target) {
            MultiResult multiResult = new MultiResult();
            for (Limb limb : ((Person) target).limbs) {
                multiResult.add(limb.name);
            }
            return multiResult;
        }
    }

    public static class LimbExtractorsConfigurator extends AbstractExtractionTest.Configurator {
        @Override
        public void doWithConfig(Config config) {
            MapConfig mapConfig = config.getMapConfig("map");

            MapAttributeConfig reducedNameAttribute = new AbstractExtractionTest.TestMapAttributeIndexConfig();
            reducedNameAttribute.setName("limb[any].name");
            reducedNameAttribute.setExtractor(
                    "com.hazelcast.query.impl.extraction.SingleCollectionDataStructure$ReducedLimbNameExtractor");
            mapConfig.addMapAttributeConfig(reducedNameAttribute);

            MapAttributeConfig indexOneNameAttribute = new AbstractExtractionTest.TestMapAttributeIndexConfig();
            indexOneNameAttribute.setName("limb[1].name");
            indexOneNameAttribute.setExtractor(
                    "com.hazelcast.query.impl.extraction.SingleCollectionDataStructure$IndexOneLimbNameExtractor");
            mapConfig.addMapAttributeConfig(indexOneNameAttribute);

            MapAttributeConfig reducedPowerAttribute = new AbstractExtractionTest.TestMapAttributeIndexConfig();
            reducedPowerAttribute.setName("limb[any].power");
            reducedPowerAttribute.setExtractor(
                    "com.hazelcast.query.impl.extraction.SingleCollectionDataStructure$ReducedLimbPowerExtractor");
            mapConfig.addMapAttributeConfig(reducedPowerAttribute);

            MapAttributeConfig indexOnePowerAttribute = new AbstractExtractionTest.TestMapAttributeIndexConfig();
            indexOnePowerAttribute.setName("limb[1].power");
            indexOnePowerAttribute.setExtractor(
                    "com.hazelcast.query.impl.extraction.SingleCollectionDataStructure$IndexOneLimbPowerExtractor");
            mapConfig.addMapAttributeConfig(indexOnePowerAttribute);
        }
    }

}
