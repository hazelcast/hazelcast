package com.hazelcast.query.impl.extractor.specification;

import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.query.Predicates;
import com.hazelcast.query.impl.extractor.AbstractExtractionTest;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Collection;
import java.util.UUID;

import static com.hazelcast.config.InMemoryFormat.BINARY;
import static com.hazelcast.config.InMemoryFormat.OBJECT;
import static com.hazelcast.query.impl.extractor.AbstractExtractionSpecification.Index.NO_INDEX;
import static com.hazelcast.query.impl.extractor.AbstractExtractionSpecification.Index.ORDERED;
import static com.hazelcast.query.impl.extractor.AbstractExtractionSpecification.Index.UNORDERED;
import static com.hazelcast.query.impl.extractor.AbstractExtractionSpecification.Multivalue.PORTABLE;
import static com.hazelcast.query.impl.extractor.specification.ComplexDataStructure.Finger;
import static com.hazelcast.query.impl.extractor.specification.ComplexDataStructure.Person;
import static com.hazelcast.query.impl.extractor.specification.ComplexDataStructure.finger;
import static com.hazelcast.query.impl.extractor.specification.ComplexDataStructure.limb;
import static com.hazelcast.query.impl.extractor.specification.ComplexDataStructure.person;
import static com.hazelcast.query.impl.extractor.specification.ComplexDataStructure.tattoos;
import static java.util.Arrays.asList;

/**
 * Specification test that verifies the behavior of corner-cases extraction in single-value attributes.
 * <p/>
 * Extraction mechanism: IN-BUILT REFLECTION EXTRACTION
 * <p/>
 * This test is parametrised on two axes (see the parametrisationData() method):
 * - in memory format
 * - indexing
 */
@RunWith(Parameterized.class)
@Category({QuickTest.class, ParallelTest.class})
public class ExtractionInPortableSpecTest extends AbstractExtractionTest {

    private static final Person BOND = person("Bond",
            limb("left-hand", tattoos(), finger("thumb"), finger(null)),
            limb("right-hand", tattoos("knife"), finger("middle"), finger("index"))
    );

    private static final Person KRUEGER = person("Krueger",
            limb("linke-hand", tattoos("bratwurst"), finger("Zeigefinger"), finger("Mittelfinger")),
            limb("rechte-hand", tattoos(), finger("Ringfinger"), finger("Daumen"))
    );

    private static final Person HUNT_WITH_NULLS = person(null,
            limb(null, new ArrayList<String>(), new Finger[]{})
    );

    public ExtractionInPortableSpecTest(InMemoryFormat inMemoryFormat, Index index, Multivalue multivalue) {
        super(inMemoryFormat, index, multivalue);
    }

    protected Configurator getInstanceConfigurator() {
        return new Configurator() {
            @Override
            public void doWithConfig(Config config, Multivalue mv) {
                config.getSerializationConfig().addPortableFactory(ComplexDataStructure.PersonPortableFactory.ID, new ComplexDataStructure.PersonPortableFactory());
            }
        };
    }

    @Override
    protected void doWithMap() {
        // init fully populated object to handle nulls properly
        if (mv == PORTABLE) {
            String key = UUID.randomUUID().toString();
            map.put(key, KRUEGER.getPortable());
            map.remove(key);
        }
    }

    @Test
    public void wrong_attribute_name() {
        execute(Input.of(BOND, KRUEGER, HUNT_WITH_NULLS),
                Query.of(Predicates.equal("name12312", "Bond"), mv),
                Expected.empty());
    }

    @Test
    public void nested_wrong_attribute_name() {
        execute(Input.of(BOND, KRUEGER),
                Query.of(Predicates.equal("firstLimb.name12312", "left-hand"), mv),
                Expected.empty());
    }

    @Test
    @Ignore("Does not work for now - portables issue - see github issue #3927")
    public void nested_wrong_attribute_notAtLeaf_name() {
        execute(Input.of(BOND, KRUEGER),
                Query.of(Predicates.equal("firstLimb.newPortable.asdfasdf", "left-hand"), mv),
                Expected.empty());
    }

    @Test
    public void indexOutOfBound_notExistingProperty() {
        execute(Input.of(BOND, KRUEGER),
                Query.of(equal("limbs_[100].sdafasdf", "knife"), mv),
                Expected.empty());
    }

    @Test
    @Ignore("Does not work for now - portables issue - see github issue #3927")
    public void indexOutOfBound_notExistingProperty_notAtLeaf() {
        execute(Input.of(BOND, KRUEGER),
                Query.of(equal("limbs_[100].sdafasdf.zxcvzxcv", "knife"), mv),
                Expected.empty());
    }

    @Test
    public void indexOutOfBound_atLeaf_notExistingProperty() {
        execute(Input.of(BOND, KRUEGER),
                Query.of(equal("limbs_[0].fingers_[100].asdfas", "knife"), mv),
                Expected.empty());
    }

    @Test
    public void wrong_attribute_name_compared_to_null() {
        execute(Input.of(BOND, KRUEGER, HUNT_WITH_NULLS),
                Query.of(Predicates.equal("name12312", null), mv),
                Expected.of(BOND, KRUEGER, HUNT_WITH_NULLS));
    }

    @Test
    public void primitiveNull_comparedToNull_matching() {
        execute(Input.of(BOND, KRUEGER, HUNT_WITH_NULLS),
                Query.of(Predicates.equal("name", null), mv),
                Expected.of(HUNT_WITH_NULLS));
    }

    @Test
    public void primitiveNull_comparedToNotNull_notMatching() {
        execute(Input.of(BOND, KRUEGER, HUNT_WITH_NULLS),
                Query.of(Predicates.equal("name", "Non-null-value"), mv),
                Expected.empty());
    }

    @Test
    public void nestedAttribute_firstIsNull_comparedToNotNull() {
        execute(Input.of(BOND, KRUEGER, HUNT_WITH_NULLS),
                Query.of(Predicates.equal("secondLimb.name", "Non-null-value"), mv),
                Expected.empty());
    }

    @Test
    public void nestedAttribute_firstIsNull_comparedToNull() {
        execute(Input.of(BOND, KRUEGER, HUNT_WITH_NULLS),
                Query.of(Predicates.equal("secondLimb.name", null), mv),
                Expected.of(HUNT_WITH_NULLS));
    }

    @Parameterized.Parameters(name = "{index}: {0}, {1}, {2}")
    public static Collection<Object[]> parametrisationData() {
        return axes(
                asList(BINARY, OBJECT),
                asList(NO_INDEX, UNORDERED, ORDERED),
                asList(PORTABLE)
        );
    }


//    @Test
//    public void correct_attribute_name() {
//        execute(Input.of(PORSCHE),
//                Query.of(Predicates.equal("name", "Porsche"), mv),
//                Expected.of(PORSCHE));
//    }
//
//    @Test
//    public void correct_nestedAttribute_name() {
//        execute(Input.of(PORSCHE),
//                Query.of(Predicates.equal("engine.power", 300), mv),
//                Expected.of(PORSCHE));
//    }
//
//    @Test
//    public void correct_portableAttribute() {
//        execute(Input.of(PORSCHE),
//                Query.of(Predicates.equal("engine", PORSCHE.engine), mv),
//                Expected.of(PORSCHE));
//    }
//
//    @Test
//    public void correct_portableNestedAttribute() {
//        execute(Input.of(PORSCHE),
//                Query.of(Predicates.equal("engine.chip", PORSCHE.engine.chip), mv),
//                Expected.of(PORSCHE));
//    }
//
//    @Test
//    public void correct_portableArrayInTheMiddle_matching() {
//        execute(Input.of(PORSCHE),
//                Query.of(Predicates.equal("wheels[0].name", "front"), mv),
//                Expected.of(PORSCHE));
//    }
//
//    @Test
//    public void correct_portableArrayInTheMiddle_notMatching() {
//        execute(Input.of(PORSCHE),
//                Query.of(Predicates.equal("wheels[1].name", "front"), mv),
//                Expected.empty());
//    }
//
//    @Test
//    public void correct_portableArrayInTheMiddle_portableAtTheEnd_matching() {
//        execute(Input.of(PORSCHE),
//                Query.of(Predicates.equal("wheels[0].chip", ((PortableDataStructure.WheelPortable) PORSCHE.wheels[0]).chip), mv),
//                Expected.of(PORSCHE));
//    }
//
//    @Test
//    public void correct_portableArrayInTheMiddle_portableAtTheEnd_notMatching() {
//        execute(Input.of(PORSCHE),
//                Query.of(Predicates.equal("wheels[0].chip", new PortableDataStructure.ChipPortable(123)), mv),
//                Expected.empty());
//    }
//
//    @Test
//    public void correct_portableArrayInTheMiddle_portableArrayAtTheEnd_matching() {
//        execute(Input.of(PORSCHE),
//                Query.of(Predicates.equal("wheels[0].chips[1]", (PortableDataStructure.ChipPortable) (((PortableDataStructure.WheelPortable) PORSCHE.wheels[0]).chips)[1]), mv),
//                Expected.of(PORSCHE));
//    }
//
//    @Test
//    public void correct_portableArrayInTheMiddle_portableArrayAtTheEnd_primitiveAttribute_notMatching() {
//        execute(Input.of(PORSCHE),
//                Query.of(Predicates.equal("wheels[0].chips[0].power", 15), mv),
//                Expected.empty());
//    }
//
//    @Test
//    public void correct_portableArrayInTheMiddle_portableArrayAtTheEnd_primitiveAttribute_matching() {
//        execute(Input.of(PORSCHE),
//                Query.of(Predicates.equal("wheels[0].chips[0].power", 20), mv),
//                Expected.of(PORSCHE));
//    }
//
//    @Test
//    public void correct_portableArrayInTheMiddle_portableArrayAtTheEnd_notMatching() {
//        execute(Input.of(PORSCHE),
//                Query.of(Predicates.equal("wheels[0].chips[0]", (PortableDataStructure.ChipPortable) (((PortableDataStructure.WheelPortable) PORSCHE.wheels[0]).chips)[1]), mv),
//                Expected.empty());
//    }
//
//    @Test
//    public void correct_portableArrayInTheMiddle_primitiveArrayAtTheEnd_matching() {
//        execute(Input.of(PORSCHE),
//                Query.of(Predicates.equal("wheels[0].serial[1]", 12), mv),
//                Expected.of(PORSCHE));
//    }
//
//    @Test
//    public void correct_portableArrayInTheMiddle_primitiveArrayAtTheEnd_notMatching() {
//        execute(Input.of(PORSCHE),
//                Query.of(Predicates.equal("wheels[0].serial[1]", 123), mv),
//                Expected.empty());
//    }
//
//    @Test
//    public void correct_portableArrayAtTheEnd_matching() {
//        execute(Input.of(PORSCHE),
//                Query.of(Predicates.equal("wheels[0]", (PortableDataStructure.WheelPortable) PORSCHE.wheels[0]), mv),
//                Expected.of(PORSCHE));
//    }
//
//    @Test
//    public void correct_portableArrayAtTheEnd_notMatching() {
//        execute(Input.of(PORSCHE),
//                Query.of(Predicates.equal("wheels[1]", (PortableDataStructure.WheelPortable) PORSCHE.wheels[0]), mv),
//                Expected.empty());
//    }
//
//    @Test
//    public void correct_primitiveArrayAtTheEnd_matching() {
//        execute(Input.of(PORSCHE),
//                Query.of(Predicates.equal("model[0]", "911"), mv),
//                Expected.of(PORSCHE));
//    }
//
//    @Test
//    public void correct_primitiveArrayAtTheEnd_notMatching() {
//        execute(Input.of(PORSCHE),
//                Query.of(Predicates.equal("model[0]", "956"), mv),
//                Expected.empty());
//    }
//
//    @Test
//    public void correct_primitiveArrayAtTheEnd_matchingX() {
//        PortableDataStructure.XPortable x = new PortableDataStructure.XPortable();
//
//        execute(Input.of(x),
//                Query.of(Predicates.equal("chips[0].serial[0]", 41), mv),
//                Expected.of(x));
//    }
//
//    @Test
//    public void correct_primitiveArrayAtTheEnd_notMatchingX() {
//        PortableDataStructure.XPortable x = new PortableDataStructure.XPortable();
//
//        execute(Input.of(x),
//                Query.of(Predicates.equal("chips[0].serial[0]", 10), mv),
//                Expected.empty());
//    }

    // TODO no object on the server classpath -> ClientMapStandaloneTest

//    @Test
//    @Ignore("Does not throw exception for now - inconsistent with other query mechanisms")
//    public void wrong_attribute_name() {
//        execute(Input.of(PORSCHE),
//                Query.of(Predicates.equal("name12312", "Porsche"), mv),
//                Expected.of(QueryException.class));
//    }

//    @Test
//    public void wrong_attribute_name_compared_to_null() {
//        execute(Input.of(BOND, KRUEGER, HUNT_WITH_NULLS),
//                Query.of(Predicates.equal("name12312", null), mv),
//                Expected.of(QueryException.class));
//    }
//
//    @Test
//    public void primitiveNull_comparedToNull_matching() {
//        execute(Input.of(BOND, KRUEGER, HUNT_WITH_NULLS),
//                Query.of(Predicates.equal("name", null), mv),
//                Expected.of(HUNT_WITH_NULLS));
//    }
//
//    @Test
//    public void primitiveNull_comparedToNotNull_notMatching() {
//        execute(Input.of(BOND, KRUEGER, HUNT_WITH_NULLS),
//                Query.of(Predicates.equal("name", "Non-null-value"), mv),
//                Expected.empty());
//    }
//
//    @Test
//    public void nestedAttribute_firstIsNull_comparedToNotNull() {
//        execute(Input.of(BOND, KRUEGER, HUNT_WITH_NULLS),
//                Query.of(Predicates.equal("secondLimb.name", "Non-null-value"), mv),
//                Expected.empty());
//    }
//
//    @Test
//    public void nestedAttribute_firstIsNull_comparedToNull() {
//        execute(Input.of(BOND, KRUEGER, HUNT_WITH_NULLS),
//                Query.of(Predicates.equal("secondLimb.name", null), mv),
//                Expected.of(HUNT_WITH_NULLS));
//    }

}
