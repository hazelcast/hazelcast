package com.hazelcast.query.impl.extractor.specification;

import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapAttributeConfig;
import com.hazelcast.config.MapConfig;
import com.hazelcast.query.Predicates;
import com.hazelcast.query.extractor.ValueCollector;
import com.hazelcast.query.extractor.ValueExtractor;
import com.hazelcast.query.impl.extractor.AbstractExtractionTest;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Collection;

import static com.hazelcast.config.InMemoryFormat.OBJECT;
import static com.hazelcast.query.impl.extractor.AbstractExtractionSpecification.Index.NO_INDEX;
import static com.hazelcast.query.impl.extractor.AbstractExtractionSpecification.Multivalue.LIST;
import static com.hazelcast.query.impl.extractor.specification.ComplexDataStructure.Person;
import static com.hazelcast.query.impl.extractor.specification.ComplexDataStructure.finger;
import static com.hazelcast.query.impl.extractor.specification.ComplexDataStructure.limb;
import static com.hazelcast.query.impl.extractor.specification.ComplexDataStructure.person;
import static com.hazelcast.query.impl.extractor.specification.ComplexDataStructure.tattoos;
import static java.util.Arrays.asList;

/**
 * Specification test that verifies the behavior of corner-cases extraction with extractor and arguments.
 * <p/>
 * Extraction mechanism: EXTRACTOR-BASED EXTRACTION
 * <p/>
 * This test is parametrised on two axes (see the parametrisationData() method):
 * - in memory format
 * - indexing
 * - extraction in collections and arrays
 */
@RunWith(Parameterized.class)
@Category({QuickTest.class, ParallelTest.class})
public class ExtractionWithExtractorsSpecTest extends AbstractExtractionTest {

    private static final Person BOND = person("Bond",
            limb("left-hand", tattoos(), finger("thumb"), finger(null)),
            limb("right-hand", tattoos("knife"), finger("middle"), finger("index"))
    );

    private static final Person KRUEGER = person("Krueger",
            limb("linke-hand", tattoos("bratwurst"), finger("Zeigefinger"), finger("Mittelfinger")),
            limb("rechte-hand", tattoos(), finger("Ringfinger"), finger("Daumen"))
    );

    private static final Person HUNT_NULL_LIMB = person("Hunt");

    public ExtractionWithExtractorsSpecTest(InMemoryFormat inMemoryFormat, Index index, Multivalue multivalue) {
        super(inMemoryFormat, index, multivalue);
    }

    @Test
    public void extractorWithParam_bondCase() {
        execute(Input.of(BOND, KRUEGER),
                Query.of(Predicates.equal("tattoosCount[1]", 1), mv),
                Expected.of(BOND));
    }

    @Test
    public void extractorWithParam_kruegerCase() {
        execute(Input.of(BOND, KRUEGER),
                Query.of(Predicates.equal("tattoosCount[0]", 1), mv),
                Expected.of(KRUEGER));
    }

    @Test
    public void extractorWithParam_indexOutOfBound() {
        execute(Input.of(BOND, KRUEGER, HUNT_NULL_LIMB),
                Query.of(Predicates.equal("tattoosCount[2]", 1), mv),
                Expected.of(IndexOutOfBoundsException.class));
    }

    @Test
    public void extractorWithParam_negativeInput() {
        execute(Input.of(BOND, KRUEGER, HUNT_NULL_LIMB),
                Query.of(Predicates.equal("tattoosCount[-1]", 1), mv),
                Expected.of(IndexOutOfBoundsException.class));
    }

    @Test
    public void extractorWithParam_wrongInput_noClosingWithArg() {
        execute(Input.of(BOND, KRUEGER, HUNT_NULL_LIMB),
                Query.of(Predicates.equal("tattoosCount[0", 1), mv),
                Expected.of(IllegalArgumentException.class));
    }

    @Test
    public void extractorWithParam_wrongInput_noOpeningWithArg() {
        execute(Input.of(BOND, KRUEGER, HUNT_NULL_LIMB),
                Query.of(Predicates.equal("tattoosCount0]", 1), mv),
                Expected.of(IllegalArgumentException.class));
    }

    @Test
    public void extractorWithParam_wrongInput_noClosing() {
        execute(Input.of(BOND, KRUEGER, HUNT_NULL_LIMB),
                Query.of(Predicates.equal("tattoosCount[", 1), mv),
                Expected.of(IllegalArgumentException.class));
    }

    @Test
    public void extractorWithParam_wrongInput_noOpening() {
        execute(Input.of(BOND, KRUEGER, HUNT_NULL_LIMB),
                Query.of(Predicates.equal("tattoosCount]", 1), mv),
                Expected.of(IllegalArgumentException.class));
    }

    @Test
    public void extractorWithParam_wrongInput_noArgumentWithBrackets() {
        execute(Input.of(BOND, KRUEGER, HUNT_NULL_LIMB),
                Query.of(Predicates.equal("tattoosCount[]", 1), mv),
                Expected.of(NumberFormatException.class));
    }

    @Test
    public void extractorWithParam_wrongInput_noArgumentNoBrackets() {
        execute(Input.of(BOND, KRUEGER, HUNT_NULL_LIMB),
                Query.of(Predicates.equal("tattoosCount", 1), mv),
                Expected.of(NumberFormatException.class));
    }

    @Test
    public void extractorWithParam_wrongInput_squareBracketsInInput() {
        execute(Input.of(BOND, KRUEGER, HUNT_NULL_LIMB),
                Query.of(Predicates.equal("tattoosCount[1183[2]3]", 1), mv),
                Expected.of(IllegalArgumentException.class));
    }

    @Test
    public void extractorWithParam_nullCollection() {
        execute(Input.of(HUNT_NULL_LIMB),
                Query.of(Predicates.equal("tattoosCount[0]", 1), mv),
                Expected.of(NullPointerException.class));
    }

    protected AbstractExtractionTest.Configurator getInstanceConfigurator() {
        return new AbstractExtractionTest.Configurator() {
            @Override
            public void doWithConfig(Config config, AbstractExtractionTest.Multivalue mv) {
                MapConfig mapConfig = config.getMapConfig("map");

                MapAttributeConfig tattoosCount = new AbstractExtractionTest.TestMapAttributeIndexConfig();
                tattoosCount.setName("tattoosCount");
                tattoosCount.setExtractor("com.hazelcast.query.impl.extractor.specification.ExtractionWithExtractorsSpecTest$LimbTattoosCountExtractor");
                mapConfig.addMapAttributeConfig(tattoosCount);
            }
        };
    }

    public static class LimbTattoosCountExtractor extends ValueExtractor<Person, String> {
        @Override
        public void extract(Person target, String arguments, ValueCollector collector) {
            Integer parsedId = Integer.parseInt(arguments);
            collector.addObject(target.limbs_list.get(parsedId).tattoos_list.size());
        }
    }

    @Parameterized.Parameters(name = "{index}: {0}, {1}, {2}")
    public static Collection<Object[]> parametrisationData() {
        return axes(
                asList(OBJECT),
                asList(NO_INDEX),
                asList(LIST)
        );
    }

}
