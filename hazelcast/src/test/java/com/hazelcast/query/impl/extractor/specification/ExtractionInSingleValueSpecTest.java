package com.hazelcast.query.impl.extractor.specification;

import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.query.Predicates;
import com.hazelcast.query.QueryException;
import com.hazelcast.query.impl.extractor.AbstractExtractionTest;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Collection;

import static com.hazelcast.config.InMemoryFormat.BINARY;
import static com.hazelcast.config.InMemoryFormat.OBJECT;
import static com.hazelcast.query.impl.extractor.AbstractExtractionSpecification.Index.NO_INDEX;
import static com.hazelcast.query.impl.extractor.AbstractExtractionSpecification.Index.ORDERED;
import static com.hazelcast.query.impl.extractor.AbstractExtractionSpecification.Index.UNORDERED;
import static com.hazelcast.query.impl.extractor.AbstractExtractionSpecification.Multivalue.SINGLE_VALUE;
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
public class ExtractionInSingleValueSpecTest extends AbstractExtractionTest {

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

    public ExtractionInSingleValueSpecTest(InMemoryFormat inMemoryFormat, Index index, Multivalue multivalue) {
        super(inMemoryFormat, index, multivalue);
    }

    @Test
    public void wrong_attribute_name() {
        execute(Input.of(BOND, KRUEGER, HUNT_WITH_NULLS),
                Query.of(Predicates.equal("name12312", "Bond"), mv),
                Expected.of(QueryException.class));
    }

    @Test
    public void wrong_attribute_name_compared_to_null() {
        execute(Input.of(BOND, KRUEGER, HUNT_WITH_NULLS),
                Query.of(Predicates.equal("name12312", null), mv),
                Expected.of(QueryException.class));
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
                asList(SINGLE_VALUE)
        );
    }

}
