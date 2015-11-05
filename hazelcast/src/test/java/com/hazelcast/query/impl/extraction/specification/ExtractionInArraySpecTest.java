package com.hazelcast.query.impl.extraction.specification;

import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.query.Predicates;
import com.hazelcast.query.QueryException;
import com.hazelcast.query.impl.extraction.AbstractExtractionTest;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Collection;

import static com.hazelcast.config.InMemoryFormat.BINARY;
import static com.hazelcast.config.InMemoryFormat.OBJECT;
import static com.hazelcast.query.impl.extraction.AbstractExtractionSpecification.Index.NO_INDEX;
import static com.hazelcast.query.impl.extraction.AbstractExtractionSpecification.Index.ORDERED;
import static com.hazelcast.query.impl.extraction.AbstractExtractionSpecification.Index.UNORDERED;
import static com.hazelcast.query.impl.extraction.AbstractExtractionSpecification.Multivalue.ARRAY;
import static com.hazelcast.query.impl.extraction.specification.ComplexDataStructure.Finger;
import static com.hazelcast.query.impl.extraction.specification.ComplexDataStructure.Person;
import static com.hazelcast.query.impl.extraction.specification.ComplexDataStructure.finger;
import static com.hazelcast.query.impl.extraction.specification.ComplexDataStructure.limb;
import static com.hazelcast.query.impl.extraction.specification.ComplexDataStructure.person;
import static com.hazelcast.query.impl.extraction.specification.ComplexDataStructure.tattoos;
import static java.util.Arrays.asList;

/**
 * Specification test that verifies the behavior of corner-cases extraction in arrays ONLY.
 * <p/>
 * Extraction mechanism: IN-BUILT REFLECTION EXTRACTION
 * <p>
 * This test is parametrised on two axes (see the parametrisationData() method):
 * - in memory format
 * - indexing
 */
@RunWith(Parameterized.class)
@Category({QuickTest.class, ParallelTest.class})
public class ExtractionInArraySpecTest extends AbstractExtractionTest {

    private static final Person BOND = person("Bond",
            limb("left-hand", tattoos(), finger("thumb"), finger(null)),
            limb("right-hand", tattoos("knife"), finger("middle"), finger("index"))
    );

    private static final Person KRUEGER = person("Krueger",
            limb("linke-hand", tattoos("bratwurst"), finger("Zeigefinger"), finger("Mittelfinger")),
            limb("rechte-hand", tattoos(), finger("Ringfinger"), finger("Daumen"))
    );

    private static final Person HUNT = person("Hunt",
            limb("left", null, new Finger[]{})
    );

    public ExtractionInArraySpecTest(InMemoryFormat inMemoryFormat, Index index, Multivalue multivalue) {
        super(inMemoryFormat, index, multivalue);
    }

    @Test
    @Ignore
    // TODO Checking array's length does not work for now
    public void lengthProperty() {
        execute(Input.of(BOND, KRUEGER),
                Query.of(Predicates.equal("limbs_[0].tattoos_.length", 1), mv),
                Expected.of(KRUEGER));
    }

    @Test
    @Ignore
    // TODO remove ignore when lengthProperty() test fixed
    public void null_arrayOrCollection_size() {
        execute(Input.of(HUNT),
                Query.of(Predicates.equal("limbs_[0].fingers_.length", 1), mv),
                Expected.of(QueryException.class));
    }

    @Test
    // TODO remove ignore when lengthProperty() test fixed
    public void null_arrayOrCollection_size_reduced() {
        execute(Input.of(HUNT),
                Query.of(Predicates.equal("limbs_[any].fingers_.length", 1), mv),
                Expected.of(QueryException.class));
    }

    @Parameterized.Parameters(name = "{index}: {0}, {1}, {2}")
    public static Collection<Object[]> parametrisationData() {
        return axes(
                asList(BINARY, OBJECT),
                asList(NO_INDEX, UNORDERED, ORDERED),
                asList(ARRAY)
        );
    }

}
