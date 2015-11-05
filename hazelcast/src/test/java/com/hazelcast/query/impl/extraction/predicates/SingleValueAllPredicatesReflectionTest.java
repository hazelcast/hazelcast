package com.hazelcast.query.impl.extraction.predicates;

import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.query.Predicates;
import com.hazelcast.query.impl.extraction.AbstractExtractionTest;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
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
import static com.hazelcast.query.impl.extraction.AbstractExtractionSpecification.Multivalue.SINGLE_VALUE;
import static com.hazelcast.query.impl.extraction.predicates.SingleValueDataStructure.Person;
import static com.hazelcast.query.impl.extraction.predicates.SingleValueDataStructure.person;
import static java.util.Arrays.asList;

/**
 * Tests whether all predicates work with the extraction in attributes that are not collections.
 * <p/>
 * Extraction mechanism: IN-BUILT REFLECTION EXTRACTION
 * <p/>
 * This test is parametrised:
 * - each test is executed separately for BINARY and OBJECT in memory format
 * - each test is executed separately having each query using NO_INDEX, UNORDERED_INDEX and ORDERED_INDEX.
 * In this way we are spec-testing most of the reasonable combinations of the configuration of map & extraction.
 */
@RunWith(Parameterized.class)
@Category({QuickTest.class, ParallelTest.class})
public class SingleValueAllPredicatesReflectionTest extends AbstractExtractionTest {

    private static final Person BOND = person(130);
    private static final Person HUNT = person(120);

    public SingleValueAllPredicatesReflectionTest(InMemoryFormat inMemoryFormat, Index index, Multivalue multivalue) {
        super(inMemoryFormat, index, multivalue);
    }

    @Test
    public void equals_predicate() {
        execute(Input.of(BOND, HUNT),
                Query.of(Predicates.equal("brain.iq", 130), mv),
                Expected.of(BOND));
    }

    @Test
    public void between_predicate() {
        execute(Input.of(BOND, HUNT),
                Query.of(Predicates.between("brain.iq", 115, 135), mv),
                Expected.of(BOND, HUNT));
    }

    @Test
    public void greater_less_predicate() {
        execute(Input.of(BOND, HUNT),
                Query.of(Predicates.lessEqual("brain.iq", 120), mv),
                Expected.of(HUNT));
    }

    @Test
    public void in_predicate() {
        execute(Input.of(BOND, HUNT),
                Query.of(Predicates.in("brain.iq", 120, 121, 122), mv),
                Expected.of(HUNT));
    }

    @Test
    public void notEqual_predicate() {
        execute(Input.of(BOND, HUNT),
                Query.of(Predicates.notEqual("brain.iq", 130), mv),
                Expected.of(HUNT));
    }

    @Test
    public void like_predicate() {
        execute(Input.of(BOND, HUNT),
                Query.of(Predicates.like("brain.name", "brain12_"), mv),
                Expected.of(HUNT));
    }

    @Test
    public void ilike_predicate() {
        execute(Input.of(BOND, HUNT),
                Query.of(Predicates.ilike("brain.name", "BR%130"), mv),
                Expected.of(BOND));
    }

    @Test
    public void regex_predicate() {
        execute(Input.of(BOND, HUNT),
                Query.of(Predicates.regex("brain.name", "brain13.*"), mv),
                Expected.of(BOND));
    }

    @Test
    public void key_equal_predicate() {
        execute(Input.of(BOND, HUNT),
                Query.of(Predicates.equal("__key", 0), mv),
                Expected.of(BOND));
    }

    @Parameterized.Parameters(name = "{index}: {0}, {1}, {2}")
    public static Collection<Object[]> data() {
        return axes(
                asList(BINARY, OBJECT),
                asList(NO_INDEX, UNORDERED, ORDERED),
                asList(SINGLE_VALUE)
        );
    }

}
