package com.hazelcast.query.impl.extraction;

import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.query.Predicates;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static com.hazelcast.query.impl.extraction.SingleValueDataStructure.Person;
import static com.hazelcast.query.impl.extraction.SingleValueDataStructure.person;

/**
 * Covers all predicates from Predicates.*
 * It does not make sense to test: and, or, not, instanceof predicates in this context
 */
@RunWith(Parameterized.class)
public class SingleValueReflectionExtractionTest extends AbstractExtractionTest {

    private static final Person BOND = person(130);
    private static final Person HUNT = person(120);

    public SingleValueReflectionExtractionTest(InMemoryFormat inMemoryFormat, Index index, Multivalue multivalue) {
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

}
