package com.hazelcast.query.impl.extraction;

import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.Predicates;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static com.hazelcast.query.impl.extraction.SingleValueDataStructure.Person;
import static com.hazelcast.query.impl.extraction.SingleValueDataStructure.person;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertThat;

/**
 * Covers all predicates from Predicates.*
 * It does not make sense to test: and, or, not, instanceof predicates in this context
 */
@RunWith(Parameterized.class)
public class SingleValueReflectionExtractionTest extends AbstractExtractionTest {

    private static final Person BOND = person(130);
    private static final Person HUNT = person(120);

    public SingleValueReflectionExtractionTest(InMemoryFormat inMemoryFormat, Index index) {
        super(inMemoryFormat, index);
    }

    @Override
    public List<String> getIndexedAttributes() {
        return Arrays.asList("brain.iq", "brain.name");
    }

    @Test
    public void equals_predicate() {
        // GIVEN
        setup(getConfigurator());
        map.put("bond", BOND);
        map.put("hunt", HUNT);

        // WHEN
        Predicate predicate = Predicates.equal("brain.iq", 130);
        Collection<Person> values = map.values(predicate);

        // THEN
        assertThat(values, hasSize(1));
        assertThat(values, contains(BOND));
    }

    @Test
    public void between_predicate() {
        // GIVEN
        setup(getConfigurator());
        map.put("bond", BOND);
        map.put("hunt", HUNT);

        // WHEN
        Predicate predicate = Predicates.between("brain.iq", 115, 135);
        Collection<Person> values = map.values(predicate);

        // THEN
        assertThat(values, hasSize(2));
        assertThat(values, containsInAnyOrder(BOND, HUNT));
    }

    @Test
    public void greater_less_predicate() {
        // GIVEN
        setup(getConfigurator());
        map.put("bond", BOND);
        map.put("hunt", HUNT);

        // WHEN
        Predicate predicate = Predicates.lessEqual("brain.iq", 120);
        Collection<Person> values = map.values(predicate);

        // THEN
        assertThat(values, hasSize(1));
        assertThat(values, contains(HUNT));
    }

    @Test
    public void in_predicate() {
        // GIVEN
        setup(getConfigurator());
        map.put("bond", BOND);
        map.put("hunt", HUNT);

        // WHEN
        Predicate predicate = Predicates.in("brain.iq", 120, 121, 122);
        Collection<Person> values = map.values(predicate);

        // THEN
        assertThat(values, hasSize(1));
        assertThat(values, contains(HUNT));
    }

    @Test
    public void notEqual_predicate() {
        // GIVEN
        setup(getConfigurator());
        map.put("bond", BOND);
        map.put("hunt", HUNT);

        // WHEN
        Predicate predicate = Predicates.notEqual("brain.iq", 130);
        Collection<Person> values = map.values(predicate);

        // THEN
        assertThat(values, hasSize(1));
        assertThat(values, contains(HUNT));
    }

    @Test
    public void like_predicate() {
        // GIVEN
        setup(getConfigurator());
        map.put("bond", BOND);
        map.put("hunt", HUNT);

        // WHEN
        Predicate predicate = Predicates.like("brain.name", "brain12_");
        Collection<Person> values = map.values(predicate);

        // THEN
        assertThat(values, hasSize(1));
        assertThat(values, contains(HUNT));
    }

    @Test
    public void ilike_predicate() {
        // GIVEN
        setup(getConfigurator());
        map.put("bond", BOND);
        map.put("hunt", HUNT);

        // WHEN
        Predicate predicate = Predicates.ilike("brain.name", "BR%130");
        Collection<Person> values = map.values(predicate);

        // THEN
        assertThat(values, hasSize(1));
        assertThat(values, contains(BOND));
    }

    @Test
    public void regex_predicate() {
        // GIVEN
        setup(getConfigurator());
        map.put("bond", BOND);
        map.put("hunt", HUNT);

        // WHEN
        Predicate predicate = Predicates.regex("brain.name", "brain13.*");
        Collection<Person> values = map.values(predicate);

        // THEN
        assertThat(values, hasSize(1));
        assertThat(values, contains(BOND));
    }

    @Test
    public void key_equal_predicate() {
        // GIVEN
        setup(getConfigurator());
        map.put("bond", BOND);
        map.put("hunt", HUNT);

        // WHEN
        Predicate predicate = Predicates.equal("__key", "bond");
        Collection<Person> values = map.values(predicate);

        // THEN
        assertThat(values, hasSize(1));
        assertThat(values, contains(BOND));
    }

}
