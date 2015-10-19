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

import static com.hazelcast.query.impl.extraction.SingleCollectionDataStructure.Person;
import static com.hazelcast.query.impl.extraction.SingleCollectionDataStructure.limb;
import static com.hazelcast.query.impl.extraction.SingleCollectionDataStructure.person;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertThat;

@RunWith(Parameterized.class)
public class SingleCollectionReflectionExtractionTest extends AbstractExtractionTest {

    private static final Person BOND = person(limb("left", 49), limb("right", 51));
    private static final Person KRUEGER = person(limb("links", 27), limb("rechts", 29));

    public SingleCollectionReflectionExtractionTest(InMemoryFormat inMemoryFormat, Index index) {
        super(inMemoryFormat, index);
    }

    @Override
    public List<String> getIndexedAttributes() {
        return Arrays.asList("limbs[1].power", "limbs[any].power", "limbs[1].name", "limbs[any].name");
    }

    @Test
    public void equals_predicate() {
        // GIVEN
        setup(getConfigurator());
        map.put("bond", BOND);
        map.put("krueger", KRUEGER);

        // WHEN
        Predicate predicate = Predicates.equal("limbs[1].power", 51);
        Collection<Person> values = map.values(predicate);

        // THEN
        assertThat(values, hasSize(1));
        assertThat(values, contains(BOND));
    }

    @Test
    public void equals_predicate_reduced() {
        // GIVEN
        setup(getConfigurator());
        map.put("bond", BOND);
        map.put("krueger", KRUEGER);

        // WHEN
        Predicate predicate = Predicates.equal("limbs[any].power", 51);
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
        map.put("krueger", KRUEGER);

        // WHEN
        Predicate predicate = Predicates.between("limbs[1].power", 40, 60);
        Collection<Person> values = map.values(predicate);

        // THEN
        assertThat(values, hasSize(1));
        assertThat(values, contains(BOND));
    }

    @Test
    public void between_predicate_reduced() {
        // GIVEN
        setup(getConfigurator());
        map.put("bond", BOND);
        map.put("krueger", KRUEGER);

        // WHEN
        Predicate predicate = Predicates.between("limbs[any].power", 20, 40);
        Collection<Person> values = map.values(predicate);

        // THEN
        assertThat(values, hasSize(1));
        assertThat(values, contains(KRUEGER));
    }

    @Test
    public void greater_less_predicate() {
        // GIVEN
        setup(getConfigurator());
        map.put("bond", BOND);
        map.put("krueger", KRUEGER);

        // WHEN
        Predicate predicate = Predicates.lessEqual("limbs[1].power", 29);
        Collection<Person> values = map.values(predicate);

        // THEN
        assertThat(values, hasSize(1));
        assertThat(values, contains(KRUEGER));
    }

    @Test
    public void greater_less_predicate_reduced() {
        // GIVEN
        setup(getConfigurator());
        map.put("bond", BOND);
        map.put("krueger", KRUEGER);

        // WHEN
        Predicate predicate = Predicates.lessEqual("limbs[any].power", 27);
        Collection<Person> values = map.values(predicate);

        // THEN
        assertThat(values, hasSize(1));
        assertThat(values, contains(KRUEGER));
    }

    @Test
    public void in_predicate() {
        // GIVEN
        setup(getConfigurator());
        map.put("bond", BOND);
        map.put("krueger", KRUEGER);

        // WHEN
        Predicate predicate = Predicates.in("limbs[1].power", 28, 29, 30);
        Collection<Person> values = map.values(predicate);

        // THEN
        assertThat(values, hasSize(1));
        assertThat(values, contains(KRUEGER));
    }

    @Test
    public void in_predicate_reduced() {
        // GIVEN
        setup(getConfigurator());
        map.put("bond", BOND);
        map.put("krueger", KRUEGER);

        // WHEN
        Predicate predicate = Predicates.in("limbs[any].power", 26, 27, 51);
        Collection<Person> values = map.values(predicate);

        // THEN
        assertThat(values, hasSize(2));
        assertThat(values, containsInAnyOrder(BOND, KRUEGER));
    }

    @Test
    public void notEqual_predicate() {
        // GIVEN
        setup(getConfigurator());
        map.put("bond", BOND);
        map.put("krueger", KRUEGER);

        // WHEN
        Predicate predicate = Predicates.notEqual("limbs[1].power", 29);
        Collection<Person> values = map.values(predicate);

        // THEN
        assertThat(values, hasSize(1));
        assertThat(values, contains(BOND));
    }

    @Test
    public void notEqual_predicate_reduced() {
        // GIVEN
        setup(getConfigurator());
        map.put("bond", BOND);
        map.put("krueger", KRUEGER);

        // WHEN
        Predicate predicate = Predicates.notEqual("limbs[any].power", 27);
        Collection<Person> values = map.values(predicate);

        // THEN
        assertThat(values, hasSize(1));
        assertThat(values, contains(BOND));
    }

    @Test
    public void like_predicate() {
        // GIVEN
        setup(getConfigurator());
        map.put("bond", BOND);
        map.put("krueger", KRUEGER);

        // WHEN
        Predicate predicate = Predicates.like("limbs[1].name", "recht_");
        Collection<Person> values = map.values(predicate);

        // THEN
        assertThat(values, hasSize(1));
        assertThat(values, contains(KRUEGER));
    }

    @Test
    public void like_predicate_reduced() {
        // GIVEN
        setup(getConfigurator());
        map.put("bond", BOND);
        map.put("krueger", KRUEGER);

        // WHEN
        Predicate predicate = Predicates.like("limbs[any].name", "le%");
        Collection<Person> values = map.values(predicate);

        // THEN
        assertThat(values, hasSize(1));
        assertThat(values, contains(BOND));
    }

    @Test
    public void ilike_predicate() {
        // GIVEN
        setup(getConfigurator());
        map.put("bond", BOND);
        map.put("krueger", KRUEGER);

        // WHEN
        Predicate predicate = Predicates.ilike("limbs[1].name", "REcht_");
        Collection<Person> values = map.values(predicate);

        // THEN
        assertThat(values, hasSize(1));
        assertThat(values, contains(KRUEGER));
    }

    @Test
    public void ilike_predicate_reduced() {
        // GIVEN
        setup(getConfigurator());
        map.put("bond", BOND);
        map.put("krueger", KRUEGER);

        // WHEN
        Predicate predicate = Predicates.ilike("limbs[any].name", "LE%");
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
        map.put("krueger", KRUEGER);

        // WHEN
        Predicate predicate = Predicates.regex("limbs[1].name", "ri.*");
        Collection<Person> values = map.values(predicate);

        // THEN
        assertThat(values, hasSize(1));
        assertThat(values, contains(BOND));
    }

    @Test
    public void regex_predicate_reduced() {
        // GIVEN
        setup(getConfigurator());
        map.put("bond", BOND);
        map.put("krueger", KRUEGER);

        // WHEN
        Predicate predicate = Predicates.regex("limbs[any].name", "li.*");
        Collection<Person> values = map.values(predicate);

        // THEN
        assertThat(values, hasSize(1));
        assertThat(values, contains(KRUEGER));
    }

}
