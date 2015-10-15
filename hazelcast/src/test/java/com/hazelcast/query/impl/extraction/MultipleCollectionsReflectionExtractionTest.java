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

import static com.hazelcast.query.impl.extraction.MultipleCollectionsDataStructure.Person;
import static com.hazelcast.query.impl.extraction.MultipleCollectionsDataStructure.limb;
import static com.hazelcast.query.impl.extraction.MultipleCollectionsDataStructure.person;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertThat;


@RunWith(Parameterized.class)
public class MultipleCollectionsReflectionExtractionTest extends AbstractExtractionTest {

    private static final Person BOND = person(limb("thumb", "pinky"), limb("middle", "index"));
    private static final Person KRUEGER = person(limb("Zeigefinger", "Mittelfinger"),
            limb("Ringfinger", "Mittelfinger"));

    public MultipleCollectionsReflectionExtractionTest(InMemoryFormat inMemoryFormat, Index index) {
        super(inMemoryFormat, index);
    }

    @Override
    public List<String> getIndexedAttributes() {
        return Arrays.asList("limbs[1].fingers[1]", "limbs[1].fingers[*]",
                "limbs[*].fingers[1]", "limbs[1].fingers[1]");
    }

    @Test
    public void equals_predicate() {
        // GIVEN
        setup(getConfigurator());
        map.put("bond", BOND);
        map.put("krueger", KRUEGER);

        // WHEN
        Predicate predicate = Predicates.equal("limbs[1].fingers[1]", "index");
        Collection<Person> values = map.values(predicate);

        // THEN
        assertThat(values, hasSize(1));
        assertThat(values, contains(BOND));
    }

    @Test
    public void equals_predicate_reducedLast() {
        // GIVEN
        setup(getConfigurator());
        map.put("bond", BOND);
        map.put("krueger", KRUEGER);

        // WHEN
        Predicate predicate = Predicates.equal("limbs[1].fingers[*]", "Ringfinger");
        Collection<Person> values = map.values(predicate);

        // THEN
        assertThat(values, hasSize(1));
        assertThat(values, contains(KRUEGER));
    }

    @Test
    public void equals_predicate_reducedFirst() {
        // GIVEN
        setup(getConfigurator());
        map.put("bond", BOND);
        map.put("krueger", KRUEGER);

        // WHEN
        Predicate predicate = Predicates.equal("limbs[*].fingers[1]", "Mittelfinger");
        Collection<Person> values = map.values(predicate);

        // THEN
        assertThat(values, hasSize(1));
        assertThat(values, contains(KRUEGER));
    }

    @Test
    public void equals_predicate_reducedBoth() {
        // GIVEN
        setup(getConfigurator());
        map.put("bond", BOND);
        map.put("krueger", KRUEGER);

        // WHEN
        Predicate predicate = Predicates.equal("limbs[*].fingers[*]", "Ringfinger");
        Collection<Person> values = map.values(predicate);

        // THEN
        assertThat(values, hasSize(1));
        assertThat(values, contains(KRUEGER));
    }

}
