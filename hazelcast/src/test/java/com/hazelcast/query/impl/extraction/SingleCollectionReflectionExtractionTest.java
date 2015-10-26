package com.hazelcast.query.impl.extraction;

import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.query.Predicates;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static com.hazelcast.query.impl.extraction.SingleCollectionDataStructure.Person;
import static com.hazelcast.query.impl.extraction.SingleCollectionDataStructure.limb;
import static com.hazelcast.query.impl.extraction.SingleCollectionDataStructure.person;

@RunWith(Parameterized.class)
@Category({QuickTest.class, ParallelTest.class})
public class SingleCollectionReflectionExtractionTest extends AbstractExtractionTest {

    private static final Person BOND = person(limb("left", 49), limb("right", 51));
    private static final Person KRUEGER = person(limb("links", 27), limb("rechts", 29));

    public SingleCollectionReflectionExtractionTest(InMemoryFormat inMemoryFormat, Index index, Multivalue multivalue) {
        super(inMemoryFormat, index, multivalue);
    }

    @Test
    public void equals_predicate() {
        execute(Input.of(BOND, KRUEGER),
                Query.of(Predicates.equal("limbs_[1].power", 51), mv),
                Expected.of(BOND));
    }

    @Test
    public void equals_predicate_reduced() {
        execute(Input.of(BOND, KRUEGER),
                Query.of(Predicates.equal("limbs_[any].power", 51), mv),
                Expected.of(BOND));
    }

    @Test
    public void between_predicate() {
        execute(Input.of(BOND, KRUEGER),
                Query.of(Predicates.between("limbs_[1].power", 40, 60), mv),
                Expected.of(BOND));
    }

    @Test
    public void between_predicate_reduced() {
        execute(Input.of(BOND, KRUEGER),
                Query.of(Predicates.between("limbs_[any].power", 20, 40), mv),
                Expected.of(KRUEGER));
    }

    @Test
    public void greater_less_predicate() {
        execute(Input.of(BOND, KRUEGER),
                Query.of(Predicates.lessEqual("limbs_[1].power", 29), mv),
                Expected.of(KRUEGER));
    }

    @Test
    public void greater_less_predicate_reduced() {
        execute(Input.of(BOND, KRUEGER),
                Query.of(Predicates.lessEqual("limbs_[any].power", 27), mv),
                Expected.of(KRUEGER));
    }

    @Test
    public void in_predicate() {
        execute(Input.of(BOND, KRUEGER),
                Query.of(Predicates.in("limbs_[1].power", 28, 29, 30), mv),
                Expected.of(KRUEGER));
    }

    @Test
    public void in_predicate_reduced() {
        execute(Input.of(BOND, KRUEGER),
                Query.of(Predicates.in("limbs_[any].power", 26, 27, 51), mv),
                Expected.of(BOND, KRUEGER));
    }

    @Test
    public void notEqual_predicate() {
        execute(Input.of(BOND, KRUEGER),
                Query.of(Predicates.notEqual("limbs_[1].power", 29), mv),
                Expected.of(BOND));
    }

    @Test
    public void notEqual_predicate_reduced() {
        execute(Input.of(BOND, KRUEGER),
                Query.of(Predicates.notEqual("limbs_[any].power", 27), mv),
                Expected.of(BOND));
    }

    @Test
    public void like_predicate() {
        execute(Input.of(BOND, KRUEGER),
                Query.of(Predicates.like("limbs_[1].name", "recht_"), mv),
                Expected.of(KRUEGER));
    }

    @Test
    public void like_predicate_reduced() {
        execute(Input.of(BOND, KRUEGER),
                Query.of(Predicates.like("limbs_[any].name", "le%"), mv),
                Expected.of(BOND));
    }

    @Test
    public void ilike_predicate() {
        execute(Input.of(BOND, KRUEGER),
                Query.of(Predicates.ilike("limbs_[1].name", "REcht_"), mv),
                Expected.of(KRUEGER));
    }

    @Test
    public void ilike_predicate_reduced() {
        execute(Input.of(BOND, KRUEGER),
                Query.of(Predicates.ilike("limbs_[any].name", "LE%"), mv),
                Expected.of(BOND));
    }

    @Test
    public void regex_predicate() {
        execute(Input.of(BOND, KRUEGER),
                Query.of(Predicates.regex("limbs_[1].name", "ri.*"), mv),
                Expected.of(BOND));
    }

    @Test
    public void regex_predicate_reduced() {
        execute(Input.of(BOND, KRUEGER),
                Query.of(Predicates.regex("limbs_[any].name", "li.*"), mv),
                Expected.of(KRUEGER));
    }

}
