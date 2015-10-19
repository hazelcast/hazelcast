package com.hazelcast.query.impl.extraction;

import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.query.Predicates;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static com.hazelcast.query.impl.extraction.MultipleCollectionsDataStructure.Person;
import static com.hazelcast.query.impl.extraction.MultipleCollectionsDataStructure.limb;
import static com.hazelcast.query.impl.extraction.MultipleCollectionsDataStructure.person;


@RunWith(Parameterized.class)
public class MultipleCollectionsReflectionExtractionTest extends AbstractExtractionTest {

    private static final Person BOND = person(limb("thumb", "pinky"), limb("middle", "index"));
    private static final Person KRUEGER = person(limb("Zeigefinger", "Mittelfinger"),
            limb("Ringfinger", "Mittelfinger"));

    public MultipleCollectionsReflectionExtractionTest(InMemoryFormat format, Index index, Multivalue multivalue) {
        super(format, index, multivalue);
    }

    @Test
    public void equals_predicate() {
        execute(Input.of(BOND, KRUEGER),
                Query.of(Predicates.equal("limbs_[1].fingers_[1]", "index"), mv),
                Expected.of(BOND));
    }

    @Test
    public void equals_predicate_reducedLast() {
        execute(Input.of(BOND, KRUEGER),
                Query.of(Predicates.equal("limbs_[1].fingers_[any]", "Ringfinger"), mv),
                Expected.of(KRUEGER));
    }

    @Test
    public void equals_predicate_reducedFirst() {
        execute(Input.of(BOND, KRUEGER),
                Query.of(Predicates.equal("limbs_[any].fingers_[1]", "Mittelfinger"), mv),
                Expected.of(KRUEGER));
    }

    @Test
    public void equals_predicate_reducedBoth() {
        execute(Input.of(BOND, KRUEGER),
                Query.of(Predicates.equal("limbs_[any].fingers_[any]", "Ringfinger"), mv),
                Expected.of(KRUEGER));
    }

}
