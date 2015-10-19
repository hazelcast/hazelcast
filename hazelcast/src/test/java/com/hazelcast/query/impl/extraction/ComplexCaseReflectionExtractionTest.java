package com.hazelcast.query.impl.extraction;

import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.Predicates;
import com.hazelcast.query.QueryException;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static com.hazelcast.query.impl.extraction.ComplexCaseDataStructure.Finger;
import static com.hazelcast.query.impl.extraction.ComplexCaseDataStructure.Person;
import static com.hazelcast.query.impl.extraction.ComplexCaseDataStructure.finger;
import static com.hazelcast.query.impl.extraction.ComplexCaseDataStructure.limb;
import static com.hazelcast.query.impl.extraction.ComplexCaseDataStructure.person;
import static com.hazelcast.query.impl.extraction.ComplexCaseDataStructure.tattoos;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertThat;


@RunWith(Parameterized.class)
public class ComplexCaseReflectionExtractionTest extends AbstractExtractionTest {

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

    public ComplexCaseReflectionExtractionTest(InMemoryFormat inMemoryFormat, Index index, Multivalue multivalue) {
        super(inMemoryFormat, index, multivalue);
    }

    @Rule
    public ExpectedException expected = ExpectedException.none();

    @Override
    public List<String> indexAttributes() {
        return Collections.emptyList();
    }

    private void putTestDataToMap(Object... objects) {
        setup(getConfigurator());
        int i = 0;
        for (Object person : objects) {
            map.put(String.valueOf(i++), person);
        }
    }

    private void execute(Input input, Query query, Expected expected) {
        // GIVEN
        putTestDataToMap(input.persons);

        // EXPECT
        if (expected.throwable != null) {
            this.expected.expect(expected.throwable);
        }

        // WHEN
        Predicate predicate = Predicates.equal(query.expression, query.input);
        Collection<Person> values = map.values(predicate);

        // THEN
        assertThat(values, hasSize(expected.objects.length));
        if (expected.objects.length > 0) {
            assertThat(values, contains(expected.objects));
        }
    }


    @Test
    public void notComparable_returned() {
        execute(Input.of(BOND, KRUEGER),
                Query.of("limbs_[1].fingers_", "knife", multivalue),
                Expected.of(IllegalArgumentException.class));
    }

    @Test
    public void indexOutOfBound() {
        execute(Input.of(BOND, KRUEGER),
                Query.of("limbs_[100].tattoos_[1]", "knife", multivalue),
                Expected.empty());
    }

    @Test
    public void indexOutOfBound_negative() {
        // TODO inconsistent with indexOutOfBound()
        // TODO inconsistent exception type with indexOutOfBound_negative_atLeaf
        execute(Input.of(BOND, KRUEGER),
                Query.of("limbs_[-1].tattoos_[1]", "knife", multivalue),
                Expected.of(QueryException.class));
    }

    @Test
    public void indexOutOfBound_atLeaf() {
        execute(Input.of(BOND, KRUEGER),
                Query.of("limbs_[0].tattoos_[100]", "knife", multivalue),
                Expected.empty());
    }

    @Test
    public void indexOutOfBound_negative_atLeaf() {
        // TODO inconsistent with indexOutOfBound_atLeaf()
        // TODO inconsistent exception type with indexOutOfBound_negative
        execute(Input.of(BOND, KRUEGER),
                Query.of("limbs_[0].tattoos_[-1]", "knife", multivalue),
                Expected.of(IllegalArgumentException.class));
    }

    @Test
    public void indexOutOfBound_notExistingProperty() {
        execute(Input.of(BOND, KRUEGER),
                Query.of("limbs_[100].sdafasdf", "knife", multivalue),
                Expected.of(QueryException.class));
    }

    @Test
    public void indexOutOfBound_atLeaf_notExistingProperty() {
        execute(Input.of(BOND, KRUEGER),
                Query.of("limbs_[0].tattoos_[100].asdfas", "knife", multivalue),
                Expected.of(QueryException.class));
    }

    @Test
    @Ignore // TODO does not work for arrays (works for lists only)
    public void sizeProperty() {
        execute(Input.of(BOND, KRUEGER),
                Query.of("limbs_[0].tattoos_.size", 1, multivalue),
                Expected.of(KRUEGER));
    }

    @Test
    @Ignore // TODO does not work for arrays nor lists
    public void lengthProperty() {
        execute(Input.of(BOND, KRUEGER),
                Query.of("limbs_[0].tattoos_.length", 1, multivalue),
                Expected.of(KRUEGER));
    }

    @Test
    public void comparable_notPrimitive() {
        execute(Input.of(BOND, KRUEGER),
                Query.of("limbs_[0].fingers_[0]", finger("thumb"), multivalue),
                Expected.of(BOND));
    }

    @Test
    public void comparable_notPrimitive_reduced() {
        execute(Input.of(BOND, KRUEGER),
                Query.of("limbs_[any].fingers_[0]", finger("thumb"), multivalue),
                Expected.of(BOND));
    }

    @Test
    public void comparable_primitive() {
        execute(Input.of(BOND, KRUEGER),
                Query.of("limbs_[0].fingers_[0].name", "thumb", multivalue),
                Expected.of(BOND));
    }

    @Test
    public void comparable_primitive_reduced() {
        execute(Input.of(BOND, KRUEGER),
                Query.of("limbs_[any].fingers_[any].name", "thumb", multivalue),
                Expected.of(BOND));
    }

    @Test
    public void comparable_primitive_comparedToNull() {
        execute(Input.of(BOND, KRUEGER),
                Query.of("limbs_[0].fingers_[0].name", null, multivalue),
                Expected.empty());
    }

    @Test
    public void comparable_notPrimitive_comparedToNull() {
        execute(Input.of(BOND, KRUEGER),
                Query.of("limbs_[0].fingers_[0]", null, multivalue),
                Expected.empty());
    }

    @Test
    public void comparable_primitive_comparedToNull_matching() {
        execute(Input.of(BOND, KRUEGER),
                Query.of("limbs_[0].fingers_[1].name", null, multivalue),
                Expected.of(BOND));
    }

    @Test
    @Ignore
    // TODO BUG to fix -> null values handling in MultiResult like in IndexStore
    public void comparable_primitive_comparedToNull_reduced_matching() {
        execute(Input.of(BOND, KRUEGER),
                Query.of("limbs_[0].fingers_[any].name", null, multivalue),
                Expected.of(BOND));
    }

    @Test
    @Ignore
    // TODO BUG to fix -> null values handling in MultiResult like in IndexStore
    public void comparable_primitive_reduced_atLeaf_comparedToNull_matching() {
        execute(Input.of(BOND, KRUEGER),
                Query.of("limbs_[any].fingers_[1].name", null, multivalue),
                Expected.of(BOND));
    }

    @Test
    @Ignore
    // TODO BUG to fix -> Collection throws NullPointer, Array throws QueryException
    // TODO Should be QueryException for consistency with null_arrayOrCollection_size
    public void null_arrayOrCollection() {
        execute(Input.of(HUNT),
                Query.of("limbs_[0].fingers_[0].name", "index", multivalue),
                Expected.of(NullPointerException.class));
    }

    @Test
    @Ignore
    // TODO BUG to fix -> see null_arrayOrCollection() test
    public void null_arrayOrCollection_reduced() {
        execute(Input.of(HUNT),
                Query.of("limbs_[0].fingers_[any].name", "index", multivalue),
                Expected.of(NullPointerException.class));
    }

    @Test
    public void null_arrayOrCollection_size() {
        execute(Input.of(HUNT),
                Query.of("limbs_[0].fingers.size", 1, multivalue),
                Expected.of(QueryException.class));
    }

    @Test
    public void null_arrayOrCollection_size_reduced() {
        execute(Input.of(HUNT),
                Query.of("limbs_[any].fingers.size", 1, multivalue),
                Expected.of(QueryException.class));
    }

}
