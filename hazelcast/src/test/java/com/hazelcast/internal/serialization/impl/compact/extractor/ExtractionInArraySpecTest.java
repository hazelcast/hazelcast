/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.internal.serialization.impl.compact.extractor;

import com.hazelcast.config.CompactSerializationConfig;
import com.hazelcast.config.Config;
import com.hazelcast.query.QueryException;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Collection;

import static com.hazelcast.internal.serialization.impl.compact.extractor.AbstractExtractionTest.Index.BITMAP;
import static com.hazelcast.internal.serialization.impl.compact.extractor.AbstractExtractionTest.Index.HASH;
import static com.hazelcast.internal.serialization.impl.compact.extractor.AbstractExtractionTest.Index.NO_INDEX;
import static com.hazelcast.internal.serialization.impl.compact.extractor.ComplexTestDataStructure.Finger;
import static com.hazelcast.internal.serialization.impl.compact.extractor.ComplexTestDataStructure.Person;
import static com.hazelcast.internal.serialization.impl.compact.extractor.ComplexTestDataStructure.finger;
import static com.hazelcast.internal.serialization.impl.compact.extractor.ComplexTestDataStructure.limb;
import static com.hazelcast.internal.serialization.impl.compact.extractor.ComplexTestDataStructure.person;
import static com.hazelcast.internal.serialization.impl.compact.extractor.ComplexTestDataStructure.tattoos;
import static com.hazelcast.query.Predicates.equal;
import static java.util.Arrays.asList;

/**
 * Specification test that verifies the behavior of corner-cases extraction in arrays and collections.
 * <p>
 * Extraction mechanism: IN-BUILT REFLECTION EXTRACTION
 * <p>
 * This test is parametrised on two axes (see the parametrisationData() method):
 * - in memory format
 * - indexing
 * - extraction in collections and arrays
 */
@RunWith(HazelcastParametrizedRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ExtractionInArraySpecTest extends AbstractExtractionTest {

    private static final Person BOND = person("Bond",
            limb("left-hand", tattoos(), finger("thumb"), finger(null)),
            limb("right-hand", tattoos("knife"), finger("middle"), finger("index"))
    );

    private static final Person KRUEGER = person("Krueger",
            limb("linke-hand", tattoos("bratwurst"), finger("Zeigefinger"), finger("Mittelfinger")),
            limb("rechte-hand", tattoos(), finger("Ringfinger"), finger("Daumen"))
    );

    private static final Person HUNT_NULL_TATTOOS = person("Hunt",
            limb("left", null, new Finger[]{})
    );

    private static final Person HUNT_NULL_TATTOO_IN_ARRAY = person("Hunt",
            limb("left", tattoos(null, "cross"), new Finger[]{})
    );

    private static final Person HUNT_NULL_LIMB = person("Hunt");

    private static final Person HUNT_NULL_FIRST = person("Hunt",
            null, limb("left", tattoos(null, "cross"), null, finger("thumbie"))
    );

    private static final Person HUNT_PRIMITIVE_NULL_FIRST = person("Hunt",
            limb("left", tattoos(null, "cross"), finger("thumbie"))
    );

    private static final Person HUNT_NO_NULL_FIRST = person("Hunt",
            limb("left", tattoos("cross"), finger("thumbie"))
    );

    @Parameterized.Parameters(name = "{index}: {0}")
    public static Collection<Object[]> parametrisationData() {
        return axes(asList(NO_INDEX, HASH, BITMAP));
    }

    public ExtractionInArraySpecTest(Index index) {
        super(index);
    }

    public void doWithConfig(Config config) {
        config.getSerializationConfig().setCompactSerializationConfig(new CompactSerializationConfig().setEnabled(true));
    }

    @Test(expected = IllegalArgumentException.class)
    public void notComparable_returned() {
        execute(Input.of(BOND, KRUEGER),
                Query.of(equal("limbs_array[1].fingers_array", "knife")),
                Expected.empty());
    }

    @Test
    public void indexOutOfBound_comparedToNull() {
        execute(Input.of(BOND, KRUEGER),
                Query.of(equal("limbs_array[100].tattoos_array[1]", null)),
                Expected.of(BOND, KRUEGER));
    }

    @Test
    public void indexOutOfBound() {
        execute(Input.of(BOND, KRUEGER),
                Query.of(equal("limbs_array[100].tattoos_array[1]", "knife")),
                Expected.empty());
    }

    @Test(expected = QueryException.class)
    public void indexOutOfBound_negative() {
        execute(Input.of(BOND, KRUEGER),
                Query.of(equal("limbs_array[-1].tattoos_array[1]", "knife")),
                Expected.empty());
    }

    @Test
    public void indexOutOfBound_nullCollection_comparedToNull() {
        execute(Input.of(BOND, HUNT_NULL_LIMB),
                Query.of(equal("limbs_array[100].tattoos_array[1]", null)),
                Expected.of(BOND, HUNT_NULL_LIMB));
    }

    @Test
    public void indexOutOfBound_nullCollection() {
        execute(Input.of(BOND, HUNT_NULL_LIMB),
                Query.of(equal("limbs_array[100].tattoos_array[1]", "knife")),
                Expected.empty());
    }

    @Test(expected = QueryException.class)
    public void indexOutOfBound_nullCollection_negative() {
        execute(Input.of(HUNT_NULL_LIMB),
                Query.of(equal("limbs_array[-1].tattoos_array[1]", "knife")),
                Expected.empty());
    }

    @Test
    public void indexOutOfBound_atLeaf_comparedToNull() {
        execute(Input.of(BOND, KRUEGER),
                Query.of(equal("limbs_array[0].tattoos_array[100]", null)),
                Expected.of(BOND, KRUEGER));
    }

    @Test
    public void indexOutOfBound_atLeaf() {
        execute(Input.of(BOND, KRUEGER),
                Query.of(equal("limbs_array[0].tattoos_array[100]", "knife")),
                Expected.empty());
    }

    @Test(expected = QueryException.class)
    public void indexOutOfBound_negative_atLeaf() {
        execute(Input.of(BOND, KRUEGER),
                Query.of(equal("limbs_array[0].tattoos_array[-1]", "knife")),
                Expected.empty());
    }

    @Test
    public void indexOutOfBound_nullCollection_atLeaf_comparedToNull() {
        execute(Input.of(HUNT_NULL_TATTOOS),
                Query.of(equal("limbs_array[0].tattoos_array[100]", null)),
                Expected.of(HUNT_NULL_TATTOOS));
    }

    @Test
    public void indexOutOfBound_nullCollection_atLeaf() {
        execute(Input.of(HUNT_NULL_TATTOOS),
                Query.of(equal("limbs_array[0].tattoos_array[100]", "knife")),
                Expected.empty());
    }

    @Test
    public void indexOutOfBound_nullCollection_negative_atLeaf() {
        execute(Input.of(HUNT_NULL_TATTOOS),
                Query.of(equal("limbs_array[0].tattoos_array[-1]", "knife")),
                Expected.empty());
    }

    @Test(expected = QueryException.class)
    public void indexOutOfBound_atLeaf_notExistingPropertyOnPrimitiveField() {
        execute(Input.of(BOND, KRUEGER),
                Query.of(equal("limbs_array[0].tattoos_array[100].asdfas", "knife")),
                Expected.empty());
    }

    @Test
    public void indexOutOfBound_nullCollection_reduced_comparedToNull() {
        execute(Input.of(HUNT_NULL_LIMB),
                Query.of(equal("limbs_array[any].tattoos_array[1]", null)),
                Expected.of(HUNT_NULL_LIMB));
    }

    @Test
    public void indexOutOfBound_nullCollection_reduced() {
        execute(Input.of(HUNT_NULL_LIMB),
                Query.of(equal("limbs_array[any].tattoos_array[1]", "knife")),
                Expected.empty());
    }

    @Test
    public void indexOutOfBound_nullCollection_reduced_atLeaf_comparedToNull() {
        execute(Input.of(HUNT_NULL_TATTOOS),
                Query.of(equal("limbs_array[0].tattoos_array[any]", null)),
                Expected.of(HUNT_NULL_TATTOOS));
    }

    @Test
    public void indexOutOfBound_nullCollection_reduced_atLeaf() {
        execute(Input.of(HUNT_NULL_TATTOOS),
                Query.of(equal("limbs_array[0].tattoos_array[any]", "knife")),
                Expected.empty());
    }

    @Test
    public void emptyCollection_reduced_atLeaf() {
        execute(Input.of(HUNT_NULL_TATTOOS),
                Query.of(equal("limbs_array[0].fingers_array[any]", null)),
                Expected.of(HUNT_NULL_TATTOOS));
    }

    @Test
    public void comparable_primitive() {
        execute(Input.of(BOND, KRUEGER),
                Query.of(equal("limbs_array[0].fingers_array[0].name", "thumb")),
                Expected.of(BOND));
    }

    @Test
    public void comparable_primitive_reduced() {
        execute(Input.of(BOND, KRUEGER),
                Query.of(equal("limbs_array[any].fingers_array[any].name", "thumb")),
                Expected.of(BOND));
    }

    @Test
    public void comparable_primitive_comparedToNull() {
        execute(Input.of(BOND, KRUEGER),
                Query.of(equal("limbs_array[0].fingers_array[0].name", null)),
                Expected.empty());
    }

    @Test
    public void comparable_primitive_comparedToNull_matching() {
        execute(Input.of(BOND, KRUEGER),
                Query.of(equal("limbs_array[0].fingers_array[1].name", null)),
                Expected.of(BOND));
    }

    @Test
    public void comparable_primitive_comparedToNull_reduced_matching() {
        execute(Input.of(BOND, KRUEGER),
                Query.of(equal("limbs_array[0].fingers_array[any].name", null)),
                Expected.of(BOND));
    }

    @Test
    public void comparable_primitive_reduced_comparedToNull_matching() {
        execute(Input.of(BOND, KRUEGER),
                Query.of(equal("limbs_array[any].fingers_array[1].name", null)),
                Expected.of(BOND));
    }

    @Test
    public void comparable_primitive_reduced_attribute_comparedToNull_matching2() {
        execute(Input.of(BOND, KRUEGER),
                Query.of(equal("limbs_array[any].fingers_array[any].name", null)),
                Expected.of(BOND));
    }

    @Test
    public void comparable_primitive_reduced_atLeaf_comparedToNull_matching() {
        execute(Input.of(BOND, KRUEGER),
                Query.of(equal("limbs_array[any].tattoos_array[any]", null)),
                Expected.of(BOND, KRUEGER));
    }

    @Test
    public void github8134_firstNonNull_string() {
        Person carlos = person("Carlos", limb("l", null), limb(null, null));
        execute(Input.of(carlos),
                Query.of(equal("limbs_array[any].name", 'l')),
                Expected.of(carlos));
    }

    @Test
    public void github8134_firstNull_string() {
        Person carlos = person("Carlos", limb(null, null), limb("l", null));
        execute(Input.of(carlos),
                Query.of(equal("limbs_array[any].name", 'l')),
                Expected.of(carlos));
    }

    @Test
    public void comparable_nullVsNoNullFirst_case1() {
        execute(Input.of(HUNT_NO_NULL_FIRST, HUNT_NULL_FIRST),
                Query.of(equal("limbs_array[any].tattoos_array[any]", "cross")),
                Expected.of(HUNT_NO_NULL_FIRST, HUNT_NULL_FIRST));
    }

    @Test
    public void comparable_nullVsNoNullFirst_case2() {
        execute(Input.of(HUNT_NO_NULL_FIRST, HUNT_NULL_FIRST),
                Query.of(equal("limbs_array[any].tattoos_array[any]", null)),
                Expected.of(HUNT_NULL_FIRST));
    }

    @Test
    public void comparable_nullVsNoNullFirst_case3() {
        execute(Input.of(HUNT_NO_NULL_FIRST, HUNT_NULL_FIRST),
                Query.of(equal("limbs_array[any].fingers_array[any].name", null)),
                Expected.of(HUNT_NULL_FIRST));
    }

    @Test
    public void comparable_nullVsNoNullFirst_case4() {
        execute(Input.of(HUNT_NO_NULL_FIRST, HUNT_NULL_FIRST),
                Query.of(equal("limbs_array[any].fingers_array[any].name", "thumbie")),
                Expected.of(HUNT_NO_NULL_FIRST, HUNT_NULL_FIRST));
    }

    @Test
    public void comparable_nullVsNoNullFirst_case8() {
        execute(Input.of(HUNT_NO_NULL_FIRST, HUNT_PRIMITIVE_NULL_FIRST),
                Query.of(equal("limbs_array[any].fingers_array[any].name", "thumbie")),
                Expected.of(HUNT_NO_NULL_FIRST, HUNT_PRIMITIVE_NULL_FIRST));
    }

    @Test
    public void comparable_primitive_notReduced_null_inside() {
        execute(Input.of(HUNT_NULL_TATTOO_IN_ARRAY),
                Query.of(equal("limbs_array[0].tattoos_array[1]", "cross")),
                Expected.of(HUNT_NULL_TATTOO_IN_ARRAY));
    }

    @Test
    public void comparable_primitive_reduced_null_inside() {
        execute(Input.of(HUNT_NULL_TATTOO_IN_ARRAY, HUNT_NO_NULL_FIRST),
                Query.of(equal("limbs_array[any].tattoos_array[any]", "cross")),
                Expected.of(HUNT_NULL_TATTOO_IN_ARRAY, HUNT_NO_NULL_FIRST));
    }

    @Test
    public void indexOutOfBound_notExistingProperty() {
        execute(Input.of(BOND, KRUEGER),
                Query.of(equal("limbs_array[100].sdafasdf", "knife")),
                Expected.empty());
    }

    @Test
    public void indexOutOfBound_notExistingProperty_notAtLeaf() {
        execute(Input.of(BOND, KRUEGER),
                Query.of(equal("limbs_array[100].sdafasdf.zxcvzxcv", "knife")),
                Expected.empty());
    }

    @Test
    public void indexOutOfBound_atLeaf_notExistingProperty() {
        execute(Input.of(BOND, KRUEGER),
                Query.of(equal("limbs_array[0].fingers_array[100].asdfas", "knife")),
                Expected.empty());
    }
}
