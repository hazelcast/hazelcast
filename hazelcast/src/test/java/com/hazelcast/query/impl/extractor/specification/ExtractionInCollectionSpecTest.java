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

package com.hazelcast.query.impl.extractor.specification;

import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.internal.util.UuidUtil;
import com.hazelcast.query.QueryException;
import com.hazelcast.query.impl.extractor.AbstractExtractionTest;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Collection;

import static com.hazelcast.config.InMemoryFormat.BINARY;
import static com.hazelcast.config.InMemoryFormat.OBJECT;
import static com.hazelcast.query.impl.extractor.AbstractExtractionSpecification.Index.NO_INDEX;
import static com.hazelcast.query.impl.extractor.AbstractExtractionSpecification.Index.ORDERED;
import static com.hazelcast.query.impl.extractor.AbstractExtractionSpecification.Index.UNORDERED;
import static com.hazelcast.query.impl.extractor.AbstractExtractionSpecification.Multivalue.ARRAY;
import static com.hazelcast.query.impl.extractor.AbstractExtractionSpecification.Multivalue.LIST;
import static com.hazelcast.query.impl.extractor.AbstractExtractionSpecification.Multivalue.PORTABLE;
import static com.hazelcast.query.impl.extractor.specification.ComplexTestDataStructure.Finger;
import static com.hazelcast.query.impl.extractor.specification.ComplexTestDataStructure.Person;
import static com.hazelcast.query.impl.extractor.specification.ComplexTestDataStructure.finger;
import static com.hazelcast.query.impl.extractor.specification.ComplexTestDataStructure.limb;
import static com.hazelcast.query.impl.extractor.specification.ComplexTestDataStructure.person;
import static com.hazelcast.query.impl.extractor.specification.ComplexTestDataStructure.tattoos;
import static java.util.Arrays.asList;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assume.assumeThat;

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
public class ExtractionInCollectionSpecTest extends AbstractExtractionTest {

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

    public ExtractionInCollectionSpecTest(InMemoryFormat inMemoryFormat, Index index, Multivalue multivalue) {
        super(inMemoryFormat, index, multivalue);
    }

    protected Configurator getInstanceConfigurator() {
        return new Configurator() {
            @Override
            public void doWithConfig(Config config, Multivalue mv) {
                config.getSerializationConfig().addPortableFactory(ComplexTestDataStructure.PersonPortableFactory.ID, new ComplexTestDataStructure.PersonPortableFactory());
            }
        };
    }

    @Override
    protected void doWithMap() {
        // init fully populated object to handle nulls properly
        if (mv == PORTABLE) {
            String key = UuidUtil.newUnsecureUuidString();
            map.put(key, KRUEGER.getPortable());
            map.remove(key);
        }
    }

    @Test
    public void notComparable_returned() {
        execute(Input.of(BOND, KRUEGER),
                Query.of(equal("limbs_[1].fingers_", "knife"), mv),
                Expected.of(IllegalArgumentException.class));
    }

    @Test
    public void indexOutOfBound_comparedToNull() {
        execute(Input.of(BOND, KRUEGER),
                Query.of(equal("limbs_[100].tattoos_[1]", null), mv),
                Expected.of(BOND, KRUEGER));
    }

    @Test
    public void indexOutOfBound() {
        execute(Input.of(BOND, KRUEGER),
                Query.of(equal("limbs_[100].tattoos_[1]", "knife"), mv),
                Expected.empty());
    }

    @Test
    public void indexOutOfBound_negative() {
        execute(Input.of(BOND, KRUEGER),
                Query.of(equal("limbs_[-1].tattoos_[1]", "knife"), mv),
                Expected.of(QueryException.class));
    }

    @Test
    public void indexOutOfBound_nullCollection_comparedToNull() {
        execute(Input.of(BOND, HUNT_NULL_LIMB),
                Query.of(equal("limbs_[100].tattoos_[1]", null), mv),
                Expected.of(BOND, HUNT_NULL_LIMB));
    }

    @Test
    public void indexOutOfBound_nullCollection() {
        execute(Input.of(BOND, HUNT_NULL_LIMB),
                Query.of(equal("limbs_[100].tattoos_[1]", "knife"), mv),
                Expected.empty());
    }

    @Test
    public void indexOutOfBound_nullCollection_negative() {
        execute(Input.of(HUNT_NULL_LIMB),
                Query.of(equal("limbs_[-1].tattoos_[1]", "knife"), mv),
                Expected.of(QueryException.class));
    }

    @Test
    public void indexOutOfBound_atLeaf_comparedToNull() {
        execute(Input.of(BOND, KRUEGER),
                Query.of(equal("limbs_[0].tattoos_[100]", null), mv),
                Expected.of(BOND, KRUEGER));
    }

    @Test
    public void indexOutOfBound_atLeaf() {
        execute(Input.of(BOND, KRUEGER),
                Query.of(equal("limbs_[0].tattoos_[100]", "knife"), mv),
                Expected.empty());
    }

    @Test
    public void indexOutOfBound_negative_atLeaf() {
        execute(Input.of(BOND, KRUEGER),
                Query.of(equal("limbs_[0].tattoos_[-1]", "knife"), mv),
                Expected.of(QueryException.class));
    }

    @Test
    public void indexOutOfBound_nullCollection_atLeaf_comparedToNull() {
        execute(Input.of(HUNT_NULL_TATTOOS),
                Query.of(equal("limbs_[0].tattoos_[100]", null), mv),
                Expected.of(HUNT_NULL_TATTOOS));
    }

    @Test
    public void indexOutOfBound_nullCollection_atLeaf() {
        execute(Input.of(HUNT_NULL_TATTOOS),
                Query.of(equal("limbs_[0].tattoos_[100]", "knife"), mv),
                Expected.empty());
    }

    @Test
    public void indexOutOfBound_nullCollection_negative_atLeaf() {
        execute(Input.of(HUNT_NULL_TATTOOS),
                Query.of(equal("limbs_[0].tattoos_[-1]", "knife"), mv),
                Expected.of(QueryException.class));
    }

    @Test
    public void indexOutOfBound_atLeaf_notExistingPropertyOnPrimitiveField() {
        execute(Input.of(BOND, KRUEGER),
                Query.of(equal("limbs_[0].tattoos_[100].asdfas", "knife"), mv),
                Expected.of(QueryException.class));
    }

    @Test
    public void indexOutOfBound_nullCollection_reduced_comparedToNull() {
        execute(Input.of(HUNT_NULL_LIMB),
                Query.of(equal("limbs_[any].tattoos_[1]", null), mv),
                Expected.of(HUNT_NULL_LIMB));
    }

    @Test
    public void indexOutOfBound_nullCollection_reduced() {
        execute(Input.of(HUNT_NULL_LIMB),
                Query.of(equal("limbs_[any].tattoos_[1]", "knife"), mv),
                Expected.empty());
    }

    @Test
    public void indexOutOfBound_nullCollection_reduced_atLeaf_comparedToNull() {
        execute(Input.of(HUNT_NULL_TATTOOS),
                Query.of(equal("limbs_[0].tattoos_[any]", null), mv),
                Expected.of(HUNT_NULL_TATTOOS));
    }

    @Test
    public void indexOutOfBound_nullCollection_reduced_atLeaf() {
        execute(Input.of(HUNT_NULL_TATTOOS),
                Query.of(equal("limbs_[0].tattoos_[any]", "knife"), mv),
                Expected.empty());
    }

    @Test
    public void emptyCollection_reduced_atLeaf() {
        execute(Input.of(HUNT_NULL_TATTOOS),
                Query.of(equal("limbs_[0].fingers_[any]", null), mv),
                Expected.of(HUNT_NULL_TATTOOS));
    }

    @Test
    public void comparable_notPrimitive() {
        execute(Input.of(BOND, KRUEGER),
                Query.of(equal("limbs_[0].fingers_[0]", finger("thumb")), mv),
                Expected.of(BOND));
    }

    @Test
    public void comparable_notPrimitive_reduced() {
        execute(Input.of(BOND, KRUEGER),
                Query.of(equal("limbs_[any].fingers_[0]", finger("thumb")), mv),
                Expected.of(BOND));
    }

    @Test
    public void comparable_primitive() {
        execute(Input.of(BOND, KRUEGER),
                Query.of(equal("limbs_[0].fingers_[0].name", "thumb"), mv),
                Expected.of(BOND));
    }

    @Test
    public void comparable_primitive_reduced() {
        execute(Input.of(BOND, KRUEGER),
                Query.of(equal("limbs_[any].fingers_[any].name", "thumb"), mv),
                Expected.of(BOND));
    }

    @Test
    public void comparable_primitive_comparedToNull() {
        execute(Input.of(BOND, KRUEGER),
                Query.of(equal("limbs_[0].fingers_[0].name", null), mv),
                Expected.empty());
    }

    @Test
    public void comparable_notPrimitive_comparedToNull() {
        execute(Input.of(BOND, KRUEGER),
                Query.of(equal("limbs_[0].fingers_[0]", null), mv),
                Expected.empty());
    }

    @Test
    public void comparable_primitive_comparedToNull_matching() {
        execute(Input.of(BOND, KRUEGER),
                Query.of(equal("limbs_[0].fingers_[1].name", null), mv),
                Expected.of(BOND));
    }

    @Test
    public void comparable_primitive_comparedToNull_reduced_matching() {
        execute(Input.of(BOND, KRUEGER),
                Query.of(equal("limbs_[0].fingers_[any].name", null), mv),
                Expected.of(BOND));
    }

    @Test
    public void comparable_primitive_reduced_comparedToNull_matching() {
        execute(Input.of(BOND, KRUEGER),
                Query.of(equal("limbs_[any].fingers_[1].name", null), mv),
                Expected.of(BOND));
    }

    @Test
    public void comparable_primitive_reduced_attribute_comparedToNull_matching2() {
        execute(Input.of(BOND, KRUEGER),
                Query.of(equal("limbs_[any].fingers_[any].name", null), mv),
                Expected.of(BOND));
    }

    @Test
    public void comparable_primitive_reduced_comparedToNull_matching2() {
        execute(Input.of(BOND, KRUEGER),
                Query.of(equal("limbs_[any].fingers_[any]", null), mv),
                Expected.empty());
    }

    @Test
    public void comparable_primitive_reduced_atLeaf_comparedToNull_matching() {
        execute(Input.of(BOND, KRUEGER),
                Query.of(equal("limbs_[any].tattoos_[any]", null), mv),
                Expected.of(BOND, KRUEGER));
    }

    @Test
    public void github8134_firstNonNull_string() {
        Person carlos = person("Carlos", limb("l", null), limb(null, null));
        execute(Input.of(carlos),
                Query.of(equal("limbs_[any].name", 'l'), mv),
                Expected.of(carlos));
    }

    @Test
    public void github8134_firstNull_string() {
        Person carlos = person("Carlos", limb(null, null), limb("l", null));
        execute(Input.of(carlos),
                Query.of(equal("limbs_[any].name", 'l'), mv),
                Expected.of(carlos));
    }

    @Test
    public void comparable_nullVsNoNullFirst_case1() {
        ignoreForPortable("Portables can't handle nulls in collection");
        execute(Input.of(HUNT_NO_NULL_FIRST, HUNT_NULL_FIRST),
                Query.of(equal("limbs_[any].tattoos_[any]", "cross"), mv),
                Expected.of(HUNT_NO_NULL_FIRST, HUNT_NULL_FIRST));
    }

    @Test
    public void comparable_nullVsNoNullFirst_case2() {
        ignoreForPortable("Portables can't handle nulls in collection");
        execute(Input.of(HUNT_NO_NULL_FIRST, HUNT_NULL_FIRST),
                Query.of(equal("limbs_[any].tattoos_[any]", null), mv),
                Expected.of(HUNT_NULL_FIRST));
    }

    @Test
    public void comparable_nullVsNoNullFirst_case3() {
        ignoreForPortable("Portables can't handle nulls in collection");
        execute(Input.of(HUNT_NO_NULL_FIRST, HUNT_NULL_FIRST),
                Query.of(equal("limbs_[any].fingers_[any]", null), mv),
                Expected.of(HUNT_NULL_FIRST));
    }

    @Test
    public void comparable_nullVsNoNullFirst_case4() {
        ignoreForPortable("Portables can't handle nulls in collection");
        execute(Input.of(HUNT_NO_NULL_FIRST, HUNT_NULL_FIRST),
                Query.of(equal("limbs_[any].fingers_[any]", finger("thumbie")), mv),
                Expected.of(HUNT_NO_NULL_FIRST, HUNT_NULL_FIRST));
    }

    @Test
    public void comparable_nullVsNoNullFirst_case5() {
        ignoreForPortable("Portables can't handle nulls in collection");
        execute(Input.of(HUNT_NO_NULL_FIRST, HUNT_NULL_FIRST),
                Query.of(equal("limbs_[any].fingers_[any].name", "thumbie"), mv),
                Expected.of(HUNT_NO_NULL_FIRST, HUNT_NULL_FIRST));
    }

    @Test
    public void comparable_nullVsNoNullFirst_case6() {
        ignoreForPortable("Portables can't handle nulls in collection");
        execute(Input.of(HUNT_NO_NULL_FIRST, HUNT_NULL_FIRST),
                Query.of(equal("limbs_[any].fingers_[any]", null), mv),
                Expected.of(HUNT_NULL_FIRST));
    }

    @Test
    public void comparable_nullVsNoNullFirst_case7() {
        ignoreForPortable("Portables can't handle nulls in collection");
        execute(Input.of(HUNT_NO_NULL_FIRST, HUNT_NULL_FIRST),
                Query.of(equal("limbs_[any].fingers_[any]", finger("thumbie")), mv),
                Expected.of(HUNT_NO_NULL_FIRST, HUNT_NULL_FIRST));
    }

    @Test
    public void comparable_nullVsNoNullFirst_case8() {
        execute(Input.of(HUNT_NO_NULL_FIRST, HUNT_PRIMITIVE_NULL_FIRST),
                Query.of(equal("limbs_[any].fingers_[any]", finger("thumbie")), mv),
                Expected.of(HUNT_NO_NULL_FIRST, HUNT_PRIMITIVE_NULL_FIRST));
    }

    @Test
    public void comparable_primitive_notReduced_null_inside() {
        execute(Input.of(HUNT_NULL_TATTOO_IN_ARRAY),
                Query.of(equal("limbs_[0].tattoos_[1]", "cross"), mv),
                Expected.of(HUNT_NULL_TATTOO_IN_ARRAY));
    }

    @Test
    public void comparable_primitive_reduced_null_inside() {
        execute(Input.of(HUNT_NULL_TATTOO_IN_ARRAY, HUNT_NO_NULL_FIRST),
                Query.of(equal("limbs_[any].tattoos_[any]", "cross"), mv),
                Expected.of(HUNT_NULL_TATTOO_IN_ARRAY, HUNT_NO_NULL_FIRST));
    }

    private void ignoreForPortable(String reason) {
        assumeThat(mv, not(equalTo(PORTABLE)));
    }

    @Parameterized.Parameters(name = "{index}: {0}, {1}, {2}")
    public static Collection<Object[]> parametrisationData() {
        return axes(
                asList(BINARY, OBJECT),
                asList(NO_INDEX, UNORDERED, ORDERED),
                asList(ARRAY, LIST, PORTABLE)
        );
    }

}
