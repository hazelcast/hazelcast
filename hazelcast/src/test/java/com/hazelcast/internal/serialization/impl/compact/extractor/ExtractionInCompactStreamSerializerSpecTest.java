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
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
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
 * Specification test that verifies the behavior of corner-cases extraction in single-value attributes.
 * It's a detailed test especially for portables, since the extraction is much more complex there.
 * <p>
 * Extraction mechanism: IN-BUILT REFLECTION EXTRACTION
 * <p>
 * This test is parametrised on two axes (see the parametrisationData() method):
 * - in memory format
 * - indexing
 */
@RunWith(HazelcastParametrizedRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ExtractionInCompactStreamSerializerSpecTest extends AbstractExtractionTest {

    private static final Person BOND = person("Bond",
            limb("left-hand", tattoos(), finger("thumb"), finger(null)),
            limb("right-hand", tattoos("knife"), finger("middle"), finger("index"))
    );

    private static final Person KRUEGER = person("Krueger",
            limb("linke-hand", tattoos("bratwurst"), finger("Zeigefinger"), finger("Mittelfinger")),
            limb("rechte-hand", tattoos(), finger("Ringfinger"), finger("Daumen"))
    );

    private static final Person HUNT_WITH_NULLS = person(null,
            limb(null, new ArrayList<String>(), new Finger[]{})
    );

    @Parameterized.Parameters(name = "{index}: {0}")
    public static Collection<Object[]> parametrisationData() {
        return axes(asList(NO_INDEX, HASH, BITMAP));
    }

    public ExtractionInCompactStreamSerializerSpecTest(Index index) {
        super(index);
    }

    @Override
    public void doWithConfig(Config config) {
        config.getSerializationConfig().setCompactSerializationConfig(new CompactSerializationConfig().setEnabled(true));
    }

    @Test
    public void wrong_attribute_name_atLeaf() {
        execute(Input.of(BOND, KRUEGER, HUNT_WITH_NULLS),
                Query.of(equal("name12312", "Bond")),
                Expected.empty());
    }

    @Test
    public void wrong_attribute_name_atLeaf_comparedToNull() {
        execute(Input.of(BOND, KRUEGER, HUNT_WITH_NULLS),
                Query.of(equal("name12312", null)),
                Expected.of(BOND, KRUEGER, HUNT_WITH_NULLS));
    }

    @Test
    public void nested_wrong_attribute_name_atLeaf() {
        execute(Input.of(BOND, KRUEGER),
                Query.of(equal("firstLimb.name12312", "left-hand")),
                Expected.empty());
    }

    @Test
    public void nested_wrong_attribute_name_atLeaf_comparedToNull() {
        execute(Input.of(BOND, KRUEGER),
                Query.of(equal("firstLimb.name12312", null)),
                Expected.of(BOND, KRUEGER));
    }

    @Test
    public void nested_wrong_attribute_notAtLeaf() {
        execute(Input.of(BOND, KRUEGER),
                Query.of(equal("firstLimb.notExisting.notExistingToo", "left-hand")),
                Expected.empty());
    }

    @Test
    public void nested_wrong_attribute_notAtLeaf_comparedToNull() {
        execute(Input.of(BOND, KRUEGER),
                Query.of(equal("firstLimb.notExisting.notExistingToo", null)),
                Expected.of(BOND, KRUEGER));
    }

    @Test
    public void indexOutOfBoundFirst_notExistingProperty_notAtLeaf() {
        execute(Input.of(BOND, KRUEGER),
                Query.of(equal("limbs_array[100].notExisting.notExistingToo", "knife")),
                Expected.empty());
    }

    @Test
    public void indexOutOfBoundFirst_notExistingProperty_notAtLeaf_comparedToNull() {
        execute(Input.of(BOND, KRUEGER),
                Query.of(equal("limbs_array[100].notExisting.notExistingToo", null)),
                Expected.of(BOND, KRUEGER));
    }

    @Test
    public void indexOutOfBound_notExistingProperty() {
        execute(Input.of(BOND, KRUEGER),
                Query.of(equal("limbs_array[100].sdafasdf", "knife")),
                Expected.empty());
    }

    @Test
    public void indexOutOfBound_atLeaf_notExistingProperty() {
        execute(Input.of(BOND, KRUEGER),
                Query.of(equal("limbs_array[0].fingers_array[100].asdfas", "knife")),
                Expected.empty());
    }

    @Test
    public void wrong_attribute_name_compared_to_null() {
        execute(Input.of(BOND, KRUEGER, HUNT_WITH_NULLS),
                Query.of(equal("name12312", null)),
                Expected.of(BOND, KRUEGER, HUNT_WITH_NULLS));
    }

    @Test
    public void primitiveNull_comparedToNull_matching() {
        execute(Input.of(BOND, KRUEGER, HUNT_WITH_NULLS),
                Query.of(equal("name", null)),
                Expected.of(HUNT_WITH_NULLS));
    }

    @Test
    public void primitiveNull_comparedToNotNull_notMatching() {
        execute(Input.of(BOND, KRUEGER, HUNT_WITH_NULLS),
                Query.of(equal("name", "Non-null-value")),
                Expected.empty());
    }

    @Test
    public void nestedAttribute_firstIsNull_comparedToNotNull() {
        execute(Input.of(BOND, KRUEGER, HUNT_WITH_NULLS),
                Query.of(equal("secondLimb.name", "Non-null-value")),
                Expected.empty());
    }

    @Test
    public void nestedAttribute_firstIsNull_comparedToNull() {
        execute(Input.of(BOND, KRUEGER, HUNT_WITH_NULLS),
                Query.of(equal("secondLimb.name", null)),
                Expected.of(HUNT_WITH_NULLS));
    }

    @Test
    public void correct_attribute_name() {
        execute(Input.of(BOND),
                Query.of(equal("name", "Bond")),
                Expected.of(BOND));
    }

    @Test
    public void correct_nestedAttribute_name() {
        execute(Input.of(BOND),
                Query.of(equal("firstLimb.name", "left-hand")),
                Expected.of(BOND));
    }

    @Test
    public void correct_portableArrayInTheMiddle_matching() {
        execute(Input.of(BOND),
                Query.of(equal("limbs_array[0].name", "left-hand")),
                Expected.of(BOND));
    }

    @Test
    public void correct_portableArrayInTheMiddle_notMatching() {
        execute(Input.of(BOND),
                Query.of(equal("limbs_array[0].name", "dasdfasdfasdf")),
                Expected.empty());
    }

    @Test
    public void correct_portableInTheMiddle_portableArrayAtTheEnd_primitiveAttribute_notMatching() {
        execute(Input.of(BOND),
                Query.of(equal("firstLimb.fingers_array[0].name", "thumb123")),
                Expected.empty());
    }

    @Test
    public void correct_portableArrayInTheMiddle_portableArrayAtTheEnd_primitiveAttribute_notMatching() {
        execute(Input.of(BOND),
                Query.of(equal("limbs_array[0].fingers_array[0].name", "thumb123")),
                Expected.empty());
    }

    @Test
    public void correct_portableArrayInTheMiddle_portableArrayAtTheEnd_primitiveAttribute_matching() {
        execute(Input.of(BOND),
                Query.of(equal("firstLimb.fingers_array[0].name", "thumb")),
                Expected.of(BOND));
    }

    @Test
    public void correct_primitiveArrayAtTheEnd_matching() {
        execute(Input.of(BOND),
                Query.of(equal("secondLimb.tattoos_array[0]", "knife")),
                Expected.of(BOND));
    }

    @Test
    public void correct_primitiveArrayAtTheEnd_notMatching() {
        execute(Input.of(BOND),
                Query.of(equal("secondLimb.tattoos_array[0]", "knife123")),
                Expected.empty());
    }
}
