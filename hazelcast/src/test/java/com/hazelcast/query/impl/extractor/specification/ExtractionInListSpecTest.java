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

import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.query.Predicates;
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
import static com.hazelcast.query.impl.extractor.AbstractExtractionSpecification.Multivalue.LIST;
import static com.hazelcast.query.impl.extractor.specification.ComplexTestDataStructure.Finger;
import static com.hazelcast.query.impl.extractor.specification.ComplexTestDataStructure.Person;
import static com.hazelcast.query.impl.extractor.specification.ComplexTestDataStructure.finger;
import static com.hazelcast.query.impl.extractor.specification.ComplexTestDataStructure.limb;
import static com.hazelcast.query.impl.extractor.specification.ComplexTestDataStructure.person;
import static com.hazelcast.query.impl.extractor.specification.ComplexTestDataStructure.tattoos;
import static java.util.Arrays.asList;

/**
 * Specification test that verifies the behavior of corner-cases extraction in collections ONLY.
 * <p>
 * Extraction mechanism: IN-BUILT REFLECTION EXTRACTION
 * <p>
 * This test is parametrised on two axes (see the parametrisationData() method):
 * - in memory format
 * - indexing
 */
@RunWith(HazelcastParametrizedRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ExtractionInListSpecTest extends AbstractExtractionTest {

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

    private static final Person HUNT_NULL_LIMB = person("Hunt");

    public ExtractionInListSpecTest(InMemoryFormat inMemoryFormat, Index index, Multivalue multivalue) {
        super(inMemoryFormat, index, multivalue);
    }

    @Test
    public void size_property() {
        execute(Input.of(BOND, KRUEGER),
                Query.of(Predicates.equal("limbs_.size", 2), mv),
                Expected.of(BOND, KRUEGER));
    }

    @Test
    public void size_property_atLeaf() {
        execute(Input.of(BOND, KRUEGER),
                Query.of(Predicates.equal("limbs_[0].tattoos_.size", 1), mv),
                Expected.of(KRUEGER));
    }

    @Test
    public void null_collection_size() {
        execute(Input.of(HUNT_NULL_LIMB),
                Query.of(Predicates.equal("limbs_[0].fingers_.size", 1), mv),
                Expected.empty());
    }

    @Test
    public void null_collection_size_compared_to_null() {
        execute(Input.of(HUNT_NULL_LIMB),
                Query.of(Predicates.equal("limbs_[0].fingers_.size", null), mv),
                Expected.of(HUNT_NULL_LIMB));
    }

    @Test
    public void null_collection_size_reduced() {
        execute(Input.of(HUNT_NULL_LIMB),
                Query.of(Predicates.equal("limbs_[any].fingers_.size", 1), mv),
                Expected.empty());
    }

    @Test
    public void null_collection_size_reduced_compared_to_null() {
        execute(Input.of(HUNT_NULL_LIMB),
                Query.of(Predicates.equal("limbs_[any].fingers_.size", null), mv),
                Expected.of(HUNT_NULL_LIMB));
    }

    @Test
    public void null_collection_size_atLeaf() {
        execute(Input.of(HUNT_NULL_TATTOOS),
                Query.of(Predicates.equal("limbs_[0].tattoos_.size", 1), mv),
                Expected.empty());
    }

    @Test
    public void null_collection_size_atLeaf_compared_to_null() {
        execute(Input.of(HUNT_NULL_TATTOOS),
                Query.of(Predicates.equal("limbs_[0].tattoos_.size", null), mv),
                Expected.of(HUNT_NULL_TATTOOS));
    }

    @Test
    public void null_collection_size_atLeaf_reduced() {
        execute(Input.of(HUNT_NULL_TATTOOS),
                Query.of(Predicates.equal("limbs_[any].tattoos_.size", 1), mv),
                Expected.empty());
    }

    @Test
    public void null_collection_size_atLeaf_reduced_compared_to_null() {
        execute(Input.of(HUNT_NULL_TATTOOS),
                Query.of(Predicates.equal("limbs_[any].tattoos_.size", null), mv),
                Expected.of(HUNT_NULL_TATTOOS));
    }

    @Test
    public void indexOutOfBound_notExistingProperty() {
        execute(Input.of(BOND, KRUEGER),
                Query.of(equal("limbs_[100].sdafasdf", "knife"), mv),
                Expected.of(QueryException.class));
    }

    @Test
    public void indexOutOfBound_notExistingProperty_notAtLeaf() {
        execute(Input.of(BOND, KRUEGER),
                Query.of(equal("limbs_[100].sdafasdf.zxcvzxcv", "knife"), mv),
                Expected.of(QueryException.class));
    }

    @Test
    public void indexOutOfBound_atLeaf_notExistingProperty() {
        execute(Input.of(BOND, KRUEGER),
                Query.of(equal("limbs_[0].fingers_[100].asdfas", "knife"), mv),
                Expected.of(QueryException.class));
    }

    @Parameterized.Parameters(name = "{index}: {0}, {1}, {2}")
    public static Collection<Object[]> data() {
        return axes(
                asList(BINARY, OBJECT),
                asList(NO_INDEX, UNORDERED, ORDERED),
                asList(LIST)
        );
    }

}
