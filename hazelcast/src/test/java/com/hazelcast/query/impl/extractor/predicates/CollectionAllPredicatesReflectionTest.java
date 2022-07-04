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

package com.hazelcast.query.impl.extractor.predicates;

import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.query.Predicates;
import com.hazelcast.query.impl.extractor.AbstractExtractionTest;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.util.Collection;

import static com.hazelcast.config.InMemoryFormat.BINARY;
import static com.hazelcast.config.InMemoryFormat.OBJECT;
import static com.hazelcast.query.impl.extractor.AbstractExtractionSpecification.Index.NO_INDEX;
import static com.hazelcast.query.impl.extractor.AbstractExtractionSpecification.Index.ORDERED;
import static com.hazelcast.query.impl.extractor.AbstractExtractionSpecification.Index.UNORDERED;
import static com.hazelcast.query.impl.extractor.AbstractExtractionSpecification.Multivalue.ARRAY;
import static com.hazelcast.query.impl.extractor.AbstractExtractionSpecification.Multivalue.LIST;
import static com.hazelcast.query.impl.extractor.predicates.CollectionDataStructure.Person;
import static com.hazelcast.query.impl.extractor.predicates.CollectionDataStructure.limb;
import static com.hazelcast.query.impl.extractor.predicates.CollectionDataStructure.person;
import static java.util.Arrays.asList;

/**
 * Tests whether all predicates work with the extraction in collections.
 * Each predicate is tested with and without the extraction including "any" operator.
 * Extraction with the "any" operator may return multiple results, thus each predicate has to handle it.
 * <p>
 * Extraction mechanism: IN-BUILT REFLECTION EXTRACTION
 * <p>
 * The trick here is that each multi-value attribute in the used data structure is present as both an array and as
 * a collection. For more details have a look at CollectionDataStructure.
 * This test is parametrised, so that it's executed for both options too (arrays and collections).
 * Let's have a look at the following path: 'limbs_[1].power'.
 * limbs_[1].power is unfolded to: limbs_array[1].power and limbs_list[1].power in two separate tests run.
 * In this way we are testing extraction in collection and arrays making sure that the default behavior is consistent
 * for both extraction sources!
 * <p>
 * It's not the only parametrisation in this test; the other ones are:
 * - each test is executed separately for BINARY and OBJECT in memory format
 * - each test is executed separately having each query using NO_INDEX, UNORDERED_INDEX and ORDERED_INDEX.
 * In this way we are spec-testing most of the reasonable combinations of the configuration of map & extraction.
 */
@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class CollectionAllPredicatesReflectionTest extends AbstractExtractionTest {

    private static final Person BOND = person(limb("left", 49), limb("right", 51));
    private static final Person KRUEGER = person(limb("links", 27), limb("rechts", 29));

    @Parameters(name = "{index}: {0}, {1}, {2}")
    public static Collection<Object[]> data() {
        return axes(
                asList(BINARY, OBJECT),
                asList(NO_INDEX, UNORDERED, ORDERED),
                asList(ARRAY, LIST)
        );
    }

    public CollectionAllPredicatesReflectionTest(InMemoryFormat inMemoryFormat, Index index, Multivalue multivalue) {
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
