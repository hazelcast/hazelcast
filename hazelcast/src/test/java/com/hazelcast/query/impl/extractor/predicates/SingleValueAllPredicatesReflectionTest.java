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
import static com.hazelcast.query.impl.extractor.AbstractExtractionSpecification.Multivalue.SINGLE;
import static com.hazelcast.query.impl.extractor.predicates.SingleValueDataStructure.Person;
import static com.hazelcast.query.impl.extractor.predicates.SingleValueDataStructure.person;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;

/**
 * Tests whether all predicates work with the extraction in attributes that are not collections.
 * <p>
 * Extraction mechanism: IN-BUILT REFLECTION EXTRACTION
 * <p>
 * This test is parametrised:
 * - each test is executed separately for BINARY and OBJECT in memory format
 * - each test is executed separately having each query using NO_INDEX, UNORDERED_INDEX and ORDERED_INDEX.
 * In this way we are spec-testing most of the reasonable combinations of the configuration of map & extraction.
 */
@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class SingleValueAllPredicatesReflectionTest extends AbstractExtractionTest {

    private static final Person BOND = person(130);
    private static final Person HUNT = person(120);

    @Parameters(name = "{index}: {0}, {1}, {2}")
    public static Collection<Object[]> data() {
        return axes(
                asList(BINARY, OBJECT),
                asList(NO_INDEX, UNORDERED, ORDERED),
                singletonList(SINGLE)
        );
    }

    public SingleValueAllPredicatesReflectionTest(InMemoryFormat inMemoryFormat, Index index, Multivalue multivalue) {
        super(inMemoryFormat, index, multivalue);
    }

    @Test
    public void equals_predicate() {
        execute(Input.of(BOND, HUNT),
                Query.of(Predicates.equal("brain.iq", 130), mv),
                Expected.of(BOND));
    }

    @Test
    public void between_predicate() {
        execute(Input.of(BOND, HUNT),
                Query.of(Predicates.between("brain.iq", 115, 135), mv),
                Expected.of(BOND, HUNT));
    }

    @Test
    public void greater_less_predicate() {
        execute(Input.of(BOND, HUNT),
                Query.of(Predicates.lessEqual("brain.iq", 120), mv),
                Expected.of(HUNT));
    }

    @Test
    public void in_predicate() {
        execute(Input.of(BOND, HUNT),
                Query.of(Predicates.in("brain.iq", 120, 121, 122), mv),
                Expected.of(HUNT));
    }

    @Test
    public void notEqual_predicate() {
        execute(Input.of(BOND, HUNT),
                Query.of(Predicates.notEqual("brain.iq", 130), mv),
                Expected.of(HUNT));
    }

    @Test
    public void like_predicate() {
        execute(Input.of(BOND, HUNT),
                Query.of(Predicates.like("brain.name", "brain12_"), mv),
                Expected.of(HUNT));
    }

    @Test
    public void ilike_predicate() {
        execute(Input.of(BOND, HUNT),
                Query.of(Predicates.ilike("brain.name", "BR%130"), mv),
                Expected.of(BOND));
    }

    @Test
    public void regex_predicate() {
        execute(Input.of(BOND, HUNT),
                Query.of(Predicates.regex("brain.name", "brain13.*"), mv),
                Expected.of(BOND));
    }

    @Test
    public void key_equal_predicate() {
        execute(Input.of(BOND, HUNT),
                Query.of(Predicates.equal("__key", 0), mv),
                Expected.of(BOND));
    }
}
