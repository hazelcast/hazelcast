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

import java.util.ArrayList;
import java.util.Collection;

import static com.hazelcast.config.InMemoryFormat.BINARY;
import static com.hazelcast.config.InMemoryFormat.OBJECT;
import static com.hazelcast.query.impl.extractor.AbstractExtractionSpecification.Index.NO_INDEX;
import static com.hazelcast.query.impl.extractor.AbstractExtractionSpecification.Index.ORDERED;
import static com.hazelcast.query.impl.extractor.AbstractExtractionSpecification.Index.UNORDERED;
import static com.hazelcast.query.impl.extractor.AbstractExtractionSpecification.Multivalue.SINGLE;
import static com.hazelcast.query.impl.extractor.specification.ComplexTestDataStructure.Finger;
import static com.hazelcast.query.impl.extractor.specification.ComplexTestDataStructure.Person;
import static com.hazelcast.query.impl.extractor.specification.ComplexTestDataStructure.finger;
import static com.hazelcast.query.impl.extractor.specification.ComplexTestDataStructure.limb;
import static com.hazelcast.query.impl.extractor.specification.ComplexTestDataStructure.person;
import static com.hazelcast.query.impl.extractor.specification.ComplexTestDataStructure.tattoos;
import static java.util.Arrays.asList;

/**
 * Specification test that verifies the behavior of corner-cases extraction in single-value attributes.
 * <p>
 * Extraction mechanism: IN-BUILT REFLECTION EXTRACTION
 * <p>
 * This test is parametrised on two axes (see the parametrisationData() method):
 * - in memory format
 * - indexing
 */
@RunWith(HazelcastParametrizedRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ExtractionInSingleValueSpecTest extends AbstractExtractionTest {

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

    public ExtractionInSingleValueSpecTest(InMemoryFormat inMemoryFormat, Index index, Multivalue multivalue) {
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

    @Test
    public void wrong_attribute_name() {
        execute(Input.of(BOND, KRUEGER, HUNT_WITH_NULLS),
                Query.of(Predicates.equal("name12312", "Bond"), mv),
                Expected.of(QueryException.class));
    }

    @Test
    public void nested_wrong_attribute_name() {
        execute(Input.of(BOND, KRUEGER),
                Query.of(Predicates.equal("firstLimb.name12312", "left-hand"), mv),
                Expected.of(QueryException.class));
    }

    @Test
    public void wrong_attribute_name_compared_to_null() {
        execute(Input.of(BOND, KRUEGER, HUNT_WITH_NULLS),
                Query.of(Predicates.equal("name12312", null), mv),
                Expected.of(QueryException.class));
    }

    @Test
    public void primitiveNull_comparedToNull_matching() {
        execute(Input.of(BOND, KRUEGER, HUNT_WITH_NULLS),
                Query.of(Predicates.equal("name", null), mv),
                Expected.of(HUNT_WITH_NULLS));
    }

    @Test
    public void primitiveNull_comparedToNotNull_notMatching() {
        execute(Input.of(BOND, KRUEGER, HUNT_WITH_NULLS),
                Query.of(Predicates.equal("name", "Non-null-value"), mv),
                Expected.empty());
    }

    @Test
    public void nestedAttribute_firstIsNull_comparedToNotNull() {
        execute(Input.of(BOND, KRUEGER, HUNT_WITH_NULLS),
                Query.of(Predicates.equal("secondLimb.name", "Non-null-value"), mv),
                Expected.empty());
    }

    @Test
    public void nestedAttribute_firstIsNull_comparedToNull() {
        execute(Input.of(BOND, KRUEGER, HUNT_WITH_NULLS),
                Query.of(Predicates.equal("secondLimb.name", null), mv),
                Expected.of(HUNT_WITH_NULLS));
    }

    @Parameterized.Parameters(name = "{index}: {0}, {1}, {2}")
    public static Collection<Object[]> parametrisationData() {
        return axes(
                asList(BINARY, OBJECT),
                asList(NO_INDEX, UNORDERED, ORDERED),
                asList(SINGLE)
        );
    }

}
