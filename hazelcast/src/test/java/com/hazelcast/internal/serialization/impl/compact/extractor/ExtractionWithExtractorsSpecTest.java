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

import com.hazelcast.config.AttributeConfig;
import com.hazelcast.config.CompactSerializationConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.query.Predicates;
import com.hazelcast.query.QueryException;
import com.hazelcast.query.extractor.ValueCallback;
import com.hazelcast.query.extractor.ValueCollector;
import com.hazelcast.query.extractor.ValueExtractor;
import com.hazelcast.query.extractor.ValueReader;
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
import static com.hazelcast.internal.serialization.impl.compact.extractor.ComplexTestDataStructure.Person;
import static com.hazelcast.internal.serialization.impl.compact.extractor.ComplexTestDataStructure.finger;
import static com.hazelcast.internal.serialization.impl.compact.extractor.ComplexTestDataStructure.limb;
import static com.hazelcast.internal.serialization.impl.compact.extractor.ComplexTestDataStructure.person;
import static com.hazelcast.internal.serialization.impl.compact.extractor.ComplexTestDataStructure.tattoos;
import static java.util.Arrays.asList;

/**
 * Specification test that verifies the behavior of corner-cases extraction with extractor and arguments.
 * <p>
 * Extraction mechanism: EXTRACTOR-BASED EXTRACTION
 * <p>
 * This test is parametrised on two axes (see the parametrisationData() method):
 * - in memory format
 * - indexing
 * - extraction in collections and arrays
 */
@RunWith(HazelcastParametrizedRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ExtractionWithExtractorsSpecTest extends AbstractExtractionTest {

    private static final Person BOND = person("Bond",
            limb("left-hand", tattoos(), finger("thumb"), finger(null)),
            limb("right-hand", tattoos("knife"), finger("middle"), finger("index"))
    );

    private static final Person KRUEGER = person("Krueger",
            limb("linke-hand", tattoos("bratwurst"), finger("Zeigefinger"), finger("Mittelfinger")),
            limb("rechte-hand", tattoos(), finger("Ringfinger"), finger("Daumen"))
    );

    private static final Person HUNT_NULL_LIMB = person("Hunt");

    public ExtractionWithExtractorsSpecTest(Index index) {
        super(index);
    }

    @Override
    public void doWithConfig(Config config) {
        MapConfig mapConfig = config.getMapConfig("map");
        AttributeConfig tattoosCount = new AttributeConfig();
        tattoosCount.setName("tattoosCount");
        tattoosCount.setExtractorClassName("com.hazelcast.internal.serialization.impl.compact.extractor.ExtractionWithExtractorsSpecTest$LimbTattoosCountExtractor");
        mapConfig.addAttributeConfig(tattoosCount);
        config.getSerializationConfig().setCompactSerializationConfig(new CompactSerializationConfig().setEnabled(true));
    }

    @Parameterized.Parameters(name = "{index}: {0}")
    public static Collection<Object[]> parametrisationData() {
        return axes(asList(NO_INDEX, HASH, BITMAP));
    }

    @Test
    public void extractorWithParam_bondCase() {
        execute(Input.of(BOND, KRUEGER),
                Query.of(Predicates.equal("tattoosCount[1]", 1)),
                Expected.of(BOND));
    }

    @Test
    public void extractorWithParam_kruegerCase() {
        execute(Input.of(BOND, KRUEGER),
                Query.of(Predicates.equal("tattoosCount[0]", 1)),
                Expected.of(KRUEGER));
    }

    @Test(expected = QueryException.class)
    public void extractorWithParam_nullCollection() {
        execute(Input.of(HUNT_NULL_LIMB),
                Query.of(Predicates.equal("tattoosCount[0]", 1)),
                Expected.empty());
    }

    @Test(expected = QueryException.class)
    public void extractorWithParam_indexOutOfBound() {
        execute(Input.of(BOND, KRUEGER, HUNT_NULL_LIMB),
                Query.of(Predicates.equal("tattoosCount[2]", 1)),
                Expected.empty());
    }

    @Test(expected = QueryException.class)
    public void extractorWithParam_negativeInput() {
        execute(Input.of(BOND, KRUEGER, HUNT_NULL_LIMB),
                Query.of(Predicates.equal("tattoosCount[-1]", 1)),
                Expected.empty());
    }

    @Test(expected = IllegalArgumentException.class)
    public void extractorWithParam_wrongInput_noClosingWithArg() {
        execute(Input.of(BOND, KRUEGER, HUNT_NULL_LIMB),
                Query.of(Predicates.equal("tattoosCount[0", 1)),
                Expected.empty());
    }

    @Test(expected = IllegalArgumentException.class)
    public void extractorWithParam_wrongInput_noOpeningWithArg() {
        execute(Input.of(BOND, KRUEGER, HUNT_NULL_LIMB),
                Query.of(Predicates.equal("tattoosCount0]", 1)),
                Expected.empty());
    }

    @Test(expected = IllegalArgumentException.class)
    public void extractorWithParam_wrongInput_noClosing() {
        execute(Input.of(BOND, KRUEGER, HUNT_NULL_LIMB),
                Query.of(Predicates.equal("tattoosCount[", 1)),
                Expected.empty());
    }

    @Test(expected = IllegalArgumentException.class)
    public void extractorWithParam_wrongInput_noOpening() {
        execute(Input.of(BOND, KRUEGER, HUNT_NULL_LIMB),
                Query.of(Predicates.equal("tattoosCount]", 1)),
                Expected.empty());
    }

    @Test(expected = QueryException.class)
    public void extractorWithParam_wrongInput_noArgumentWithBrackets() {
        execute(Input.of(BOND, KRUEGER, HUNT_NULL_LIMB),
                Query.of(Predicates.equal("tattoosCount[]", 1)),
                Expected.empty());
    }

    @Test(expected = QueryException.class)
    public void extractorWithParam_wrongInput_noArgumentNoBrackets() {
        execute(Input.of(BOND, KRUEGER, HUNT_NULL_LIMB),
                Query.of(Predicates.equal("tattoosCount", 1)),
                Expected.empty());
    }

    @Test(expected = QueryException.class)
    public void extractorWithParam_wrongInput_squareBracketsInInput() {
        execute(Input.of(BOND, KRUEGER, HUNT_NULL_LIMB),
                Query.of(Predicates.equal("tattoosCount[1183[2]3]", 1)),
                Expected.empty());
    }


    @SuppressWarnings("unchecked")
    public static class LimbTattoosCountExtractor implements ValueExtractor {
        @Override
        public void extract(Object target, Object arguments, final ValueCollector collector) {
            Integer parsedId = Integer.parseInt((String) arguments);
            if (target instanceof Person) {
                Integer size = ((Person) target).limbs_array[parsedId].tattoos_array.length;
                collector.addObject(size);
            } else {
                ValueReader reader = (ValueReader) target;
                reader.read("limbs_array[" + parsedId + "].tattoos_array", new ValueCallback() {
                    @Override
                    public void onResult(Object value) {
                        collector.addObject(((String[]) value).length);
                    }
                });
            }
        }
    }

}
