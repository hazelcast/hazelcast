/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.query.impl;

import com.hazelcast.config.IndexType;
import com.hazelcast.core.HazelcastJsonValue;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.query.Predicates;
import com.hazelcast.query.impl.getters.Extractors;
import com.hazelcast.query.impl.predicates.AndPredicate;
import com.hazelcast.query.impl.predicates.EqualPredicate;
import com.hazelcast.test.HazelcastSerialParametersRunnerFactory;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
@UseParametersRunnerFactory(HazelcastSerialParametersRunnerFactory.class)
@Category(QuickTest.class)
public class IndexJsonTest {

    @Parameter(0)
    public IndexCopyBehavior copyBehavior;

    @Parameters(name = "copyBehavior: {0}")
    public static Collection<Object[]> parameters() {
        return Arrays.asList(new Object[][]{
                {IndexCopyBehavior.COPY_ON_READ},
                {IndexCopyBehavior.COPY_ON_WRITE},
                {IndexCopyBehavior.NEVER},
        });
    }

    final InternalSerializationService ss = new DefaultSerializationServiceBuilder().build();

    @Test
    public void testJsonIndex() {
        InternalSerializationService ss = new DefaultSerializationServiceBuilder().build();
        Indexes is = Indexes.newBuilder(ss, copyBehavior).extractors(Extractors.newBuilder(ss).build()).indexProvider(
                new DefaultIndexProvider()).usesCachedQueryableEntries(true).statsEnabled(true).global(true).build();
        Index numberIndex = is.addOrGetIndex(IndexUtils.createTestIndexConfig(IndexType.HASH, "age"), null);
        Index boolIndex = is.addOrGetIndex(IndexUtils.createTestIndexConfig(IndexType.HASH, "active"), null);
        Index stringIndex = is.addOrGetIndex(IndexUtils.createTestIndexConfig(IndexType.HASH, "name"), null);

        for (int i = 0; i < 1001; i++) {
            Data key = ss.toData(i);
            String jsonString = "{\"age\" : " + i + "  , \"name\" : \"sancar\" , \"active\" :  " + (i % 2 == 0) + " } ";
            is.putEntry(new QueryEntry(ss, key, new HazelcastJsonValue(jsonString), Extractors.newBuilder(ss).build()), null, Index.OperationSource.USER);
        }

        assertEquals(1, numberIndex.getRecords(10).size());
        assertEquals(0, numberIndex.getRecords(-1).size());
        assertEquals(1001, stringIndex.getRecords("sancar").size());
        assertEquals(501, boolIndex.getRecords(true).size());
        assertEquals(501, is.query(new AndPredicate(new EqualPredicate("name", "sancar"), new EqualPredicate("active", "true"))).size());
        assertEquals(300, is.query(Predicates.and(Predicates.greaterThan("age", 400), Predicates.equal("active", true))).size());
        assertEquals(1001, is.query(Predicates.sql("name == sancar")).size());
    }

}
