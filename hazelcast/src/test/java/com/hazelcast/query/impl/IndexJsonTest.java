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

package com.hazelcast.query.impl;

import com.hazelcast.config.IndexType;
import com.hazelcast.core.HazelcastJsonValue;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.query.Predicates;
import com.hazelcast.query.impl.getters.Extractors;
import com.hazelcast.query.impl.predicates.AndPredicate;
import com.hazelcast.query.impl.predicates.EqualPredicate;
import com.hazelcast.query.impl.predicates.SqlPredicate;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.HazelcastSerialParametersRunnerFactory;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.util.Arrays;
import java.util.Collection;

import static com.hazelcast.config.MapConfig.DEFAULT_IN_MEMORY_FORMAT;
import static com.hazelcast.internal.util.IterableUtil.size;
import static com.hazelcast.query.impl.Indexes.SKIP_PARTITIONS_COUNT_CHECK;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParametrizedRunner.class)
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
        Indexes indexes = Indexes.newBuilder(ss, copyBehavior, DEFAULT_IN_MEMORY_FORMAT).extractors(Extractors.newBuilder(ss).build()).indexProvider(
                new DefaultIndexProvider()).usesCachedQueryableEntries(true).statsEnabled(true).global(true).build();
        Index numberIndex = indexes.addOrGetIndex(IndexUtils.createTestIndexConfig(IndexType.HASH, "age"));
        Index boolIndex = indexes.addOrGetIndex(IndexUtils.createTestIndexConfig(IndexType.HASH, "active"));
        Index stringIndex = indexes.addOrGetIndex(IndexUtils.createTestIndexConfig(IndexType.HASH, "name"));

        for (int i = 0; i < 1001; i++) {
            Data key = ss.toData(i);
            String jsonString = "{\"age\" : " + i + "  , \"name\" : \"sancar\" , \"active\" :  " + (i % 2 == 0) + " } ";
            indexes.putEntry(new QueryEntry(ss, key, new HazelcastJsonValue(jsonString), Extractors.newBuilder(ss).build()), null, Index.OperationSource.USER);
        }

        assertEquals(1, numberIndex.getRecords(10).size());
        assertEquals(0, numberIndex.getRecords(-1).size());
        assertEquals(1001, stringIndex.getRecords("sancar").size());
        assertEquals(501, boolIndex.getRecords(true).size());
        assertEquals(501, size(indexes.query(new AndPredicate(new EqualPredicate("name", "sancar"), new EqualPredicate("active", "true")), SKIP_PARTITIONS_COUNT_CHECK)));
        assertEquals(300, size(indexes.query(Predicates.and(Predicates.greaterThan("age", 400), Predicates.equal("active", true)), SKIP_PARTITIONS_COUNT_CHECK)));
        assertEquals(1001, size(indexes.query(new SqlPredicate("name == sancar"), SKIP_PARTITIONS_COUNT_CHECK)));
    }
}
