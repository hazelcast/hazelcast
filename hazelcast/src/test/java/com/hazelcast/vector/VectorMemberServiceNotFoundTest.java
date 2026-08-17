/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.vector;

import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.vector.Metric;
import com.hazelcast.config.vector.VectorCollectionConfig;
import com.hazelcast.config.vector.VectorIndexConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.vector.impl.spi.VectorCollectionLocator.MISSED_VECTOR_MODULE_MESSAGE;
import static org.assertj.core.api.Assertions.assertThatCode;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class VectorMemberServiceNotFoundTest extends HazelcastTestSupport {

    private static final String VECTOR_COLLECTION_NAME = "vector-collection";

    private TestHazelcastFactory factory;
    private HazelcastInstance member;

    @Before
    public void setUp() {
        factory = new TestHazelcastFactory();
        member = factory.newHazelcastInstance(smallInstanceConfig());
    }

    @After
    public void tearDown() {
        factory.shutdownAll();
    }

    @Test
    public void getVectorCollection_whenVectorServiceMissing_throwsHazelcastExceptionWithServiceNotFoundCause() {
        assertThatCode(() -> member.getVectorCollection(VECTOR_COLLECTION_NAME))
            .isInstanceOf(UnsupportedOperationException.class)
            .hasMessage(MISSED_VECTOR_MODULE_MESSAGE);
    }

    @Test
    public void addVectorCollectionConfig_whenVectorServiceMissing_doesNotThrow() {
        VectorCollectionConfig config = new VectorCollectionConfig(VECTOR_COLLECTION_NAME)
            .addVectorIndexConfig(new VectorIndexConfig().setDimension(1).setMetric(Metric.DOT));
        assertThatCode(() -> member.getConfig().addVectorCollectionConfig(config)).doesNotThrowAnyException();
    }
}
