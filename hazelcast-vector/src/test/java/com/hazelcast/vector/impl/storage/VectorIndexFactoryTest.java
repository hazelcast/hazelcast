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

package com.hazelcast.vector.impl.storage;

import com.hazelcast.config.vector.VectorIndexConfig;
import com.hazelcast.internal.util.RuntimeAvailableProcessors;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.properties.HazelcastProperties;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.lang.reflect.Field;
import java.util.concurrent.ForkJoinPool;

import static com.hazelcast.config.vector.Metric.DOT;
import static com.hazelcast.spi.properties.ClusterProperty.INDEX_ISOLATED_EXECUTOR_MAX_PARALLELISM;
import static com.hazelcast.spi.properties.ClusterProperty.USE_INDEX_ISOLATED_EXECUTOR;
import static com.hazelcast.test.HazelcastTestSupport.smallInstanceConfig;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class VectorIndexFactoryTest {

    private NodeEngine nodeEngine;

    @Before
    public void setUp() {
        nodeEngine = mock(NodeEngine.class, RETURNS_DEEP_STUBS);
        HazelcastProperties properties = new HazelcastProperties(smallInstanceConfig());
        when(nodeEngine.getHazelcastInstance().getName()).thenReturn("hz-test");
        when(nodeEngine.getProperties()).thenReturn(properties);
    }

    @After
    public void tearDown() {
        System.clearProperty(USE_INDEX_ISOLATED_EXECUTOR.getName());
        System.clearProperty(INDEX_ISOLATED_EXECUTOR_MAX_PARALLELISM.getName());
    }

    @Test
    public void shouldCreateMultipleKeysIndexWhenDeduplicationEnabled() {
        VectorIndexFactory factory = new VectorIndexFactory(nodeEngine);
        VectorIndexConfig config = new VectorIndexConfig()
            .setName("index")
            .setDimension(1)
            .setMetric(DOT)
            .setUseDeduplication(true);
        AbstractVectorIndex index = factory.create(config);
        assertTrue(index instanceof VectorIndexMultipleKeys);
    }

    @Test
    public void shouldCreateSingleKeyIndexWhenDeduplicationDisabled() {
        VectorIndexFactory factory = new VectorIndexFactory(nodeEngine);
        VectorIndexConfig config = new VectorIndexConfig()
            .setName("index")
            .setDimension(1)
            .setMetric(DOT)
            .setUseDeduplication(false);
        AbstractVectorIndex index = factory.create(config);
        assertTrue(index instanceof VectorIndexSingleKey);
    }

    @Test
    public void shouldUseCommonPoolWhenExecutorDisabled() {
        System.setProperty(
            USE_INDEX_ISOLATED_EXECUTOR.getName(),
            "false"
        );

        VectorIndexFactory factory = new VectorIndexFactory(nodeEngine);
        ForkJoinPool pool = extractPool(factory);
        assertSame(ForkJoinPool.commonPool(), pool);
    }

    @Test
    public void shouldCreateCustomPoolWhenExecutorEnabled() {
        System.setProperty(
            USE_INDEX_ISOLATED_EXECUTOR.getName(),
            "true"
        );

        System.setProperty(
            INDEX_ISOLATED_EXECUTOR_MAX_PARALLELISM.getName(),
            "1"
        );

        VectorIndexFactory factory = new VectorIndexFactory(nodeEngine);
        ForkJoinPool pool = extractPool(factory);

        assertNotNull(pool);
        assertNotSame(ForkJoinPool.commonPool(), pool);
        assertEquals(pool.getParallelism(), 1);
    }

    @Test
    public void shouldCreateCustomPoolByDefault() {
        VectorIndexFactory factory = new VectorIndexFactory(nodeEngine);
        ForkJoinPool pool = extractPool(factory);

        assertNotNull(pool);
        assertNotSame(ForkJoinPool.commonPool(), pool);
        assertEquals(pool.getParallelism(), RuntimeAvailableProcessors.get() - 1);
    }

    @Test
    public void shouldNotShutdownCommonPool() {
        System.setProperty(
            USE_INDEX_ISOLATED_EXECUTOR.getName(),
            "false"
        );

        VectorIndexFactory factory = new VectorIndexFactory(nodeEngine);
        factory.shutdown();
        assertFalse(ForkJoinPool.commonPool().isShutdown());
    }

    private ForkJoinPool extractPool(VectorIndexFactory factory) {
        try {
            Field field = VectorIndexFactory.class.getDeclaredField("parallelExecutor");
            field.setAccessible(true);
            return (ForkJoinPool) field.get(factory);
        } catch (Exception e) {
            throw new RuntimeException("Failed to extract parallelExecutor", e);
        }
    }
}
