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

package com.hazelcast.internal.monitor.jmx;

import com.hazelcast.internal.jmx.MBeanDataHolder;
import com.hazelcast.spi.impl.executionservice.ExecutionService;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class VectorQueryExecutorMBeanTest extends HazelcastTestSupport {

    private static final String EXECUTOR_TYPE_NAME = "HazelcastInstance.ManagedExecutorService";

    private final TestHazelcastInstanceFactory hazelcastInstanceFactory = createHazelcastInstanceFactory(1);
    private MBeanDataHolder holder;

    private void createInstance() {
        holder = new MBeanDataHolder(hazelcastInstanceFactory);
    }

    @After
    public void tearDown() {
        hazelcastInstanceFactory.shutdownAll();
    }

    @Test
    public void testVectorQueryExecutorMBeanExists() {
        createInstance();

        holder.assertMBeanExistEventually(EXECUTOR_TYPE_NAME, ExecutionService.VECTOR_QUERY_EXECUTOR);
    }
}
