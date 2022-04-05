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

package com.hazelcast.internal.partition;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.partition.impl.InternalPartitionImpl;
import com.hazelcast.internal.partition.impl.InternalPartitionServiceImpl;
import com.hazelcast.spi.exception.PartitionMigratingException;
import com.hazelcast.spi.impl.InternalCompletableFuture;
import com.hazelcast.spi.impl.operationservice.ExceptionAction;
import com.hazelcast.spi.impl.operationservice.InvocationBuilder;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.ReadonlyOperation;
import com.hazelcast.spi.impl.operationservice.impl.OperationServiceImpl;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.ExecutionException;

import static com.hazelcast.test.Accessors.getOperationService;
import static com.hazelcast.test.Accessors.getPartitionService;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class StaleReadDuringMigrationTest extends HazelcastTestSupport {

    @Test
    public void testReadOperationFailsWhenStaleReadDisabledDuringMigration()
            throws ExecutionException, InterruptedException {
        final Config config = new Config();
        config.setProperty(ClusterProperty.DISABLE_STALE_READ_ON_PARTITION_MIGRATION.getName(), "true");

        final InternalCompletableFuture<Boolean> future = invokeOperation(config);
        try {
            future.get();
            fail();
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof PartitionMigratingException);
        }
    }

    @Test
    public void testReadOperationSucceedsWhenStaleReadEnabledDuringMigration()
            throws ExecutionException, InterruptedException {
        final InternalCompletableFuture<Boolean> future = invokeOperation(new Config());

        assertTrue(future.get());
    }

    private InternalCompletableFuture<Boolean> invokeOperation(final Config config) {
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(1);
        final HazelcastInstance instance = factory.newHazelcastInstance(config);
        warmUpPartitions(instance);

        final int partitionId = 0;
        final InternalPartitionServiceImpl partitionService = (InternalPartitionServiceImpl) getPartitionService(instance);
        final InternalPartitionImpl partition = (InternalPartitionImpl) partitionService.getPartition(partitionId);
        partition.setMigrating();

        final OperationServiceImpl operationService = getOperationService(instance);
        final InvocationBuilder invocationBuilder = operationService
                .createInvocationBuilder(InternalPartitionService.SERVICE_NAME, new DummyOperation(), partitionId);
        return invocationBuilder.invoke();
    }

    private static class DummyOperation extends Operation implements ReadonlyOperation {

        @Override
        public void run() throws Exception {
        }

        @Override
        public Object getResponse() {
            return true;
        }

        @Override
        public ExceptionAction onInvocationException(Throwable throwable) {
            if (throwable instanceof PartitionMigratingException) {
                return ExceptionAction.THROW_EXCEPTION;
            }
            return super.onInvocationException(throwable);
        }
    }
}
