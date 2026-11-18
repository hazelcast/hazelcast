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

package com.hazelcast.spi.impl.operationexecutor.slowoperationdetector;

import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class SlowOperationLogTest {

    @Test
    public void taskDescriptorProvider_descriptionIsAppendedToClassName() {
        SlowOperationLog log = new SlowOperationLog("stackTrace", new DescribedTask());

        assertEquals(DescribedTask.class.getName()
                + " (operation=com.hazelcast.map.impl.operation.PutOperation,"
                + " map=map-0, partitionId=1)", log.operation);
        assertEquals(log.operation, log.createDTO().operation);
    }

    @Test
    public void taskDescriptorProvider_keepsClassNameAsPrefix() {
        SlowOperationLog log = new SlowOperationLog("stackTrace", new DescribedTask());

        // Existing diagnostics and Management Center consumers match on the
        // operation class name, so it must remain the first part of the value.
        assertTrue(log.operation.startsWith(DescribedTask.class.getName()));
    }

    @Test
    public void operationTask_keepsOperationDescriptor() {
        Operation operation = new Operation() {
        };
        SlowOperationLog log = new SlowOperationLog("stackTrace", operation);

        assertEquals(operation.getClass().getName(), log.operation);
    }

    @Test
    public void taskWithoutDescriptor_keepsClassNameOnly() {
        SlowOperationLog log = new SlowOperationLog("stackTrace", new Object());

        assertEquals(Object.class.getName(), log.operation);
        assertEquals(log.operation, log.createDTO().operation);
    }

    private static final class DescribedTask implements TaskDescriptor {
        @Override
        public String getTaskDescription() {
            return "operation=com.hazelcast.map.impl.operation.PutOperation,"
                    + " map=map-0, partitionId=1";
        }
    }
}
