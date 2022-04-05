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

package com.hazelcast.spi.impl.operationservice.impl.operations;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.OperationFactory;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;

import static com.hazelcast.spi.impl.operationservice.impl.operations.PartitionAwareFactoryAccessor.extractPartitionAware;
import static org.junit.Assert.assertNull;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class PartitionAwareFactoryAccessorTest extends HazelcastTestSupport {

    @Test
    public void testConstructor() {
        assertUtilityConstructor(PartitionAwareFactoryAccessor.class);
    }

    @Test
    public void returns_null_when_supplied_factory_null() {
        assertNull(extractPartitionAware(null));
    }

    @Test
    public void returns_null_when_supplied_factory_is_not_partition_aware() {
        assertNull(extractPartitionAware(new RawOpFactory()));
    }

    @Test
    public void returns_partition_aware_factory_when_supplied_factory_is_partition_aware() {
        PartitionAwareOpFactory factory = new PartitionAwareOpFactory();
        PartitionAwareOperationFactory extractedFactory = extractPartitionAware(factory);

        assertInstanceOf(PartitionAwareOperationFactory.class, extractedFactory);
    }

    private static class RawOpFactory implements OperationFactory {

        @Override
        public int getFactoryId() {
            return 0;
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
        }

        @Override
        public Operation createOperation() {
            return null;
        }

        @Override
        public int getClassId() {
            return 0;
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
        }
    }

    private static class PartitionAwareOpFactory extends PartitionAwareOperationFactory {

        @Override
        public int getFactoryId() {
            return 0;
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
        }

        @Override
        public Operation createPartitionOperation(int partition) {
            return null;
        }

        @Override
        public Operation createOperation() {
            return null;
        }

        @Override
        public int getClassId() {
            return 0;
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
        }
    }
}
