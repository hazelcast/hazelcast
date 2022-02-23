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

package com.hazelcast.internal.diagnostics;

import com.hazelcast.cluster.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.impl.operationservice.BackupOperation;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.OperationFactory;
import com.hazelcast.spi.impl.operationservice.impl.DummyOperation;
import com.hazelcast.spi.impl.operationservice.impl.operations.Backup;
import com.hazelcast.spi.impl.operationservice.impl.operations.PartitionIteratingOperation;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.net.UnknownHostException;

import static com.hazelcast.internal.diagnostics.OperationDescriptors.toOperationDesc;
import static java.lang.String.format;
import static org.junit.Assert.assertEquals;

/**
 * Idea: in the future we could check the operation 'names', e.g. it would be helpful to figure out if a Get operation from
 * employees map would be slow. Currently you would see just 'Get operation'
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class OperationDescriptorsTest extends HazelcastTestSupport {

    @Test
    public void testConstructor() {
        assertUtilityConstructor(OperationDescriptors.class);
    }

    @Test
    public void testNormalOperation() {
        assertEquals(DummyOperation.class.getName(), toOperationDesc(new DummyOperation()));
    }

    @Test
    public void testBackupOperation() throws UnknownHostException {
        Backup backup = new Backup(new DummyBackupOperation(), new Address("127.0.0.1", 5701), new long[]{}, false);
        String result = toOperationDesc(backup);
        assertEquals(format("Backup(%s)", DummyBackupOperation.class.getName()), result);
    }

    @Test
    public void testPartitionIteratingOperation() {
        PartitionIteratingOperation op = new PartitionIteratingOperation(new DummyOperationFactory(), new int[0]);
        String result = toOperationDesc(op);
        assertEquals(format("PartitionIteratingOperation(%s)", DummyOperationFactory.class.getName()), result);
    }

    static class DummyOperationFactory implements OperationFactory {
        @Override
        public Operation createOperation() {
            return new DummyOperation();
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
        }

        @Override
        public int getFactoryId() {
            return 0;
        }

        @Override
        public int getClassId() {
            return 0;
        }
    }

    static class DummyBackupOperation extends Operation implements BackupOperation {
        @Override
        public void run() throws Exception {
        }
    }
}
