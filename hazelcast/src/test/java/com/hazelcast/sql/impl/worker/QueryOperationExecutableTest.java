/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.sql.impl.worker;

import com.hazelcast.internal.nio.Packet;
import com.hazelcast.sql.impl.operation.QueryExecuteOperation;
import com.hazelcast.sql.impl.operation.QueryOperation;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class QueryOperationExecutableTest {
    @Test
    public void testLocalExecutable() {
        QueryOperation operation = new QueryExecuteOperation();

        QueryOperationExecutable localExecutable = QueryOperationExecutable.local(operation);

        assertSame(operation, localExecutable.getLocalOperation());
        assertNull(localExecutable.getRemoteOperation());
    }

    @Test
    public void testRemoteExecutable() {
        Packet packet = new Packet();

        QueryOperationExecutable remoteExecutable = QueryOperationExecutable.remote(packet);

        assertNull(remoteExecutable.getLocalOperation());
        assertSame(packet, remoteExecutable.getRemoteOperation());
    }
}
