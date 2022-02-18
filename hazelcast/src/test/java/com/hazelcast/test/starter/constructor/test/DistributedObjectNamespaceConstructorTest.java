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

package com.hazelcast.test.starter.constructor.test;

import com.hazelcast.internal.services.DistributedObjectNamespace;
import com.hazelcast.internal.services.ObjectNamespace;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.test.starter.constructor.DistributedObjectNamespaceConstructor;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.ringbuffer.impl.RingbufferService.getRingbufferNamespace;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class DistributedObjectNamespaceConstructorTest {

    @Test
    public void testConstructor() {
        ObjectNamespace objectNamespace = getRingbufferNamespace("myRingbuffer");

        DistributedObjectNamespaceConstructor constructor
                = new DistributedObjectNamespaceConstructor(DistributedObjectNamespace.class);
        ObjectNamespace clonedObjectNamespace = (ObjectNamespace) constructor.createNew(objectNamespace);

        assertEquals(objectNamespace.getServiceName(), clonedObjectNamespace.getServiceName());
        assertEquals(objectNamespace.getObjectName(), clonedObjectNamespace.getObjectName());
    }
}
