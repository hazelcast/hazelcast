/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.concurrent.lock;

import com.hazelcast.internal.cluster.Versions;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.internal.serialization.impl.SerializationServiceV1;
import com.hazelcast.nio.BufferObjectDataInput;
import com.hazelcast.nio.BufferObjectDataOutput;
import com.hazelcast.spi.DefaultObjectNamespace;
import com.hazelcast.spi.DistributedObjectNamespace;
import com.hazelcast.spi.ObjectNamespace;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.version.Version;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ObjectNamespaceSerializationHelperTest extends HazelcastTestSupport {

    private SerializationServiceV1 ss;

    @Before
    public void init() {
        final DefaultSerializationServiceBuilder builder = new DefaultSerializationServiceBuilder();
        ss = builder.setVersion(InternalSerializationService.VERSION_1).build();
    }

    @Test
    public void testConstructor() {
        assertUtilityConstructor(ObjectNamespaceSerializationHelper.class);
    }

    @Test
    public void writeNamespaceCompatibly() throws Exception {
        final DefaultObjectNamespace defaultNS = new DefaultObjectNamespace("service", "object");
        final DistributedObjectNamespace distributedNS = new DistributedObjectNamespace("service", "object");

        for (Version v : Arrays.asList(Versions.V3_8, Versions.V3_9)) {
            assertTrue(Arrays.equals(serialize(defaultNS, v), serialize(distributedNS, v)));
        }
    }

    @Test
    public void readNamespaceCompatibly() throws Exception {
        final DefaultObjectNamespace defaultNS = new DefaultObjectNamespace("service", "object");
        final DistributedObjectNamespace distributedNS = new DistributedObjectNamespace("service", "object");

        for (Version v : Arrays.asList(Versions.V3_8, Versions.V3_9)) {
            final ObjectNamespace o1 = serialiseDeserialise(defaultNS, v);
            final ObjectNamespace o2 = serialiseDeserialise(distributedNS, v);
            assertTrue(o1 instanceof DistributedObjectNamespace && o2 instanceof DistributedObjectNamespace);
            assertEquals(o1, o2);
        }
    }

    private ObjectNamespace serialiseDeserialise(ObjectNamespace ns, Version streamVersion) throws IOException {
        final byte[] bytes = serialize(ns, streamVersion);
        final BufferObjectDataInput input = ss.createObjectDataInput(bytes);
        return ObjectNamespaceSerializationHelper.readNamespaceCompatibly(input);
    }

    private byte[] serialize(ObjectNamespace ns, Version streamVersion) throws IOException {
        final BufferObjectDataOutput out = ss.createObjectDataOutput();
        out.setVersion(streamVersion);
        ObjectNamespaceSerializationHelper.writeNamespaceCompatibly(ns, out);
        return out.toByteArray();
    }
}
