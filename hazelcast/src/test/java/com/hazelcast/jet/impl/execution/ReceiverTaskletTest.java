/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.impl.execution;

import com.hazelcast.cluster.Address;
import com.hazelcast.internal.nio.BufferObjectDataOutput;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.logging.LoggingService;
import com.hazelcast.test.HazelcastSerialClassRunner;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.IOException;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

@RunWith(HazelcastSerialClassRunner.class)
public class ReceiverTaskletTest {

    private ReceiverTasklet t;
    private MockOutboundCollector collector;
    private InternalSerializationService serService;

    @Before
    public void before() {
        collector = new MockOutboundCollector(2);
        serService = new DefaultSerializationServiceBuilder().build();
        t = new ReceiverTasklet(collector, serService, 3, 100, mock(LoggingService.class),
                new Address(), 0, "", null, "");
    }

    @Test
    public void when_receiveTwoObjects_then_emitThem() throws IOException {
        pushObjects(1, 2);
        t.call();
        assertEquals(asList(1, 2), collector.getBuffer());
    }

    private void pushObjects(Object... objs) throws IOException {
        final BufferObjectDataOutput out = serService.createObjectDataOutput();
        out.writeInt(objs.length);
        for (Object obj : objs) {
            out.writeObject(obj);
            out.writeInt(Math.abs(obj.hashCode())); // partition id
        }
        t.receiveStreamPacket(out.toByteArray(), 0);
    }
}
