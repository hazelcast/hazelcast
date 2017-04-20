/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.nio.BufferObjectDataOutput;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.io.Serializable;

import static java.util.Arrays.asList;
import static org.junit.Assert.*;

@Category(QuickTest.class)
public class ReceiverTaskletTest {

    private ReceiverTasklet t;
    private InternalSerializationService serService;
    private MockOutboundCollector collector;

    @Before
    public void before() {
        collector = new MockOutboundCollector(2);
        t = new ReceiverTasklet(collector, 3, 100);
        serService = new DefaultSerializationServiceBuilder().build();
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
        t.receiveStreamPacket(serService.createObjectDataInput(out.toByteArray()));
    }
}
