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

package com.hazelcast.client.protocol;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.MapPutCodec;
import com.hazelcast.client.impl.protocol.util.ClientProtocolBuffer;
import com.hazelcast.client.impl.protocol.util.SafeBuffer;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.serialization.SerializationService;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Encode Decode Tests
 */
@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class MapMessageEncodeDecodeTest {

    private static final SerializationService serializationService = new DefaultSerializationServiceBuilder().build();

    private static final String NAME = "name";
    private static final Data DATA = serializationService.toData("The Test");
    private static final long THE_LONG = 0xFFFFL;

    private ClientProtocolBuffer byteBuffer;

    @Before
    public void setUp() {
        byteBuffer = new SafeBuffer(new byte[20]);
    }

    @Test
    public void shouldEncodeDecodeCorrectly_PUT() {
        final int calculatedSize = MapPutCodec.RequestParameters.calculateDataSize(NAME, DATA, DATA, THE_LONG, THE_LONG);
        ClientMessage cmEncode = MapPutCodec.encodeRequest(NAME, DATA, DATA, THE_LONG, THE_LONG);
        cmEncode.setVersion((short) 3).addFlag(ClientMessage.BEGIN_AND_END_FLAGS)
                .setCorrelationId(Long.MAX_VALUE).setPartitionId(77);

        assertTrue(calculatedSize > cmEncode.getFrameLength());
        byteBuffer = cmEncode.buffer();

        ClientMessage cmDecode = ClientMessage.createForDecode(byteBuffer, 0);
        MapPutCodec.RequestParameters decodeParams = MapPutCodec.decodeRequest(cmDecode);

        assertEquals(MapPutCodec.REQUEST_TYPE.id(), cmDecode.getMessageType());
        assertEquals(3, cmDecode.getVersion());
        assertEquals(ClientMessage.BEGIN_AND_END_FLAGS, cmDecode.getFlags());
        assertEquals(Long.MAX_VALUE, cmDecode.getCorrelationId());
        assertEquals(77, cmDecode.getPartitionId());

        assertEquals(NAME, decodeParams.name);
        assertEquals(DATA, decodeParams.key);
        assertEquals(DATA, decodeParams.value);
        assertEquals(THE_LONG, decodeParams.threadId);
        assertEquals(THE_LONG, decodeParams.ttl);
    }

}
