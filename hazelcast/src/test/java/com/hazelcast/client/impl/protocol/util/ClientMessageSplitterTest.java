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

package com.hazelcast.client.impl.protocol.util;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.ClientAuthenticationCodec;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.RequireAssertEnabled;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.LinkedList;
import java.util.List;
import java.util.UUID;

import static com.hazelcast.client.impl.protocol.util.ClientMessageSplitter.getFragments;
import static groovy.util.GroovyTestCase.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientMessageSplitterTest extends HazelcastTestSupport {

    private ClientMessage clientMessage;

    @Before
    public void setUp() throws Exception {
        String clientName = generateRandomString(1000);
        String username = generateRandomString(1000);
        String password = generateRandomString(1000);
        UUID uuid = UUID.randomUUID();
        String clientType = generateRandomString(1000);
        String clientSerializationVersion = generateRandomString(1000);
        String clusterName = generateRandomString(1000);
        LinkedList<String> labels = new LinkedList<>();
        for (int i = 0; i < 10; i++) {
            labels.add(generateRandomString(1000));
        }

        clientMessage = ClientAuthenticationCodec.encodeRequest(clientName, username, password, uuid,
                clientType, (byte) 1, clientSerializationVersion, clusterName, labels);
    }

    @Test
    public void testConstructor() {
        assertUtilityConstructor(ClientMessageSplitter.class);
    }

    @Test
    public void testGetSubFrames() {
        List<ClientMessage> fragments = getFragments(128, clientMessage);
        ClientMessage.ForwardFrameIterator originalIterator = clientMessage.frameIterator();
        assertEquals(19, fragments.size());

        assertFragments(fragments, originalIterator);
    }

    @Test
    @RequireAssertEnabled
    public void testGetSubFrame_whenFrameSizeGreaterThanFrameLength_thenReturnOriginalMessage() {
        List<ClientMessage> fragments = getFragments(4000, clientMessage);
        ClientMessage.ForwardFrameIterator originalIterator = clientMessage.frameIterator();

        assertFragments(fragments, originalIterator);
    }

    private void assertFragments(List<ClientMessage> fragments, ClientMessage.ForwardFrameIterator originalIterator) {
        for (ClientMessage fragment : fragments) {
            ClientMessage.ForwardFrameIterator iterator = fragment.frameIterator();
            //skip fragmentation header
            iterator.next();
            while (iterator.hasNext()) {
                ClientMessage.Frame actualFrame = iterator.next();
                ClientMessage.Frame expectedFrame = originalIterator.next();
                assertEquals(actualFrame.getSize(), expectedFrame.getSize());
                assertEquals(actualFrame.flags, expectedFrame.flags);
                Assert.assertArrayEquals(actualFrame.content, expectedFrame.content);
            }
        }
    }

}
