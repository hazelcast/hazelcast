/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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
import java.util.ListIterator;

import static com.hazelcast.client.impl.protocol.util.ClientMessageSplitter.getFragments;
import static groovy.util.GroovyTestCase.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientMessageSplitterTest extends HazelcastTestSupport {

    private ClientMessage clientMessage;

    @Before
    public void setUp() throws Exception {
        String username = generateRandomString(1000);
        String password = generateRandomString(1000);
        String uuid = generateRandomString(1000);
        String ownerUuid = generateRandomString(1000);
        boolean isOwnerConnection = false;
        String clientType = generateRandomString(1000);
        String clientSerializationVersion = generateRandomString(1000);
        String clientName = generateRandomString(1000);
        String clusterId = generateRandomString(1000);
        LinkedList<String> labels = new LinkedList<>();
        for (int i = 0; i < 10; i++) {
            labels.add(generateRandomString(1000));
        }

        clientMessage = ClientAuthenticationCodec.encodeRequest(username, password, uuid, ownerUuid, isOwnerConnection,
                clientType, (byte) 1, clientSerializationVersion, clientName, labels, 1, clusterId);
    }

    @Test
    public void testConstructor() {
        assertUtilityConstructor(ClientMessageSplitter.class);
    }

    @Test
    public void testGetSubFrames() {
        List<ClientMessage> fragments = getFragments(128, clientMessage);
        ListIterator<ClientMessage.Frame> originalIterator = clientMessage.listIterator();
        assertEquals(21, fragments.size());

        assertFragments(fragments, originalIterator);
    }

    @Test
    @RequireAssertEnabled
    public void testGetSubFrame_whenFrameSizeGreaterThanFrameLength_thenReturnOriginalMessage() {
        List<ClientMessage> fragments = getFragments(4000, clientMessage);
        ListIterator<ClientMessage.Frame> originalIterator = clientMessage.listIterator();

        assertFragments(fragments, originalIterator);
    }

    private void assertFragments(List<ClientMessage> fragments, ListIterator<ClientMessage.Frame> originalIterator) {
        for (ClientMessage fragment : fragments) {
            ListIterator<ClientMessage.Frame> iterator = fragment.listIterator();
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
