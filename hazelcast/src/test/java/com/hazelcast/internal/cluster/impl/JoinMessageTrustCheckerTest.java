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

package com.hazelcast.internal.cluster.impl;

import com.hazelcast.logging.ILogger;
import com.hazelcast.cluster.Address;
import com.hazelcast.internal.nio.Packet;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.version.MemberVersion;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.UUID;

import static java.util.Collections.EMPTY_SET;
import static java.util.Collections.singleton;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class JoinMessageTrustCheckerTest extends HazelcastTestSupport {

    private ILogger logger = mock(ILogger.class);

    @Test
    public void givenNoInterfaceIsConfigured_whenMessageArrives_thenTrust() throws UnknownHostException {
        JoinMessageTrustChecker joinMessageTrustChecker = new JoinMessageTrustChecker(EMPTY_SET, logger);
        JoinMessage message = createJoinMessage("127.0.0.1");
        assertTrue(joinMessageTrustChecker.isTrusted(message));
    }

    @Test
    public void givenInterfaceIsConfigured_whenMessageWithMatchingHost_thenTrust() throws UnknownHostException {
        JoinMessageTrustChecker joinMessageTrustChecker = new JoinMessageTrustChecker(singleton("127.0.0.1"), logger);
        JoinMessage message = createJoinMessage("127.0.0.1");
        assertTrue(joinMessageTrustChecker.isTrusted(message));
    }

    @Test
    public void givenInterfaceIsConfigured_whenMessageWithNonMatchingHost_thenDoNotTrust() throws UnknownHostException {
        JoinMessageTrustChecker joinMessageTrustChecker = new JoinMessageTrustChecker(singleton("127.0.0.2"), logger);
        JoinMessage message = createJoinMessage("127.0.0.1");
        assertFalse(joinMessageTrustChecker.isTrusted(message));
    }

    @Test
    public void givenInterfaceRangeIsConfigured_whenMessageWithMatchingHost_thenTrust() throws UnknownHostException {
        JoinMessageTrustChecker joinMessageTrustChecker = new JoinMessageTrustChecker(singleton("127.0.0.1-100"), logger);
        JoinMessage message = createJoinMessage("127.0.0.2");
        assertTrue(joinMessageTrustChecker.isTrusted(message));
    }

    @Test
    public void givenInterfaceRangeIsConfigured_whenMessageWithNonMatchingHost_thenDoNotTrust() throws UnknownHostException {
        JoinMessageTrustChecker joinMessageTrustChecker = new JoinMessageTrustChecker(singleton("127.0.0.1-100"), logger);
        JoinMessage message = createJoinMessage("127.0.0.101");
        assertFalse(joinMessageTrustChecker.isTrusted(message));
    }

    private JoinMessage createJoinMessage(String ip) throws UnknownHostException {
        InetAddress inetAddress = InetAddress.getByName(ip);
        Address address = new Address(inetAddress, 5701);
        ConfigCheck configCheck = new ConfigCheck();
        return new JoinMessage(Packet.VERSION, 0, MemberVersion.UNKNOWN, address, UUID.randomUUID(), false, configCheck);
    }
}
