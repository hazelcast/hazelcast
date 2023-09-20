/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import com.hazelcast.cluster.Address;
import com.hazelcast.logging.ILogger;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class AddressCheckerImplTest extends HazelcastTestSupport {

    private ILogger logger = mock(ILogger.class);

    @Test
    public void givenNoInterfaceIsConfigured_whenMessageArrives_thenTrust() throws UnknownHostException {
        AddressCheckerImpl joinMessageTrustChecker = new AddressCheckerImpl(emptySet(), logger);
        Address address = createAddress("127.0.0.1");
        assertTrue(joinMessageTrustChecker.isTrusted(address));
    }

    @Test
    public void givenInterfaceIsConfigured_whenMessageWithMatchingHost_thenTrust() throws UnknownHostException {
        AddressCheckerImpl joinMessageTrustChecker = new AddressCheckerImpl(singleton("127.0.0.1"), logger);
        Address address = createAddress("127.0.0.1");
        assertTrue(joinMessageTrustChecker.isTrusted(address));
    }

    @Test
    public void givenInterfaceIsConfigured_whenMessageWithNonMatchingHost_thenDoNotTrust() throws UnknownHostException {
        AddressCheckerImpl joinMessageTrustChecker = new AddressCheckerImpl(singleton("127.0.0.2"), logger);
        Address address = createAddress("127.0.0.1");
        assertFalse(joinMessageTrustChecker.isTrusted(address));
    }

    @Test
    public void givenInterfaceRangeIsConfigured_whenMessageWithMatchingHost_thenTrust() throws UnknownHostException {
        AddressCheckerImpl joinMessageTrustChecker = new AddressCheckerImpl(singleton("127.0.0.1-100"), logger);
        Address address = createAddress("127.0.0.2");
        assertTrue(joinMessageTrustChecker.isTrusted(address));
    }

    @Test
    public void givenInterfaceRangeIsConfigured_whenMessageWithNonMatchingHost_thenDoNotTrust() throws UnknownHostException {
        AddressCheckerImpl joinMessageTrustChecker = new AddressCheckerImpl(singleton("127.0.0.1-100"), logger);
        Address address = createAddress("127.0.0.101");
        assertFalse(joinMessageTrustChecker.isTrusted(address));
    }

    @Test
    public void testAsteriskWildcard() throws UnknownHostException {
        AddressCheckerImpl joinMessageTrustChecker = new AddressCheckerImpl(singleton("127.0.*.*"), logger);
        assertTrue(joinMessageTrustChecker.isTrusted(createAddress("127.0.1.1")));
        assertFalse(joinMessageTrustChecker.isTrusted(createAddress("127.1.1.1")));

        joinMessageTrustChecker = new AddressCheckerImpl(singleton("127.*.1.*"), logger);
        assertTrue(joinMessageTrustChecker.isTrusted(createAddress("127.0.1.1")));
        assertTrue(joinMessageTrustChecker.isTrusted(createAddress("127.127.1.127")));
        assertFalse(joinMessageTrustChecker.isTrusted(createAddress("127.127.127.127")));
    }

    @Test
    public void testIntervalRange() throws UnknownHostException {
        AddressCheckerImpl joinMessageTrustChecker = new AddressCheckerImpl(singleton("127.0.110-115.*"), logger);
        assertTrue(joinMessageTrustChecker.isTrusted(createAddress("127.0.110.1")));
        assertTrue(joinMessageTrustChecker.isTrusted(createAddress("127.0.112.1")));
        assertTrue(joinMessageTrustChecker.isTrusted(createAddress("127.0.115.255")));
        assertFalse(joinMessageTrustChecker.isTrusted(createAddress("127.0.116.255")));
        assertFalse(joinMessageTrustChecker.isTrusted(createAddress("127.0.1.1")));

        joinMessageTrustChecker = new AddressCheckerImpl(singleton("127.0.110-115.1-2"), logger);
        assertTrue(joinMessageTrustChecker.isTrusted(createAddress("127.0.110.2")));
        assertFalse(joinMessageTrustChecker.isTrusted(createAddress("127.0.110.3")));
        assertFalse(joinMessageTrustChecker.isTrusted(createAddress("127.0.109.2")));
    }

    private Address createAddress(String ip) throws UnknownHostException {
        InetAddress inetAddress = InetAddress.getByName(ip);
        return new Address(inetAddress, 5701);
    }
}
