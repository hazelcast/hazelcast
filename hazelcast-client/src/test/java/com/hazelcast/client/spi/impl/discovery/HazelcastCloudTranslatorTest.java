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

package com.hazelcast.client.spi.impl.discovery;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.logging.LoggingService;
import com.hazelcast.nio.Address;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class HazelcastCloudTranslatorTest {

    private final Map<Address, Address> lookup = new HashMap<Address, Address>();

    private Address privateAddress;
    private Address publicAddress;
    private LoggingService loggingService;
    private HazelcastCloudAddressTranslator translator;

    @Before
    public void setUp() throws Exception {
        privateAddress = new Address("127.0.0.1", 5701);
        publicAddress = new Address("192.168.0.1", 5701);
        lookup.put(privateAddress, publicAddress);
        lookup.put(new Address("127.0.0.2", 5701), new Address("192.168.0.2", 5701));

        HazelcastCloudDiscovery cloudDiscovery = mock(HazelcastCloudDiscovery.class);
        when(cloudDiscovery.discoverNodes()).thenReturn(lookup);

        ILogger logger = Logger.getLogger(HazelcastCloudTranslatorTest.class);

        loggingService = mock(LoggingService.class);
        when(loggingService.getLogger(eq(HazelcastCloudAddressTranslator.class))).thenReturn(logger);

        translator = new HazelcastCloudAddressTranslator(cloudDiscovery, loggingService);
    }


    @Test
    public void testTranslate_whenAddressIsNull_thenReturnNull() {
        Address actual = translator.translate(null);

        assertNull(actual);
    }

    @Test
    public void testTranslate() {
        Address actual = translator.translate(privateAddress);

        assertEquals(publicAddress.getHost(), actual.getHost());
        assertEquals(privateAddress.getPort(), actual.getPort());
    }

    @Test
    public void testRefreshAndTranslate() {
        translator.refresh();
        Address actual = translator.translate(privateAddress);

        assertEquals(publicAddress.getHost(), actual.getHost());
        assertEquals(privateAddress.getPort(), actual.getPort());
    }

    @Test
    public void testTranslate_whenNotFound_thenReturnNull() throws UnknownHostException {
        Address notAvailableAddress = new Address("127.0.0.3", 5701);
        Address actual = translator.translate(notAvailableAddress);

        assertNull(actual);
    }

    @Test
    public void testRefresh_whenException_thenLogWarning() {
        HazelcastCloudDiscovery cloudDiscovery = mock(HazelcastCloudDiscovery.class);
        when(cloudDiscovery.discoverNodes()).thenReturn(lookup);

        translator = new HazelcastCloudAddressTranslator(cloudDiscovery, loggingService);

        translator.refresh();
    }
}
