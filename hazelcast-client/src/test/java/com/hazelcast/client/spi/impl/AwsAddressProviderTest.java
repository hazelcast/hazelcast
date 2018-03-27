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

package com.hazelcast.client.spi.impl;

import com.hazelcast.aws.AWSClient;
import com.hazelcast.client.config.ClientAwsConfig;
import com.hazelcast.logging.LoggingService;
import com.hazelcast.nio.Address;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static com.hazelcast.logging.Logger.getLogger;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class AwsAddressProviderTest {

    private static final String PRIVATE_IP_ADDRESS = "127.0.0.1";
    private static final String PUBLIC_IP_ADDRESS = "198.51.100.1";

    private AWSClient awsClient;
    private LoggingService loggingService;

    private AwsAddressProvider provider;

    @Before
    public void setUp() throws Exception {
        Map<String, String> addresses = new HashMap<String, String>();
        addresses.put(PRIVATE_IP_ADDRESS, PUBLIC_IP_ADDRESS);

        awsClient = mock(AWSClient.class);
        when(awsClient.getAddresses()).thenReturn(addresses);

        loggingService = mock(LoggingService.class);
        when(loggingService.getLogger(eq(AwsAddressProvider.class))).thenReturn(getLogger(AwsAddressProviderTest.class));

        provider = new AwsAddressProvider(awsClient, loggingService);
    }

    @Test
    public void testLoadAddresses() {
        Collection<Address> addresses = provider.loadAddresses();

        assertTrue("Expected that at least one address is loaded", addresses.size() > 0);
        for (Address address : addresses) {
            assertEquals(PRIVATE_IP_ADDRESS, address.getHost());
        }
    }

    @Test
    public void testLoadAddresses_whenExceptionIsThrown() throws Exception {
        when(awsClient.getAddresses()).thenThrow(new IllegalStateException("Expected exception"));

        Collection<Address> addresses = provider.loadAddresses();

        assertEquals("Expected that no addresses are loaded", 0, addresses.size());
    }

    @Test
    public void testLoadAddresses_whenConfigIsIncomplete() {
        ClientAwsConfig config = new ClientAwsConfig();
        config.setEnabled(false);
        provider = new AwsAddressProvider(config, loggingService);

        Collection<Address> addresses = provider.loadAddresses();

        assertEquals("Expected that no addresses are loaded", 0, addresses.size());
    }
}
