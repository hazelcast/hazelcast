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

package com.hazelcast.kubernetes;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.NoLogFactory;
import com.hazelcast.spi.discovery.DiscoveryNode;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(PowerMockRunner.class)
@PrepareForTest(DnsEndpointResolver.class)
public class DnsEndpointResolverTest {
    private static final ILogger LOGGER = new NoLogFactory().getLogger("no");

    private static final String SERVICE_DNS = "my-release-hazelcast.default.svc.cluster.local";
    private static final int DEFAULT_SERVICE_DNS_TIMEOUT_SECONDS = 5;
    private static final int TEST_DNS_TIMEOUT_SECONDS = 1;
    private static final int UNSET_PORT = 0;
    private static final int DEFAULT_PORT = 5701;
    private static final int CUSTOM_PORT = 5702;
    private static final String IP_SERVER_1 = "192.168.0.5";
    private static final String IP_SERVER_2 = "192.168.0.6";

    @Before
    public void setUp()
            throws Exception {
        PowerMockito.mockStatic(InetAddress.class);

        InetAddress address1 = mock(InetAddress.class);
        InetAddress address2 = mock(InetAddress.class);
        when(address1.getHostAddress()).thenReturn(IP_SERVER_1);
        when(address2.getHostAddress()).thenReturn(IP_SERVER_2);
        PowerMockito.when(InetAddress.getAllByName(SERVICE_DNS)).thenReturn(new InetAddress[]{address1, address2});
    }

    @Test
    public void resolve() {
        // given
        DnsEndpointResolver dnsEndpointResolver = new DnsEndpointResolver(LOGGER, SERVICE_DNS, UNSET_PORT, DEFAULT_SERVICE_DNS_TIMEOUT_SECONDS);

        // when
        List<DiscoveryNode> result = dnsEndpointResolver.resolve();

        // then

        Set<?> resultAddresses = setOf(result.get(0).getPrivateAddress().getHost(), result.get(1).getPrivateAddress().getHost());
        Set<?> resultPorts = setOf(result.get(0).getPrivateAddress().getPort(), result.get(1).getPrivateAddress().getPort());
        assertEquals(setOf(IP_SERVER_1, IP_SERVER_2), resultAddresses);
        assertEquals(setOf(DEFAULT_PORT), resultPorts);
    }

    @Test
    public void resolveCustomPort() {
        // given
        DnsEndpointResolver dnsEndpointResolver = new DnsEndpointResolver(LOGGER, SERVICE_DNS, CUSTOM_PORT, DEFAULT_SERVICE_DNS_TIMEOUT_SECONDS);

        // when
        List<DiscoveryNode> result = dnsEndpointResolver.resolve();

        // then

        Set<?> resultAddresses = setOf(result.get(0).getPrivateAddress().getHost(), result.get(1).getPrivateAddress().getHost());
        Set<?> resultPorts = setOf(result.get(0).getPrivateAddress().getPort(), result.get(1).getPrivateAddress().getPort());
        assertEquals(setOf(IP_SERVER_1, IP_SERVER_2), resultAddresses);
        assertEquals(setOf(CUSTOM_PORT), resultPorts);
    }

    @Test
    public void resolveException()
            throws Exception {
        // given
        ILogger logger = mock(ILogger.class);
        PowerMockito.when(InetAddress.getAllByName(SERVICE_DNS)).thenThrow(new UnknownHostException());
        DnsEndpointResolver dnsEndpointResolver = new DnsEndpointResolver(logger, SERVICE_DNS, UNSET_PORT, DEFAULT_SERVICE_DNS_TIMEOUT_SECONDS);

        // when
        List<DiscoveryNode> result = dnsEndpointResolver.resolve();

        // then
        assertEquals(0, result.size());
        verify(logger).warning(String.format("DNS lookup for serviceDns '%s' failed: unknown host", SERVICE_DNS));
        verify(logger, never()).warning(anyString(), any(Throwable.class));
    }

    @Test
    public void resolveNotFound()
            throws Exception {
        // given
        PowerMockito.when(InetAddress.getAllByName(SERVICE_DNS)).thenReturn(new InetAddress[0]);
        DnsEndpointResolver dnsEndpointResolver = new DnsEndpointResolver(LOGGER, SERVICE_DNS, UNSET_PORT, DEFAULT_SERVICE_DNS_TIMEOUT_SECONDS);

        // when
        List<DiscoveryNode> result = dnsEndpointResolver.resolve();

        // then
        assertEquals(0, result.size());
    }

    @Test
    public void resolveTimeout()
            throws Exception {
        // given
        ILogger logger = mock(ILogger.class);
        PowerMockito.when(InetAddress.getAllByName(SERVICE_DNS)).then(waitAndAnswer());
        DnsEndpointResolver dnsEndpointResolver = new DnsEndpointResolver(logger, SERVICE_DNS, UNSET_PORT, TEST_DNS_TIMEOUT_SECONDS);

        // when
        List<DiscoveryNode> result = dnsEndpointResolver.resolve();

        // then
        assertEquals(0, result.size());
        verify(logger).warning(String.format("DNS lookup for serviceDns '%s' failed: DNS resolution timeout", SERVICE_DNS));
        verify(logger, never()).warning(anyString(), any(Throwable.class));
    }

    private static Answer<InetAddress[]> waitAndAnswer() {
        return new Answer<InetAddress[]>() {
            @Override
            public InetAddress[] answer(InvocationOnMock invocation) throws Throwable {
                Thread.sleep(TEST_DNS_TIMEOUT_SECONDS * 5 * 1000);
                return new InetAddress[0];
            }
        };
    }

    private static Set<?> setOf(Object... objects) {
        Set<Object> result = new HashSet<Object>();
        for (Object object : objects) {
            result.add(object);
        }
        return result;
    }
}
