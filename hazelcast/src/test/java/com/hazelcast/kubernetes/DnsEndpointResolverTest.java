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

package com.hazelcast.kubernetes;

import com.hazelcast.kubernetes.DnsEndpointResolver.RawLookupProvider;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.NoLogFactory;
import com.hazelcast.spi.discovery.DiscoveryNode;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
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

    @Test
    public void resolve() {
        // given
        RawLookupProvider lookupProvider = staticLookupProvider(SERVICE_DNS, IP_SERVER_1, IP_SERVER_2);
        DnsEndpointResolver dnsEndpointResolver = new DnsEndpointResolver(LOGGER, SERVICE_DNS, UNSET_PORT, DEFAULT_SERVICE_DNS_TIMEOUT_SECONDS, lookupProvider);

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
        RawLookupProvider lookupProvider = staticLookupProvider(SERVICE_DNS, IP_SERVER_1, IP_SERVER_2);
        DnsEndpointResolver dnsEndpointResolver = new DnsEndpointResolver(LOGGER, SERVICE_DNS, CUSTOM_PORT, DEFAULT_SERVICE_DNS_TIMEOUT_SECONDS, lookupProvider);

        // when
        List<DiscoveryNode> result = dnsEndpointResolver.resolve();

        // then

        Set<?> resultAddresses = setOf(result.get(0).getPrivateAddress().getHost(), result.get(1).getPrivateAddress().getHost());
        Set<?> resultPorts = setOf(result.get(0).getPrivateAddress().getPort(), result.get(1).getPrivateAddress().getPort());
        assertEquals(setOf(IP_SERVER_1, IP_SERVER_2), resultAddresses);
        assertEquals(setOf(CUSTOM_PORT), resultPorts);
    }

    @Test
    public void resolveException() throws Exception {
        // given
        ILogger logger = mock(ILogger.class);
        RawLookupProvider lookupProvider = nonResolvingLookupProvider();
        DnsEndpointResolver dnsEndpointResolver = new DnsEndpointResolver(logger, SERVICE_DNS, UNSET_PORT, DEFAULT_SERVICE_DNS_TIMEOUT_SECONDS, nonResolvingLookupProvider());

        // when
        List<DiscoveryNode> result = dnsEndpointResolver.resolve();

        // then
        assertEquals(0, result.size());
        verify(logger).warning(String.format("DNS lookup for serviceDns '%s' failed: unknown host", SERVICE_DNS));
        verify(logger, never()).warning(anyString(), any(Throwable.class));
    }

    @Test
    public void resolveNotFound() throws Exception {
        // given
        RawLookupProvider lookupProvider = staticLookupProvider(SERVICE_DNS);
        DnsEndpointResolver dnsEndpointResolver = new DnsEndpointResolver(LOGGER, SERVICE_DNS, UNSET_PORT, DEFAULT_SERVICE_DNS_TIMEOUT_SECONDS, lookupProvider);

        // when
        List<DiscoveryNode> result = dnsEndpointResolver.resolve();

        // then
        assertEquals(0, result.size());
    }

    @Test
    public void resolveTimeout() {
        // given
        ILogger logger = mock(ILogger.class);
        DnsEndpointResolver dnsEndpointResolver = new DnsEndpointResolver(logger, SERVICE_DNS, UNSET_PORT, TEST_DNS_TIMEOUT_SECONDS, timingOutLookupProvider());

        // when
        List<DiscoveryNode> result = dnsEndpointResolver.resolve();

        // then
        assertEquals(0, result.size());
        verify(logger).warning(String.format("DNS lookup for serviceDns '%s' failed: DNS resolution timeout", SERVICE_DNS));
        verify(logger, never()).warning(anyString(), any(Throwable.class));
    }

    private static Set<?> setOf(Object... objects) {
        return new HashSet<>(Arrays.asList(objects));
    }

    private static RawLookupProvider staticLookupProvider(String host, String... ipAddresses) {
        Map<String, InetAddress[]> entries = new HashMap<>();
        InetAddress[] inetAddresses = Arrays.stream(ipAddresses).map(ip -> inetAddress(host, ip)).toArray(InetAddress[]::new);
        entries.put(host, inetAddresses);
        return entries::get;
    }

    private static InetAddress inetAddress(String host, String ipAddress) {
        try {
            return InetAddress.getByAddress(host, InetAddress.getByName(ipAddress).getAddress());
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
    }

    private static RawLookupProvider timingOutLookupProvider() {
        return host -> {
            try {
                Thread.sleep(TEST_DNS_TIMEOUT_SECONDS * 2 * 1000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
            return new InetAddress[0];
        };
    }

    private static RawLookupProvider nonResolvingLookupProvider() {
        return host -> {
            throw new UnknownHostException();
        };
    }
}
