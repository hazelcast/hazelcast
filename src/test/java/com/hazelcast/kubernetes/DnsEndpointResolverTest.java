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

package com.hazelcast.kubernetes;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.NoLogFactory;
import com.hazelcast.spi.discovery.DiscoveryNode;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.api.support.membermodification.MemberMatcher;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.xbill.DNS.Lookup;
import org.xbill.DNS.Name;
import org.xbill.DNS.Record;
import org.xbill.DNS.SRVRecord;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(PowerMockRunner.class)
@PrepareForTest({DnsEndpointResolver.class, Lookup.class, SRVRecord.class})
public class DnsEndpointResolverTest {

    private static final ILogger LOGGER = new NoLogFactory().getLogger("no");
    private static final int SERVICE_DNS_TIMEOUT = 5;

    @Mock
    private DefaultKubernetesClient client;

    @Mock
    private Lookup lookup;

    @Mock
    private SRVRecord srvRecord;

    @Before
    public void setup()
            throws Exception {
        PowerMockito.whenNew(DefaultKubernetesClient.class).withAnyArguments().thenReturn(client);
        PowerMockito.whenNew(SRVRecord.class).withAnyArguments().thenReturn(srvRecord);
        when(srvRecord.getTarget()).thenReturn(Name.fromString("127.0.0.1"));
    }

    @Test
    public void testInvalidServiceDns() {
        DnsEndpointResolver endpointResolver = new DnsEndpointResolver(LOGGER, "http://test", 0, SERVICE_DNS_TIMEOUT);
        List<DiscoveryNode> nodes = endpointResolver.resolve();
        assertTrue(nodes.isEmpty());
    }

    @Test
    public void testValidServiceDns()
            throws Exception {
        testValidServiceDns(0, 5701);
    }

    @Test
    public void testValidServiceDnsWithCustomPort()
            throws Exception {
        testValidServiceDns(333, 333);
    }

    private void testValidServiceDns(final int port, final int expectedPort)
            throws Exception {
        DnsEndpointResolver endpointResolver = PowerMockito
                .spy(new DnsEndpointResolver(LOGGER, "hazelcast.com", port, SERVICE_DNS_TIMEOUT));
        PowerMockito.when(endpointResolver, MemberMatcher.method(DnsEndpointResolver.class, "buildLookup")).withNoArguments()
                    .thenReturn(lookup);
        when(lookup.getResult()).thenReturn(Lookup.SUCCESSFUL);
        when(lookup.run()).thenReturn(getRecords());
        List<DiscoveryNode> nodes = endpointResolver.resolve();
        assertEquals(1, nodes.size());
        assertEquals("127.0.0.1", nodes.get(0).getPrivateAddress().getHost());
        assertEquals(expectedPort, nodes.get(0).getPrivateAddress().getPort());
    }

    @Test
    public void testDnsFailFlow()
            throws Exception {
        DnsEndpointResolver endpointResolver = PowerMockito
                .spy(new DnsEndpointResolver(LOGGER, "hazelcast.com", 0, SERVICE_DNS_TIMEOUT));
        PowerMockito.when(endpointResolver, MemberMatcher.method(DnsEndpointResolver.class, "buildLookup")).withNoArguments()
                    .thenReturn(lookup);

        when(lookup.getResult()).thenReturn(Lookup.HOST_NOT_FOUND);
        when(lookup.run()).thenReturn(getRecords());

        List<DiscoveryNode> nodes = endpointResolver.resolve();
        assertTrue(nodes.isEmpty());

        verify(lookup).run();
        verify(lookup).getResult();
        verify(lookup).getErrorString();
    }

    private Record[] getRecords() {
        return new Record[]{srvRecord};
    }
}