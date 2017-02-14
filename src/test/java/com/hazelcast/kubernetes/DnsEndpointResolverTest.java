package com.hazelcast.kubernetes;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.NoLogFactory;
import com.hazelcast.spi.discovery.DiscoveryNode;
import io.fabric8.kubernetes.api.model.EndpointAddress;
import io.fabric8.kubernetes.api.model.EndpointSubset;
import io.fabric8.kubernetes.api.model.Endpoints;
import io.fabric8.kubernetes.api.model.EndpointsList;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.dsl.ClientMixedOperation;
import org.junit.Before;
import org.junit.Ignore;
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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
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
    public void setup() throws Exception {
        PowerMockito.whenNew(DefaultKubernetesClient.class).withAnyArguments().thenReturn(client);
        PowerMockito.whenNew(SRVRecord.class).withAnyArguments().thenReturn(srvRecord);
        when(srvRecord.getTarget()).thenReturn(Name.fromString("127.0.0.1"));
    }

    @Test
    public void testInvalidServiceDns() {
        DnsEndpointResolver endpointResolver = new DnsEndpointResolver(LOGGER, "http://test", SERVICE_DNS_TIMEOUT);
        List<DiscoveryNode> nodes = endpointResolver.resolve();
        assertTrue(nodes.isEmpty());
    }

    @Test
    public void testValidServiceDns() throws Exception {
        DnsEndpointResolver endpointResolver = PowerMockito.spy(new DnsEndpointResolver(LOGGER, "hazelcast.com", SERVICE_DNS_TIMEOUT));
        PowerMockito.when(endpointResolver, MemberMatcher.method(DnsEndpointResolver.class, "buildLookup", null)).withNoArguments().thenReturn(lookup);
        when(lookup.getResult()).thenReturn(Lookup.SUCCESSFUL);
        when(lookup.run()).thenReturn(getRecords());
        List<DiscoveryNode> nodes = endpointResolver.resolve();
        assertEquals(1, nodes.size());
        assertEquals("127.0.0.1", nodes.get(0).getPrivateAddress().getHost());
    }

    private Record[] getRecords() {
        return new Record[] {srvRecord};
    }
}