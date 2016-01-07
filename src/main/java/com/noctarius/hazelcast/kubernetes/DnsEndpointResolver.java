/*
 * Copyright (c) 2015, Christoph Engelbert (aka noctarius) and
 * contributors. All rights reserved.
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
package com.noctarius.hazelcast.kubernetes;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.discovery.DiscoveryNode;
import com.hazelcast.spi.discovery.SimpleDiscoveryNode;
import org.xbill.DNS.Lookup;
import org.xbill.DNS.Record;
import org.xbill.DNS.SRVRecord;
import org.xbill.DNS.TextParseException;
import org.xbill.DNS.Type;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

final class DnsEndpointResolver extends HazelcastKubernetesDiscoveryStrategy.EndpointResolver {

    private static final ILogger LOGGER = Logger.getLogger(DnsEndpointResolver.class);

    private final String serviceDns;

    public DnsEndpointResolver(ILogger logger, String serviceDns) {
        super(logger);
        this.serviceDns = serviceDns;
    }

    List<DiscoveryNode> resolve() {
        try {
            Lookup lookup = new Lookup(serviceDns, Type.SRV);
            Record[] records = lookup.run();

            if (lookup.getResult() != Lookup.SUCCESSFUL) {
                LOGGER.warning("DNS lookup for serviceDns '" + serviceDns + "' failed");
                return Collections.emptyList();
            }

            List<DiscoveryNode> discoveredNodes = new ArrayList<DiscoveryNode>();
            for (Record record : records) {
                // Should be a safe cast as we've looked up only SRV records.
                SRVRecord srv = (SRVRecord) record;
                InetAddress inetAddress = getAddress(srv);

                if (inetAddress == null) {
                    continue;
                }

                int port = srv.getPort();

                Address address = new Address(inetAddress, port);
                discoveredNodes.add(new SimpleDiscoveryNode(address));
            }

            return discoveredNodes;
        } catch (TextParseException e) {
            throw new RuntimeException("Could not resolve services via DNS", e);
        }
    }

    private InetAddress getAddress(SRVRecord srv) {
        try {
            return org.xbill.DNS.Address.getByName(srv.getTarget().canonicalize().toString(true));
        } catch (UnknownHostException e) {
            return null;
        }
    }
}
