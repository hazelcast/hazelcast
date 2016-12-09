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

import com.hazelcast.config.NetworkConfig;
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
import java.util.*;

final class DnsEndpointResolver
        extends HazelcastKubernetesDiscoveryStrategy.EndpointResolver {

    private static final ILogger LOGGER = Logger.getLogger(DnsEndpointResolver.class);

    private final String serviceDns;

    public DnsEndpointResolver(ILogger logger, String serviceDns) {
        super(logger);
        this.serviceDns = serviceDns;
    }

    List<DiscoveryNode> resolve() {
        try {
            Lookup lookup = new Lookup(serviceDns, Type.SRV);
            lookup.setCache(null); // Avoid caching temporary DNS lookup failures indefinitely in global cache
            Record[] records = lookup.run();

            if (lookup.getResult() != Lookup.SUCCESSFUL) {
                LOGGER.warning("DNS lookup for serviceDns '" + serviceDns + "' failed");
                return Collections.emptyList();
            }

            Set<Address> discoveredAddresses = new HashSet<Address>();

            for (Record record : records) {
                // nslookup u219692-hazelcast.u219692-hazelcast.svc.cluster.local 172.30.0.1
                //      Server:         172.30.0.1
                //      Address:        172.30.0.1#53
                //
                //      Name:   u219692-hazelcast.u219692-hazelcast.svc.cluster.local
                //      Address: 10.1.2.8
                //      Name:   u219692-hazelcast.u219692-hazelcast.svc.cluster.local
                //      Address: 10.1.5.28
                //      Name:   u219692-hazelcast.u219692-hazelcast.svc.cluster.local
                //      Address: 10.1.9.33
                SRVRecord srv = (SRVRecord) record;
                InetAddress[] inetAddress = getAllAddresses(srv);
                int port = getHazelcastPort(srv.getPort());

                for (InetAddress i : inetAddress) {
                    Address address = new Address(i, port);

                    if (LOGGER.isFinestEnabled()) {
                        LOGGER.finest("Found node ip-address is: " + address);
                    }
                    discoveredAddresses.add(address);
                }

            }
            if (discoveredAddresses.isEmpty()) {
                LOGGER.warning("Could not find any service for serviceDns '" + serviceDns + "' failed");
                return Collections.EMPTY_LIST;
            }

            List<DiscoveryNode> discoveredNodes = new ArrayList<DiscoveryNode>(discoveredAddresses.size());

            for (Address address : discoveredAddresses) {
                discoveredNodes.add(new SimpleDiscoveryNode(address));
            }

            return discoveredNodes;

        } catch (TextParseException e) {
            throw new RuntimeException("Could not resolve services via DNS", e);
        } catch (UnknownHostException e) {
            throw new RuntimeException("Could not resolve services via DNS", e);
        }
    }

    private int getHazelcastPort(int port) {
        if (port > 0) {
            return port;
        }
        return NetworkConfig.DEFAULT_PORT;
    }

    private InetAddress[] getAllAddresses(SRVRecord srv)
            throws UnknownHostException {

        try {
            return org.xbill.DNS.Address.getAllByName(srv.getTarget().canonicalize().toString(true));
        } catch (UnknownHostException e) {
            LOGGER.severe("Parsing DNS records failed", e);
            throw e;
        }
    }
}
