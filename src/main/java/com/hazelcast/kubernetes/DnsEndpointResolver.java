/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.config.NetworkConfig;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.logging.ILogger;
import com.hazelcast.cluster.Address;
import com.hazelcast.spi.discovery.DiscoveryNode;
import com.hazelcast.spi.discovery.SimpleDiscoveryNode;

import javax.naming.Context;
import javax.naming.NameNotFoundException;
import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.directory.Attribute;
import javax.naming.directory.Attributes;
import javax.naming.directory.DirContext;
import javax.naming.directory.InitialDirContext;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.List;
import java.util.Set;

final class DnsEndpointResolver
        extends HazelcastKubernetesDiscoveryStrategy.EndpointResolver {

    private final String serviceDns;
    private final int port;
    private final DirContext dirContext;

    DnsEndpointResolver(ILogger logger, String serviceDns, int port, DirContext dirContext) {
        super(logger);
        this.serviceDns = serviceDns;
        this.port = port;
        this.dirContext = dirContext;
    }

    DnsEndpointResolver(ILogger logger, String serviceDns, int port, int serviceDnsTimeout) {
        this(logger, serviceDns, port, createDirContext(serviceDnsTimeout));

    }

    @SuppressWarnings("checkstyle:magicnumber")
    private static DirContext createDirContext(int serviceDnsTimeout) {
        Hashtable<String, String> env = new Hashtable<String, String>();
        env.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.dns.DnsContextFactory");
        env.put(Context.PROVIDER_URL, "dns:");
        env.put("com.sun.jndi.dns.timeout.initial", String.valueOf(serviceDnsTimeout * 1000L));
        try {
            return new InitialDirContext(env);
        } catch (NamingException e) {
            throw new HazelcastException("Error while initializing DirContext", e);
        }
    }

    List<DiscoveryNode> resolve() {
        try {
            return lookup();
        } catch (NameNotFoundException e) {
            logger.warning(String.format("DNS lookup for serviceDns '%s' failed: name not found", serviceDns));
            return Collections.emptyList();
        } catch (Exception e) {
            logger.warning(String.format("DNS lookup for serviceDns '%s' failed", serviceDns), e);
            return Collections.emptyList();
        }
    }

    private List<DiscoveryNode> lookup()
            throws NamingException, UnknownHostException {
        Set<String> addresses = new HashSet<String>();
        Attributes attributes = dirContext.getAttributes(serviceDns, new String[]{"SRV"});
        Attribute srvAttribute = attributes.get("srv");
        if (srvAttribute != null) {
            NamingEnumeration<?> servers = srvAttribute.getAll();
            while (servers.hasMore()) {
                String server = (String) servers.next();
                String serverHost = extractHost(server);
                InetAddress address = InetAddress.getByName(serverHost);
                if (addresses.add(address.getHostAddress()) && logger.isFinestEnabled()) {
                    logger.finest("Found node service with address: " + address);
                }
            }
        }

        if (addresses.size() == 0) {
            logger.warning("Could not find any service for serviceDns '" + serviceDns + "'");
            return Collections.emptyList();
        }

        List<DiscoveryNode> result = new ArrayList<DiscoveryNode>();
        for (String address : addresses) {
            result.add(new SimpleDiscoveryNode(new Address(address, getHazelcastPort(port))));
        }
        return result;
    }

    /**
     * Extracts host from the DNS record.
     * <p>
     * Sample record: "10 25 0 6235386366386436.my-release-hazelcast.default.svc.cluster.local".
     */
    private static String extractHost(String server) {
        String host = server.split(" ")[3];
        return host.replaceAll("\\\\.$", "");
    }

    private static int getHazelcastPort(int port) {
        if (port > 0) {
            return port;
        }
        return NetworkConfig.DEFAULT_PORT;
    }
}
