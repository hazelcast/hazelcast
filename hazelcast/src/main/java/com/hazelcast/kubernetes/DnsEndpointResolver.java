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

import com.hazelcast.cluster.Address;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.discovery.DiscoveryNode;
import com.hazelcast.spi.discovery.SimpleDiscoveryNode;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

final class DnsEndpointResolver
        extends HazelcastKubernetesDiscoveryStrategy.EndpointResolver {
    // executor service for dns lookup calls
    private static final ExecutorService DNS_LOOKUP_SERVICE = Executors.newCachedThreadPool();

    private final String serviceDns;
    private final int port;
    private final int serviceDnsTimeout;
    private final RawLookupProvider rawLookupProvider;

    DnsEndpointResolver(ILogger logger, String serviceDns, int port, int serviceDnsTimeout) {
        this(logger, serviceDns, port, serviceDnsTimeout, InetAddress::getAllByName);
    }

    /**
     * Used externally only for testing
     */
    DnsEndpointResolver(ILogger logger, String serviceDns, int port, int serviceDnsTimeout, RawLookupProvider rawLookupProvider) {
        super(logger);
        this.serviceDns = serviceDns;
        this.port = port;
        this.serviceDnsTimeout = serviceDnsTimeout;
        this.rawLookupProvider = rawLookupProvider;
    }

    List<DiscoveryNode> resolve() {
        try {
            return lookup();
        } catch (TimeoutException e) {
            logger.warning(String.format("DNS lookup for serviceDns '%s' failed: DNS resolution timeout", serviceDns));
            return Collections.emptyList();
        } catch (UnknownHostException e) {
            logger.warning(String.format("DNS lookup for serviceDns '%s' failed: unknown host", serviceDns));
            return Collections.emptyList();
        } catch (Exception e) {
            logger.warning(String.format("DNS lookup for serviceDns '%s' failed", serviceDns), e);
            return Collections.emptyList();
        }
    }

    private List<DiscoveryNode> lookup()
            throws UnknownHostException, InterruptedException, ExecutionException, TimeoutException {
        Set<String> addresses = new HashSet<>();

        Future<InetAddress[]> future = DNS_LOOKUP_SERVICE.submit(() -> rawLookupProvider.getAddresses(serviceDns));

        try {
            for (InetAddress address : future.get(serviceDnsTimeout, TimeUnit.SECONDS)) {
                if (addresses.add(address.getHostAddress()) && logger.isFinestEnabled()) {
                    logger.finest("Found node service with address: " + address);
                }
            }
        } catch (ExecutionException e) {
            if (e.getCause() instanceof UnknownHostException) {
                throw (UnknownHostException) e.getCause();
            } else {
                throw e;
            }
        } catch (TimeoutException e) {
            // cancel DNS lookup
            future.cancel(true);
            throw e;
        }

        if (addresses.isEmpty()) {
            logger.warning("Could not find any service for serviceDns '" + serviceDns + "'");
            return Collections.emptyList();
        }

        List<DiscoveryNode> result = new ArrayList<DiscoveryNode>();
        for (String address : addresses) {
            result.add(new SimpleDiscoveryNode(new Address(address, getHazelcastPort(port))));
        }
        return result;
    }

    @FunctionalInterface
    interface RawLookupProvider {
        InetAddress[] getAddresses(String host) throws UnknownHostException;
    }

    private static int getHazelcastPort(int port) {
        if (port > 0) {
            return port;
        }
        return NetworkConfig.DEFAULT_PORT;
    }
}
