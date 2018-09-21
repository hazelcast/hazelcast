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

package com.hazelcast.client.impl;

import static com.hazelcast.internal.probing.CharSequenceUtils.startsWith;

import java.util.Collection;

import com.hazelcast.internal.metrics.ProbeLevel;
import com.hazelcast.internal.probing.ProbeSource;
import com.hazelcast.internal.probing.ProbingCycle;
import com.hazelcast.internal.probing.CharSequenceUtils.Lines;
import com.hazelcast.internal.probing.ProbingCycle.Tags;

/**
 * {@link ProbeSource} for {@link ClientEngineImpl} that got extracted into a
 * class on its own to not clutter {@link ClientEngineImpl} too much with
 * probing concerns.
 *
 * That means this {@link ProbeSource} is created and registered by the
 * {@link ClientEngineImpl}.
 */
public final class ClientEngineProbeSource implements ProbeSource {

    private final ClientEndpointManagerImpl endpointManager;

    ClientEngineProbeSource(ClientEndpointManagerImpl endpointManager) {
        this.endpointManager = endpointManager;
    }

    @Override
    public void probeNow(ProbingCycle cycle) {
        cycle.probe("client.endpoint", endpointManager);
        if (!cycle.isProbed(ProbeLevel.INFO)) {
            return;
        }
        Collection<ClientEndpoint> endpoints = endpointManager.getEndpoints();
        if (!endpoints.isEmpty()) {
            for (ClientEndpoint endpoint : endpoints) {
                Tags tags = cycle.openContext().tag(TAG_TYPE, "client");
                if (endpoint.isAlive()) {
                    tags.tag(TAG_INSTANCE, endpoint.getUuid());
                    cycle.probe(endpoint);
                    String version = endpoint.getClientVersionString();
                    String address = endpoint.getAddress();
                    tags.tag(TAG_TARGET, endpoint.getClientType().name())
                    .tag("version", version == null ? "?" : version)
                    .tag("address", address == null ? "?" : address);
                    // this particular metric is used to convey details of the endpoint via tags
                    boolean isOwnerConnection = endpoint.isOwnerConnection();
                    cycle.probe("ownerConnection", isOwnerConnection);
                    if (isOwnerConnection) {
                        probeForwarded(cycle, endpoint.getUuid(), endpoint.getClientStatistics());
                    }
                }
            }
        }
    }

    public static void probeForwarded(ProbingCycle cycle, CharSequence origin, CharSequence stats) {
        if (stats == null) {
            return;
        }
        // protocol version 1 (since 3.12)
        if (startsWith("1\n", stats)) {
            Lines lines = new Lines(stats);
            lines.next();
            cycle.openContext().tag("origin", origin)
            .tag(TAG_TYPE, lines.next())
            .tag(TAG_INSTANCE, lines.next())
            .tag(TAG_TARGET, lines.next())
            .tag("version", lines.next());
            // this additional metric is used to convey client details via tags
            cycle.probe("principal", "?".contentEquals(lines.next()));
            cycle.openContext().tag("origin", origin);
            lines.next();
            while (lines.length() > 0) {
                cycle.probeForwarded(lines.key(), lines.value());
                // first to end of current line as key goes back
                lines.next().next();
            }
        }
    }

}
