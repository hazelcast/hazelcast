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

package com.hazelcast.client.impl.statistics;

import com.hazelcast.client.impl.ClientEndpoint;
import com.hazelcast.client.impl.protocol.task.ClientStatisticsMessageTask;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * An immutable class representing the statistics sent by the clients
 * to the members.
 *
 * @see ClientStatisticsMessageTask
 * @see ClientEndpoint#setClientStatistics(ClientStatistics)
 * @see ClientEndpoint#getClientAttributes()
 */
public final class ClientStatistics {
    private final long timestamp;
    private final String clientAttributes;
    private final byte[] metricsBlob;

    @SuppressFBWarnings("EI_EXPOSE_REP2")
    public ClientStatistics(long timestamp, String clientAttributes, byte[] metricsBlob) {
        this.timestamp = timestamp;
        this.clientAttributes = clientAttributes;
        this.metricsBlob = metricsBlob;
    }

    public long timestamp() {
        return timestamp;
    }

    public String clientAttributes() {
        return clientAttributes;
    }

    @SuppressFBWarnings("EI_EXPOSE_REP")
    public byte[] metricsBlob() {
        return metricsBlob;
    }
}
