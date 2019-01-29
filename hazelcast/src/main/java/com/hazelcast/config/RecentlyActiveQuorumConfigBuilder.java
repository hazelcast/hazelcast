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

package com.hazelcast.config;

import com.hazelcast.quorum.impl.RecentlyActiveQuorumFunction;

import static com.hazelcast.spi.properties.GroupProperty.MAX_NO_HEARTBEAT_SECONDS;
import static java.lang.Integer.parseInt;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Builder for a {@link QuorumConfig} configured with {@link com.hazelcast.quorum.impl.RecentlyActiveQuorumFunction} that
 * considers members present in the context of a quorum if the last received heartbeat is within a recent window of time.
 */
public class RecentlyActiveQuorumConfigBuilder extends QuorumConfigBuilder {

    /**
     * Default duration of time window during which a heartbeat must have been received from a member, for it to be considered
     * present for quorum (in milliseconds).
     */
    public static final int DEFAULT_HEARTBEAT_TOLERANCE_MILLIS =
            (int) SECONDS.toMillis(parseInt(MAX_NO_HEARTBEAT_SECONDS.getDefaultValue()));

    private final String name;
    private final int size;
    private final int heartbeatToleranceMillis;

    RecentlyActiveQuorumConfigBuilder(String name, int size, int heartbeatToleranceMillis) {
        this.name = name;
        this.size = size;
        this.heartbeatToleranceMillis = heartbeatToleranceMillis;
    }

    public QuorumConfig build() {
        RecentlyActiveQuorumFunction quorumFunction = new RecentlyActiveQuorumFunction(size, heartbeatToleranceMillis);
        QuorumConfig quorumConfig = new QuorumConfig(name, enabled, size);
        quorumConfig.setQuorumFunctionImplementation(quorumFunction);
        return quorumConfig;
    }
}
