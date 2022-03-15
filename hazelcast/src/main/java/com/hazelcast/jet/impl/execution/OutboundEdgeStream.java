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

package com.hazelcast.jet.impl.execution;

/**
 * The outbound side of a data stream corresponding to the origin of a single DAG edge identified by its ordinal.
 * A thin wrapper around an {@link OutboundCollector}
 */
public class OutboundEdgeStream {

    private final int ordinal;
    private final OutboundCollector collector;

    public OutboundEdgeStream(int ordinal, OutboundCollector collector) {
        this.ordinal = ordinal;
        this.collector = collector;
    }

    int ordinal() {
        return ordinal;
    }

    OutboundCollector getCollector() {
        return collector;
    }

    @Override
    public String toString() {
        return "OutboundEdgeStream(ordinal=" + ordinal + ')';
    }
}
