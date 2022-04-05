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

package com.hazelcast.internal.metrics;

/**
 * With the probe level one can control which probes are being tracked by the
 * MetricsRegistry and which ones are not.
 */
public enum ProbeLevel {

    /**
     * Indicates that a probe is mandatory. E.g. memory usage, since other parts
     * of the system rely on it (e.g the health monitor)
     */
    MANDATORY(2),

    /**
     * Indicates that it is a regular probe.
     */
    INFO(1),

    /**
     * Indicates that is a probe for debugging purposes.
     */
    DEBUG(0);

    private final int precedence;

    ProbeLevel(int precedence) {
        this.precedence = precedence;
    }

    /**
     * Checks if the this ProbeLevel has a precedence equal or higher than the
     * minimumLevel.
     *
     * @param minimumLevel the minimum ProbeLevel
     * @return true if it is trackable, false otherwise.
     */
    public boolean isEnabled(ProbeLevel minimumLevel) {
        return precedence >= minimumLevel.precedence;
    }
}
