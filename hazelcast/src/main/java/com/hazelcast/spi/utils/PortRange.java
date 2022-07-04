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

package com.hazelcast.spi.utils;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Represents the range of IPv4 Ports.
 */
public final class PortRange {
    private static final Pattern PORT_NUMBER_REGEX = Pattern.compile("^(\\d+)$");
    private static final Pattern PORT_RANGE_REGEX = Pattern.compile("^(\\d+)-(\\d+)$");

    private static final int MIN_PORT = 0;
    private static final int MAX_PORT = 65535;

    private final int fromPort;
    private final int toPort;

    /**
     * Creates {@link PortRange} from the {@code spec} String.
     *
     * @param spec port number (e.g "5701") or port range (e.g. "5701-5708")
     * @throws IllegalArgumentException if the specified spec is not a valid port or port range
     */
    public PortRange(String spec) {
        Matcher portNumberMatcher = PORT_NUMBER_REGEX.matcher(spec);
        Matcher portRangeMatcher = PORT_RANGE_REGEX.matcher(spec);
        if (portNumberMatcher.find()) {
            int port = Integer.parseInt(spec);
            this.fromPort = port;
            this.toPort = port;
        } else if (portRangeMatcher.find()) {
            this.fromPort = Integer.parseInt(portRangeMatcher.group(1));
            this.toPort = Integer.parseInt(portRangeMatcher.group(2));
        } else {
            throw new IllegalArgumentException(String.format("Invalid port range specification: %s", spec));
        }

        validatePorts();
    }

    private void validatePorts() {
        if (fromPort < MIN_PORT || fromPort > MAX_PORT) {
            throw new IllegalArgumentException(
                String.format("Specified port (%s) outside of port range (%s-%s)", fromPort, MIN_PORT, MAX_PORT));
        }
        if (toPort < MIN_PORT || toPort > MAX_PORT) {
            throw new IllegalArgumentException(
                String.format("Specified port (%s) outside of port range (%s-%s)", toPort, MIN_PORT, MAX_PORT));
        }
        if (fromPort > toPort) {
            throw new IllegalArgumentException(String.format("Port %s is greater than %s", fromPort, toPort));
        }
    }

    public int getFromPort() {
        return fromPort;
    }

    public int getToPort() {
        return toPort;
    }

    @Override
    public String toString() {
        return String.format("%d-%d", fromPort, toPort);
    }
}
