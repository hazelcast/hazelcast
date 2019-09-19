/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.cluster.impl;

import com.hazelcast.config.MulticastConfig;
import com.hazelcast.logging.ILogger;

import java.util.Set;

import static com.hazelcast.internal.util.AddressUtil.matchAnyInterface;
import static java.lang.String.format;

/**
 * Check if a received join message is to be trusted.
 *
 * To be precise it's checking if IP of the JoinMessage sender
 * is among configured {@link MulticastConfig#getTrustedInterfaces()}
 *
 * When no trusted interfaces were explicitly configured then all messages are deemed
 * as trusted.
 *
 */
final class JoinMessageTrustChecker {
    private final Set<String> trustedInterfaces;
    private final ILogger logger;

    JoinMessageTrustChecker(Set<String> trustedInterfaces, ILogger logger) {
        this.trustedInterfaces = trustedInterfaces;
        this.logger = logger;
    }

    boolean isTrusted(JoinMessage joinMessage) {
        if (trustedInterfaces.isEmpty()) {
            return true;
        }

        String host = joinMessage.getAddress().getHost();
        if (matchAnyInterface(host, trustedInterfaces)) {
            return true;
        } else {
            if (logger.isFineEnabled()) {
                logger.fine(format(
                        "JoinMessage from %s is dropped because its sender is not a trusted interface", host));
            }
            return false;
        }
    }
}
