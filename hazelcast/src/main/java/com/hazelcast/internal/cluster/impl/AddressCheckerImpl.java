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

package com.hazelcast.internal.cluster.impl;

import static com.hazelcast.internal.util.AddressUtil.matchAnyInterface;
import static java.lang.String.format;

import java.util.Set;

import com.hazelcast.cluster.Address;
import com.hazelcast.internal.cluster.AddressChecker;
import com.hazelcast.logging.ILogger;

/**
 * Check if {@link Address} belongs to set of trusted one.
 *
 * When no trusted interfaces were explicitly configured then all messages are deemed
 * as trusted.
 */
public final class AddressCheckerImpl implements AddressChecker {

    private final Set<String> trustedInterfaces;
    private final ILogger logger;

    public AddressCheckerImpl(Set<String> trustedInterfaces, ILogger logger) {
        this.trustedInterfaces = trustedInterfaces;
        this.logger = logger;
    }

    public boolean isTrusted(Address address) {
        if (address == null) {
            return false;
        }
        if (trustedInterfaces.isEmpty()) {
            return true;
        }

        String host = address.getHost();
        if (matchAnyInterface(host, trustedInterfaces)) {
            return true;
        } else {
            if (logger.isFineEnabled()) {
                logger.fine(format(
                        "Address %s doesn't match any trusted interface", host));
            }
            return false;
        }
    }
}
