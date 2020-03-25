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

package com.hazelcast.config;

import java.util.Set;

/**
 * Interface defining methods used to configure trusted interfaces (source IP addresses).
 *
 * @param <T> implementing class type
 */
public interface TrustedInterfacesConfigurable<T extends TrustedInterfacesConfigurable<?>> {

    /**
     * Adds trusted interface (i.e. source IP address or expression).
     *
     * @param ip IP address to be added.
     * @return configuration object itself
     */
    T addTrustedInterface(String ip);

    /**
     * Sets the trusted interfaces.
     *
     * @param interfaces the new trusted interfaces
     * @return configuration object itself
     */
    T setTrustedInterfaces(Set<String> interfaces);

    /**
     * Gets the trusted interfaces.
     *
     * @return the trusted interfaces
     */
    Set<String> getTrustedInterfaces();
}
