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

package com.hazelcast.spi.discovery.integration;

/**
 * The <code>DiscoveryServiceProvider</code> interface provides the possibility to build {@link DiscoveryService}s.
 * <code>DiscoveryService</code> implementations should be immutable and therefore the provider is introduced to
 * provide this ability. Every service should have its own provider, however in rare cases a single provider might
 * create different <code>DiscoveryService</code> implementations based on the provided {@link DiscoveryMode} or other
 * configuration details.
 *
 * @since 3.6
 */
@FunctionalInterface
public interface DiscoveryServiceProvider {

    /**
     * Instantiates a new instance of the {@link DiscoveryService}.
     *
     * @param settings The settings to pass to creation of the <code>DiscoveryService</code>
     * @return a new instance of the discovery service
     */
    DiscoveryService newDiscoveryService(DiscoveryServiceSettings settings);

}
