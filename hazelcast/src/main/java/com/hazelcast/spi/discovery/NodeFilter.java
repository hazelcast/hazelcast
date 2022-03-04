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

package com.hazelcast.spi.discovery;

/**
 * The NodeFilter, if supplied, will retrieve all discovered nodes and might
 * apply additional filtering based on vendor provided metadata. These metadata
 * are expected to be provided using the {@link DiscoveryNode#getProperties()}
 * method and may contain additional information initially setup by the user.
 * <p>
 * This is useful for additional security settings, to apply a certain type of
 * policy or to work around insufficient query languages of cloud providers.
 * <p>
 * Denied {@link DiscoveryNode}s will not be
 * handed over to the Hazelcast connection framework and therefore are not
 * known to the discovered.
 *
 * @since 3.6
 */
@FunctionalInterface
public interface NodeFilter {

    /**
     * Accepts or denies a {@link DiscoveryNode}
     * based on the implemented rules.
     *
     * @param candidate the candidate to be tested
     * @return true if the DiscoveryNode is selected to be discovered, otherwise false.
     */
    boolean test(DiscoveryNode candidate);
}
