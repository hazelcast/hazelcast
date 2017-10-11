/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.spi;

import com.hazelcast.config.AddressLocatorConfig;

import java.net.InetSocketAddress;

/**
 * Allow to customize:
 * 1. What address Hazelcast will bind to
 * 2. What address Hazelcast will advertise to others
 *
 * This is practical in some cloud environments where the default strategy is not yielding good results.
 *
 * See also {@link com.hazelcast.instance.DefaultAddressPicker}
 * and {@link com.hazelcast.config.NetworkConfig#setAddressLocatorConfig(AddressLocatorConfig)}
 *
 *
 */
public interface AddressLocator {
    /**
     * What address should Hazelcast bind to. When port is set to 0 then it will use a port as configured in Hazelcast
     * network configuration
     *
     * @return Address to bind to
     */
    InetSocketAddress getBindAddress();

    /**
     * What address should Hazelcast advertise to other members and clients.
     * When port is 0 then it will broadcast the same port it is bound to.
     *
     * @return Address to advertise to others
     */
    InetSocketAddress getPublicAddress();
}
