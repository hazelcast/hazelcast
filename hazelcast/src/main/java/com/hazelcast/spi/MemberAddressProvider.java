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

package com.hazelcast.spi;

import com.hazelcast.config.NetworkConfig;
import com.hazelcast.instance.AddressPicker;
import com.hazelcast.instance.EndpointQualifier;

import java.net.InetSocketAddress;
import java.util.List;

/**
 * <b>IMPORTANT</b>
 * This interface is not intended to provide addresses of other cluster members with
 * which the hazelcast instance will form a cluster. This is an SPI for advanced use in
 * cases where the {@link AddressPicker} implementation does not
 * pick suitable addresses to bind to and publish to other cluster members.
 * For instance, this could allow easier deployment in some cases when running on
 * Docker, AWS or other cloud environments.
 *
 * That said, if you are just starting with Hazelcast, you will probably want to
 * set the member addresses by using {@link com.hazelcast.config.TcpIpConfig#setMembers(List)},
 * {@link com.hazelcast.config.MulticastConfig} or adding a discovery strategy.
 *
 * Allow to customize:
 * 1. What address Hazelcast will bind to
 * 2. What address Hazelcast will advertise to other members on which they can bind to
 * <p>
 * This is practical in some cloud environments where the default strategy is not yielding good results.
 *
 * @see com.hazelcast.config.NetworkConfig#setMemberAddressProviderConfig(com.hazelcast.config.MemberAddressProviderConfig)
 */
public interface MemberAddressProvider {
    /**
     * What address should Hazelcast bind to.
     * When the port is set to {@code 0} then it will use a port as
     * configured in the Hazelcast network configuration.
     *
     * @return address to bind to
     * @see NetworkConfig#getPort()
     * @see NetworkConfig#isPortAutoIncrement()
     */
    InetSocketAddress getBindAddress();

    InetSocketAddress getBindAddress(EndpointQualifier qualifier);

    /**
     * What address should Hazelcast advertise to other members and clients.
     * When the port is set to {@code 0} then it will broadcast the same
     * port that it is bound to.
     *
     * @return address to advertise to others
     */
    InetSocketAddress getPublicAddress();

    InetSocketAddress getPublicAddress(EndpointQualifier qualifier);
}
