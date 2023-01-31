/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.impl.connection;


import com.hazelcast.client.impl.management.ClientConnectionProcessListener;
import com.hazelcast.client.impl.management.ClientConnectionProcessListenerRunner;
import com.hazelcast.cluster.Address;
import com.hazelcast.cluster.Member;

import java.util.List;

/**
 * Provides initial addresses for client to find and connect to a node &
 * Translates given address if necessary when connecting a service
 */
public interface AddressProvider {

    /**
     * @param listenerRunner that runs the listeners'
     *                       {@link
     *                       ClientConnectionProcessListener#possibleAddressesCollected(List)}
     *                       method once the possible member addresses are all
     *                       collected by the implementation. Prior to that,
     *                       during address collection, the
     *                       {@link
     *                       ClientConnectionProcessListener#hostNotFound(String)}
     *                       method can be invoked, e.g. in cases when the
     *                       address provider tries to access an address that
     *                       doesn't exist.
     * @return The possible member addresses to connect to.
     * @throws Exception when a remote service can not provide addressee
     */
    Addresses loadAddresses(ClientConnectionProcessListenerRunner listenerRunner) throws Exception;

    /**
     * Translates the given address to another address specific to network or
     * service
     *
     * @param address to be translated
     * @return new address if given address is known, otherwise return null
     * @throws Exception when a remote service can not provide addressee
     */
    Address translate(Address address) throws Exception;

    /**
     * Implementations of this will handle returning the public address of the
     * member if necessary. See
     * {@link
     * com.hazelcast.client.impl.spi.impl.DefaultAddressProvider#translate(Member)}
     */
    Address translate(Member member) throws Exception;
}
