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

package com.hazelcast.client.impl.connection.tcp;

import com.hazelcast.client.impl.management.ClientConnectionProcessListener;
import com.hazelcast.cluster.Address;
import com.hazelcast.logging.ILogger;

import java.util.List;

class ExceptionCatchingConnectionProcessListener
        implements ClientConnectionProcessListener {

    private final ClientConnectionProcessListener delegate;
    private final ILogger logger;

    ExceptionCatchingConnectionProcessListener(ClientConnectionProcessListener delegate, ILogger logger) {
        this.delegate = delegate;
        this.logger = logger;
    }

    private void runSafely(Runnable r) {
        try {
            r.run();
        } catch (RuntimeException e) {
            logger.severe("exception thrown by ClientConnectionProcessListener", e);
        }
    }

    @Override
    public void attemptingToConnectToAddress(Address address) {
        runSafely(() -> delegate.attemptingToConnectToAddress(address));
    }

    @Override
    public void connectionAttemptFailed(Address target) {
        runSafely(() -> delegate.connectionAttemptFailed(target));
    }

    @Override
    public void hostNotFound(String host) {
        runSafely(() -> delegate.hostNotFound(host));
    }

    @Override
    public void possibleAddressesCollected(List<Address> addresses) {
        runSafely(() -> delegate.possibleAddressesCollected(addresses));
    }

    @Override
    public void authenticationSuccess(Address remoteAddress) {
        runSafely(() -> delegate.authenticationSuccess(remoteAddress));
    }

    @Override
    public void credentialsFailed(Address remoteAddress) {
        runSafely(() -> delegate.credentialsFailed(remoteAddress));
    }

    @Override
    public void clientNotAllowedInCluster(Address remoteAddress) {
        runSafely(() -> delegate.clientNotAllowedInCluster(remoteAddress));
    }

    @Override
    public void clusterConnectionFailed(String clusterName) {
        runSafely(() -> delegate.clusterConnectionFailed(clusterName));
    }

    @Override
    public void clusterConnectionSucceeded(String clusterName) {
        runSafely(() -> delegate.clusterConnectionSucceeded(clusterName));
    }

    @Override
    public void remoteClosedConnection(Address address) {
        runSafely(() -> delegate.remoteClosedConnection(address));
    }
}
