/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.config;

import com.hazelcast.client.util.AddressHelper;
import com.hazelcast.config.GroupConfig;
import com.hazelcast.nio.SocketInterceptor;
import com.hazelcast.security.Credentials;
import com.hazelcast.security.UsernamePasswordCredentials;

import java.net.InetSocketAddress;
import java.util.*;

public class ClientConfig {

    private GroupConfig groupConfig = new GroupConfig();
    private final List<InetSocketAddress> addressList = new ArrayList<InetSocketAddress>(10);
    private Credentials credentials;
    private int connectionTimeout = 300000;
    private int initialConnectionAttemptLimit = 1;
    private int reconnectionAttemptLimit = 1;
    private int reConnectionTimeOut = 5000;
    private boolean shuffle = false;
    private boolean updateAutomatic = true;
    private SocketInterceptor socketInterceptor = null;
    private final Collection<EventListener> listeners = new HashSet<EventListener>();

    public SocketInterceptor getSocketInterceptor() {
        return socketInterceptor;
    }

    public void setSocketInterceptor(SocketInterceptor socketInterceptor) {
        this.socketInterceptor = socketInterceptor;
    }

    public int getReConnectionTimeOut() {
        return reConnectionTimeOut;
    }

    public ClientConfig setReConnectionTimeOut(int reConnectionTimeOut) {
        this.reConnectionTimeOut = reConnectionTimeOut;
        return this;
    }

    public int getReconnectionAttemptLimit() {
        return reconnectionAttemptLimit;
    }

    public ClientConfig setReconnectionAttemptLimit(int reconnectionAttemptLimit) {
        this.reconnectionAttemptLimit = reconnectionAttemptLimit;
        return this;
    }

    public int getInitialConnectionAttemptLimit() {
        return initialConnectionAttemptLimit;
    }

    public ClientConfig setInitialConnectionAttemptLimit(int initialConnectionAttemptLimit) {
        this.initialConnectionAttemptLimit = initialConnectionAttemptLimit;
        return this;
    }

    public int getConnectionTimeout() {
        return connectionTimeout;
    }

    public ClientConfig setConnectionTimeout(int connectionTimeout) {
        this.connectionTimeout = connectionTimeout;
        return this;
    }

    public Credentials getCredentials() {
        if (getCredentials() == null) {
            setCredentials(new UsernamePasswordCredentials(getGroupConfig().getName(),
                    getGroupConfig().getPassword()));
        }
        return credentials;
    }

    public ClientConfig setCredentials(Credentials credentials) {
        this.credentials = credentials;
        return this;
    }

    public ClientConfig addInetSocketAddress(List<InetSocketAddress> inetSocketAddresses) {
        this.addressList.addAll(inetSocketAddresses);
        return this;
    }

    public ClientConfig addInetSocketAddress(InetSocketAddress... inetSocketAddresses) {
        for (InetSocketAddress inetSocketAddress : inetSocketAddresses) {
            this.addressList.add(inetSocketAddress);
        }
        return this;
    }

    public ClientConfig addAddress(String... addresses) {
        for (String address : addresses) {
            this.addressList.addAll(AddressHelper.getSocketAddresses(address));
        }
        return this;
    }

    // required for spring module
    public void setAddresses(List<String> addresses) {
        addressList.clear();
        for (String address : addresses) {
            addressList.addAll(AddressHelper.getSocketAddresses(address));
        }
    }

    public Collection<InetSocketAddress> getAddressList() {
        if (getAddressList().size() == 0) {
            addAddress("localhost");
        }
        return addressList;
    }

    public GroupConfig getGroupConfig() {
        return groupConfig;
    }

    public ClientConfig setGroupConfig(GroupConfig groupConfig) {
        this.groupConfig = groupConfig;
        return this;
    }

    public void setShuffle(boolean shuffle) {
        this.shuffle = shuffle;
    }

    public boolean isShuffle() {
        return shuffle;
    }

    public boolean isUpdateAutomatic() {
        return updateAutomatic;
    }

    public void setUpdateAutomatic(boolean updateAutomatic) {
        this.updateAutomatic = updateAutomatic;
    }

    public Collection<EventListener> getListeners() {
        return listeners;
    }

    /**
     * Adds a listener object to configuration to be registered when {@code HazelcastClient} starts.
     *
     * @param listener one of {@link com.hazelcast.core.LifecycleListener}, {@link com.hazelcast.core.DistributedObjectListener}
     *                 or {@link com.hazelcast.core.MembershipListener}
     * @return
     */
    public ClientConfig addListener(EventListener listener) {
        listeners.add(listener);
        return this;
    }
}
