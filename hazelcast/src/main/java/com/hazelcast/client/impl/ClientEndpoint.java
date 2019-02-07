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

package com.hazelcast.client.impl;

import com.hazelcast.client.impl.client.ClientPrincipal;
import com.hazelcast.core.Client;
import com.hazelcast.nio.Connection;
import com.hazelcast.security.Credentials;
import com.hazelcast.transaction.TransactionContext;

import javax.security.auth.Subject;
import javax.security.auth.login.LoginContext;
import java.util.Set;
import java.util.concurrent.Callable;

/**
 * Represents an endpoint to a client. So for each client connected to a member, a ClientEndpoint object is available.
 */
public interface ClientEndpoint extends Client {

    /**
     * Checks if the endpoint is alive.
     *
     * @return true if alive, false otherwise.
     */
    boolean isAlive();

    /**
     * Adds a remove callable to be called when endpoint is destroyed to clean related listener
     * Following line will be called when endpoint destroyed :
     * eventService.deregisterListener(service, topic, ID);
     * Note: removeDestroyAction should be called when there is no need to destroy action anymore.
     *
     * @param service name of the related service of listener
     * @param topic   topic name of listener(mostly distributed object name)
     * @param id      registration ID of remove action
     */
    void addListenerDestroyAction(String service, String topic, String id);

    /**
     * Adds a remove callable to be called when endpoint is destroyed
     * Note: removeDestroyAction should be called when there is no need to destroy action anymore.
     *
     * @param registrationId registration ID of destroy action
     * @param removeAction   callable that will be called when endpoint is destroyed
     */
    void addDestroyAction(String registrationId, Callable<Boolean> removeAction);

    /**
     * @param id registration ID of destroy action
     * @return true if remove is successful
     */
    boolean removeDestroyAction(String id);

    Credentials getCredentials();

    void setTransactionContext(TransactionContext context);

    TransactionContext getTransactionContext(String txnId);

    void removeTransactionContext(String txnId);

    /**
     * Indicates whether this endpoint is the owner connection for that client.
     * @return {@code true} when this endpoint is the owner connection for the client
     */
    boolean isOwnerConnection();

    Subject getSubject();

    void clearAllListeners();

    Connection getConnection();

    void setLoginContext(LoginContext lc);

    void authenticated(ClientPrincipal principal, Credentials credentials, boolean firstConnection, String clientVersion,
                       long authCorrelationId, String clientName, Set<String> labels);

    void authenticated(ClientPrincipal principal);

    /**
     * @return true if endpoint is authenticated with valid security credentials, returns false otherwise
     */
    boolean isAuthenticated();

    /**
     * @return the client version as calculated by {@link com.hazelcast.instance.BuildInfo}
     */
    int getClientVersion();

    /**
     * @param version the version string as obtained from the environment
     */
    void setClientVersion(String version);

    /**
     * Updates to the latest client statistics.
     *
     * @param stats the latest statistics retrieved from the client
     */
    void setClientStatistics(String stats);

    /**
     * @return statistics string for the client
     */
    String getClientStatistics();
}
