/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client;

import com.hazelcast.client.impl.client.ClientPrincipal;
import com.hazelcast.nio.Connection;
import com.hazelcast.security.Credentials;
import com.hazelcast.transaction.TransactionContext;

import javax.security.auth.Subject;
import javax.security.auth.login.LoginContext;

/**
 * Represents an endpoint to a client. So for each client connected to a member, a ClientEndpoint object is available.
 */
public interface ClientEndpoint {

    /**
     * Checks if the endpoint is alive.
     *
     * @return true if alive, false otherwise.
     */
    boolean isAlive();

    //TODO remove after requests removed
    void sendResponse(Object response, int callId);

    //TODO remove after requests removed
    void sendEvent(Object key, Object event, int callId);

    void setListenerRegistration(String service, String topic, String id);

    String getUuid();

    Credentials getCredentials();

    void setTransactionContext(TransactionContext context);

    TransactionContext getTransactionContext(String txnId);

    void removeTransactionContext(String txnId);

    boolean isFirstConnection();

    Subject getSubject();

    void clearAllListeners();

    Connection getConnection();

    void setLoginContext(LoginContext lc);

    void authenticated(ClientPrincipal principal, Credentials credentials, boolean firstConnection);

    void authenticated(ClientPrincipal principal);

    void setDistributedObjectListener(String registrationId);

    ClientPrincipal getPrincipal();

}
