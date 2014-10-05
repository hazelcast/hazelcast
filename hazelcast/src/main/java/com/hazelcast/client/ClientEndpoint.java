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

    void sendResponse(Object response, int callId);

    void sendEvent(Object event, int callId);

    void setListenerRegistration(String service, String topic, String id);

    String getUuid();

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
