package com.hazelcast.client.impl.protocol;

import com.hazelcast.instance.Node;
import com.hazelcast.nio.Connection;
import com.hazelcast.security.SecurityContext;

import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;
import java.security.Permission;
import java.util.logging.Level;

/**
 * Custom Authentication with custom credential impl
 */
public class AuthenticationCustomCredentialsMessageTask
        extends AuthenticationBaseMessageTask<AuthenticationCustomCredentialsParameters> {

    public AuthenticationCustomCredentialsMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected boolean isOwnerConnection() {
        return parameters.isOwnerConnection;
    }

    @Override
    protected AuthenticationCustomCredentialsParameters decodeClientMessage(ClientMessage clientMessage) {
        return AuthenticationCustomCredentialsParameters.decode(clientMessage);
    }

    protected boolean authenticate() {
        Connection connection = endpoint.getConnection();
        boolean authenticated = (clientEngine.getSecurityContext() != null) && authenticateWithCustomCredentials(
                clientEngine.getSecurityContext());
        logger.log((authenticated ? Level.INFO : Level.WARNING), "Received auth from " + connection + ", "
                + (authenticated ? "successfully authenticated" : "authentication failed"));
        return authenticated;
    }

    private boolean authenticateWithCustomCredentials(SecurityContext securityContext) {
        Connection connection = endpoint.getConnection();
        credentials.setEndpoint(connection.getInetAddress().getHostAddress());
        try {
            LoginContext lc = securityContext.createClientLoginContext(credentials);
            lc.login();
            endpoint.setLoginContext(lc);
            return true;
        } catch (LoginException e) {
            logger.warning(e);
            return false;
        }
    }

    @Override
    public Permission getRequiredPermission() {
        return null;
    }
}
