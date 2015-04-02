package com.hazelcast.client.impl.protocol;

import com.hazelcast.client.AuthenticationException;
import com.hazelcast.client.ClientEndpoint;
import com.hazelcast.client.impl.client.ClientPrincipal;
import com.hazelcast.client.impl.operations.ClientReAuthOperation;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.instance.Node;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Connection;
import com.hazelcast.security.Credentials;
import com.hazelcast.util.UuidUtil;

import java.security.Permission;
import java.util.Collection;
import java.util.Set;

/**
 * Base authentication task
 */
public abstract class AuthenticationBaseMessageTask<CM extends ClientMessage>
        extends AbstractCallableMessageTask<CM> {

    protected transient ClientPrincipal principal;
    protected transient Credentials credentials;

    public AuthenticationBaseMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    public Object call() {
        boolean authenticated = authenticate();

        if (authenticated) {
            return handleAuthenticated();
        } else {
            return handleUnauthenticated();
        }
    }

    protected abstract boolean authenticate();

    private Object handleUnauthenticated() {
        return new AuthenticationException("Invalid credentials!");
    }

    private Object handleAuthenticated() {
        if (isOwnerConnection()) {
            final String uuid = getUuid();
            final String localMemberUUID = clientEngine.getLocalMember().getUuid();

            principal = new ClientPrincipal(uuid, localMemberUUID);
            reAuthLocal();
            Collection<MemberImpl> members = nodeEngine.getClusterService().getMemberList();
            for (MemberImpl member : members) {
                if (!member.localMember()) {
                    ClientReAuthOperation op = new ClientReAuthOperation(uuid);
                    op.setCallerUuid(localMemberUUID);
                    nodeEngine.getOperationService().send(op, member.getAddress());
                }
            }
        }

        endpoint.authenticated(principal, credentials, isOwnerConnection());
        endpointManager.registerEndpoint(endpoint);
        clientEngine.bind(endpoint);

        final Address thisAddress = clientEngine.getThisAddress();
        final AuthenticationResultParameters resultParameters = AuthenticationResultParameters
                .encode(thisAddress.getHost(), thisAddress.getPort(), principal.getUuid(), principal.getOwnerUuid());
        return resultParameters;
    }

    protected abstract boolean isOwnerConnection();

    private String getUuid() {
        if (principal != null) {
            return principal.getUuid();
        }
        return UuidUtil.createClientUuid(endpoint.getConnection().getEndPoint());
    }

    private void reAuthLocal() {
        final Set<ClientEndpoint> endpoints = endpointManager.getEndpoints(principal.getUuid());
        for (ClientEndpoint endpoint : endpoints) {
            endpoint.authenticated(principal);
        }
    }

    @Override
    public Permission getRequiredPermission() {
        return null;
    }
}
