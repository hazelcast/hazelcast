package com.hazelcast.client.connection;

import com.hazelcast.client.exception.AuthenticationException;

import java.io.IOException;

/**
 * @mdogan 5/15/13
 */
public interface Authenticator {

    void auth(Connection connection) throws AuthenticationException, IOException;

}
