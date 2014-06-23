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

package com.hazelcast.security;

import javax.security.auth.Subject;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;
import java.io.IOException;
import java.net.Socket;
import java.security.AccessControlException;
import java.security.Permission;
import java.util.concurrent.Callable;

/**
 * SecurityContext is responsible for managing lifecycle of security object such as
 * {@link ICredentialsFactory}, {@link IPermissionPolicy} etc, to creating {@link LoginContext}es
 * for member and client authentications and checking permissions for client operations.
 */
public interface SecurityContext {

    /**
     * Creates member {@link LoginContext}.
     *
     * @param credentials member credentials
     * @return {@link LoginContext}
     * @throws LoginException
     */
    LoginContext createMemberLoginContext(Credentials credentials) throws LoginException;

    /**
     * Creates client {@link LoginContext}.
     *
     * @param credentials client credentials
     * @return {@link LoginContext}
     * @throws LoginException
     */
    LoginContext createClientLoginContext(Credentials credentials) throws LoginException;

    /**
     * Returns current {@link ICredentialsFactory}.
     *
     * @return {@link ICredentialsFactory}
     */
    ICredentialsFactory getCredentialsFactory();

    /**
     * Checks whether current {@link Subject} has been granted specified permission or not.
     *
     * @param permission
     * @throws AccessControlException
     */
    void checkPermission(Subject subject, Permission permission) throws AccessControlException;

    /**
     * Creates secure callable that runs in a sandbox.
     *
     * @param <V>      return type of callable
     * @param subject
     * @param callable
     * @return result of callable
     */
    <V> SecureCallable<V> createSecureCallable(Subject subject, Callable<V> callable);

    /**
     * Destroys {@link SecurityContext} and all security elements.
     */
    void destroy();

    /**
     * Intercept given socket if a {@link com.hazelcast.nio.SocketInterceptor} is configured
     * @param socket
     * @param onAccept
     * @throws IOException
     */
    void interceptSocket(Socket socket, boolean onAccept) throws IOException;
}
