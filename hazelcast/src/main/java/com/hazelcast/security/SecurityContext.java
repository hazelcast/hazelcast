/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.config.PermissionConfig;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.sql.impl.security.SqlSecurityContext;

import javax.annotation.Nonnull;
import javax.security.auth.Subject;
import javax.security.auth.login.Configuration;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;

import java.net.InetAddress;
import java.security.AccessControlException;
import java.security.Permission;
import java.util.Set;
import java.util.concurrent.Callable;

/**
 * SecurityContext is responsible for managing lifecycle of security object such as
 * {@link ICredentialsFactory}, {@link IPermissionPolicy} etc, to creating {@link LoginContext}es
 * for member and client authentications and checking permissions for client operations.
 */
public interface SecurityContext {

    /**
     * Creates member {@link LoginContext}.
     * @param clusterName cluster name received from the connecting member
     * @param credentials member credentials
     * @param connection member connection
     *
     * @return {@link LoginContext}
     * @throws LoginException in case of any exceptional case
     */
    LoginContext createMemberLoginContext(String clusterName, Credentials credentials, Connection connection)
            throws LoginException;

    /**
     * Creates client {@link LoginContext}.
     *
     * @param clusterName cluster name reported on the client protocol
     * @param credentials client credentials
     * @param connection client connection
     * @return {@link LoginContext}
     * @throws LoginException in case of any exceptional case
     */
    LoginContext createClientLoginContext(String clusterName, Credentials credentials, Connection connection)
            throws LoginException;

    /**
     * Creates JAAS login {@link Configuration} from given Security Realm configuration.
     *
     * @param realmName security realm name
     * @return {@link Configuration} for given realm (or default authentication configuration if the realm doesn't exist).
     */
    Configuration createLoginConfigurationForRealm(String realmName);

    /**
     * Creates {@link LoginContext} from given JAAS Configuration.
     *
     * @param configuration JAAS configuration object
     * @param clusterName cluster name
     * @param remoteAddress address of the HTTP client
     * @return {@link LoginContext}
     * @throws LoginException in case of any exceptional case
     */
    LoginContext createLoginContext(@Nonnull Configuration configuration, String clusterName, Credentials credentials,
            InetAddress remoteAddress) throws LoginException;

    /**
     * Returns current {@link ICredentialsFactory}.
     *
     * @return {@link ICredentialsFactory}
     */
    ICredentialsFactory getCredentialsFactory();

    /**
     * Checks whether current {@link Subject} has been granted specified permission or not.
     *
     * @param subject the current subject
     * @param permission the specified permission for the subject
     * @throws AccessControlException if the specified permission has not been granted to the subject
     */
    void checkPermission(Subject subject, Permission permission) throws AccessControlException;


    /**
     * intercepts a request before process if any {@link SecurityInterceptor} configured
     *
     * @throws AccessControlException if access is denied
     */
    void interceptBefore(Credentials credentials, String serviceName, String objectName,
                         String methodName, Object[] parameters) throws AccessControlException;

    /**
     * intercepts a request after process if any {@link SecurityInterceptor} configured
     * Any exception thrown during interception will be ignored
     */
    void interceptAfter(Credentials credentials, String serviceName, String objectName, String methodName);

    /**
     * Creates secure callable that runs in a sandbox.
     *
     * @param <V>      return type of callable
     * @return result of callable
     */
    <V> SecureCallable<V> createSecureCallable(Subject subject, Callable<V> callable);

    /**
     * Creates secure callable that runs in a sandbox.
     *
     * @param <V>      return type of callable
     * @return Will always return null after {@link Runnable} finishes running.
     */
    <V> SecureCallable<?> createSecureCallable(Subject subject, Runnable runnable);

    /**
     * Destroys {@link SecurityContext} and all security elements.
     */
    void destroy();

    void refreshPermissions(Set<PermissionConfig> permissionConfigs);

    SqlSecurityContext createSqlContext(Subject subject);
}
