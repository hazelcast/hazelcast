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

package com.hazelcast.security;

import com.hazelcast.config.PermissionConfig;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.sql.impl.security.SqlSecurityContext;

import javax.security.auth.Subject;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;
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
     * @param credentials
     * @param serviceName
     * @param objectName
     * @param methodName
     * @param parameters
     * @throws AccessControlException if access is denied
     */
    void interceptBefore(Credentials credentials, String serviceName, String objectName,
                         String methodName, Object[] parameters) throws AccessControlException;

    /**
     * intercepts a request after process if any {@link SecurityInterceptor} configured
     * Any exception thrown during interception will be ignored
     *
     * @param credentials
     * @param serviceName
     * @param objectName
     * @param methodName
     */
    void interceptAfter(Credentials credentials, String serviceName, String objectName, String methodName);

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
     * Creates secure callable that runs in a sandbox.
     *
     * @param <V>      return type of callable
     * @param subject
     * @param runnable
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
