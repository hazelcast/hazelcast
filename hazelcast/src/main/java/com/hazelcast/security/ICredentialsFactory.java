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

import java.util.Properties;

import javax.security.auth.callback.CallbackHandler;

import com.hazelcast.cluster.Address;

/**
 * ICredentialsFactory is used to create {@link Credentials} objects to be used during node authentication before connection is
 * accepted by the master node.
 */
public interface ICredentialsFactory {

    /**
     * This method is (only) called if the factory instance is newly created from a class name provided in
     * {@link com.hazelcast.config.CredentialsFactoryConfig}.
     *
     * @param properties factory properties defined in configuration
     */
    default void init(Properties properties) {
    }

    /**
     * Configures {@link ICredentialsFactory}.
     *
     * @param callbackHandler callback handler which can provide access to system internals
     */
    void configure(CallbackHandler callbackHandler);

    /**
     * Creates new {@link Credentials} object.
     *
     * @return the new Credentials object
     */
    Credentials newCredentials();

    /**
     * Creates new {@link Credentials} object for given target {@link Address}.
     *
     * @param address Target {@link Address} (may be {@code null})
     * @return
     */
    default Credentials newCredentials(Address address) {
        return newCredentials();
    }

    /**
     * Destroys {@link ICredentialsFactory}.
     */
    void destroy();
}
