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

package com.hazelcast.config.security;

import com.hazelcast.security.ICredentialsFactory;

/**
 * This interface represents identity configuration in security realms or client security configurations. It provides a method
 * to convert the configuration into a {@link ICredentialsFactory} instance.
 */
public interface IdentityConfig {

    /**
     * Converts current configuration to a {@link ICredentialsFactory} instance.
     *
     * @param cl class loader to be used if the credentials factory class has to be constructed.
     * @return {@link ICredentialsFactory} instance
     */
    ICredentialsFactory asCredentialsFactory(ClassLoader cl);

    /**
     * Makes a copy (or clone) of the config object.
     */
    IdentityConfig copy();
}
