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

package com.hazelcast.access;

import java.util.Properties;

import javax.annotation.Nonnull;
import javax.security.auth.callback.CallbackHandler;

/**
 * Interface implemented by {@link AccessControlService} factory classes.
 */
public interface AccessControlServiceFactory {

    /**
     * Initializes this class with given properties. The callbackHandler can provide access to member internals.
     *
     * @param callbackHandler callback handler which provides access to member internals
     * @param properties properties from the config
     */
    void init(@Nonnull CallbackHandler callbackHandler, @Nonnull Properties properties) throws Exception;

    /**
     * Creates the configured {@link AccessControlService} instance.
     */
    @Nonnull
    AccessControlService createAccessControlService() throws Exception;
}
