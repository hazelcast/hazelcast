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

import com.hazelcast.config.LoginModuleConfig;
import com.hazelcast.config.LoginModuleConfig.LoginModuleUsage;

/**
 * Default {@link AuthenticationConfig} implementation which just references the {@code DefaultLoginModule}.
 */
public final class DefaultAuthenticationConfig implements AuthenticationConfig {

    /**
     * Singleton instance.
     */
    public static final DefaultAuthenticationConfig INSTANCE = new DefaultAuthenticationConfig();

    private DefaultAuthenticationConfig() {
    }

    @Override
    public LoginModuleConfig[] asLoginModuleConfigs() {
        return new LoginModuleConfig[] {
                new LoginModuleConfig("com.hazelcast.security.loginimpl.DefaultLoginModule", LoginModuleUsage.REQUIRED) };
    }

}
