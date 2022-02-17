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

/**
 * This interface represents authentication configuration in a security realm. It provides a method to convert the configuration
 * into {@link LoginModuleConfig} form.
 */
public interface AuthenticationConfig {

    /**
     * Converts current configuration to stack of login modules.
     * @return login modules stack
     */
    LoginModuleConfig[] asLoginModuleConfigs();
}
