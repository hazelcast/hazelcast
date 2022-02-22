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

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import com.hazelcast.config.LoginModuleConfig;

/**
 * This {@link AuthenticationConfig} implementation is a imple holder for login module configurations.
 */
public class JaasAuthenticationConfig implements AuthenticationConfig {

    private List<LoginModuleConfig> loginModuleConfigs = new ArrayList<>();

    public List<LoginModuleConfig> getLoginModuleConfigs() {
        return loginModuleConfigs;
    }

    public JaasAuthenticationConfig setLoginModuleConfigs(List<LoginModuleConfig> loginModuleConfigs) {
        this.loginModuleConfigs = loginModuleConfigs;
        return this;
    }

    public JaasAuthenticationConfig addLoginModuleConfig(LoginModuleConfig loginModuleConfig) {
        loginModuleConfigs.add(loginModuleConfig);
        return this;
    }

    @Override
    public LoginModuleConfig[] asLoginModuleConfigs() {
        return loginModuleConfigs.toArray(new LoginModuleConfig[0]);
    }

    @Override
    public String toString() {
        return "JaasAuthenticationConfig [loginModuleConfigs=" + loginModuleConfigs + "]";
    }

    @Override
    public int hashCode() {
        return Objects.hash(loginModuleConfigs);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        JaasAuthenticationConfig other = (JaasAuthenticationConfig) obj;
        return Objects.equals(loginModuleConfigs, other.loginModuleConfigs);
    }
}
