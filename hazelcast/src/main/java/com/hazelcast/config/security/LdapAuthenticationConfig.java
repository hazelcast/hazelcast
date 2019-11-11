/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

import static com.hazelcast.internal.util.StringUtil.isNullOrEmpty;

import java.util.Objects;
import java.util.Properties;

import javax.naming.Context;

import com.hazelcast.config.LoginModuleConfig;
import com.hazelcast.config.LoginModuleConfig.LoginModuleUsage;

/**
 * Typed authentication configuration for {@code BasicLdapLoginModule} and {@code LdapLoginModule}.
 */
@SuppressWarnings({"checkstyle:methodcount"})
public class LdapAuthenticationConfig implements AuthenticationConfig {

    private String url;
    private String socketFactoryClassName;
    private String systemUserDn;
    private String systemUserPassword;

    // BasicLdapLoginModule
    private boolean parseDn;
    private String roleContext;
    private String roleFilter;
    private String roleMappingAttribute;
    private LdapRoleMappingMode roleMappingMode;
    private String roleNameAttribute;
    private Integer roleRecursionMaxDepth;
    private LdapSearchScope roleSearchScope;
    private String userNameAttribute;

    // LdapLoginModule
    private String passwordAttribute;
    private String userContext;
    private String userFilter;
    private LdapSearchScope userSearchScope;

    public String getUrl() {
        return url;
    }

    public LdapAuthenticationConfig setUrl(String url) {
        this.url = url;
        return this;
    }

    public String getSocketFactoryClassName() {
        return socketFactoryClassName;
    }

    public LdapAuthenticationConfig setSocketFactoryClassName(String socketFactoryClassName) {
        this.socketFactoryClassName = socketFactoryClassName;
        return this;
    }

    public String getSystemUserDn() {
        return systemUserDn;
    }

    public LdapAuthenticationConfig setSystemUserDn(String systemUserDn) {
        this.systemUserDn = systemUserDn;
        return this;
    }

    public String getSystemUserPassword() {
        return systemUserPassword;
    }

    public LdapAuthenticationConfig setSystemUserPassword(String systemUserPassword) {
        this.systemUserPassword = systemUserPassword;
        return this;
    }

    public boolean isParseDn() {
        return parseDn;
    }

    public LdapAuthenticationConfig setParseDn(boolean parseDn) {
        this.parseDn = parseDn;
        return this;
    }

    public String getRoleContext() {
        return roleContext;
    }

    public LdapAuthenticationConfig setRoleContext(String roleContext) {
        this.roleContext = roleContext;
        return this;
    }

    public String getRoleFilter() {
        return roleFilter;
    }

    public LdapAuthenticationConfig setRoleFilter(String roleFilter) {
        this.roleFilter = roleFilter;
        return this;
    }

    public String getRoleMappingAttribute() {
        return roleMappingAttribute;
    }

    public LdapAuthenticationConfig setRoleMappingAttribute(String roleMappingAttribute) {
        this.roleMappingAttribute = roleMappingAttribute;
        return this;
    }

    public LdapRoleMappingMode getRoleMappingMode() {
        return roleMappingMode;
    }

    public LdapAuthenticationConfig setRoleMappingMode(LdapRoleMappingMode roleMappingMode) {
        this.roleMappingMode = roleMappingMode;
        return this;
    }

    public String getRoleNameAttribute() {
        return roleNameAttribute;
    }

    public LdapAuthenticationConfig setRoleNameAttribute(String roleNameAttribute) {
        this.roleNameAttribute = roleNameAttribute;
        return this;
    }

    public Integer getRoleRecursionMaxDepth() {
        return roleRecursionMaxDepth;
    }

    public LdapAuthenticationConfig setRoleRecursionMaxDepth(Integer roleRecursionMaxDepth) {
        this.roleRecursionMaxDepth = roleRecursionMaxDepth;
        return this;
    }

    public LdapSearchScope getRoleSearchScope() {
        return roleSearchScope;
    }

    public LdapAuthenticationConfig setRoleSearchScope(LdapSearchScope roleSearchScope) {
        this.roleSearchScope = roleSearchScope;
        return this;
    }

    public String getUserNameAttribute() {
        return userNameAttribute;
    }

    public LdapAuthenticationConfig setUserNameAttribute(String userNameAttribute) {
        this.userNameAttribute = userNameAttribute;
        return this;
    }

    public String getPasswordAttribute() {
        return passwordAttribute;
    }

    public LdapAuthenticationConfig setPasswordAttribute(String passwordAttribute) {
        this.passwordAttribute = passwordAttribute;
        return this;
    }

    public String getUserContext() {
        return userContext;
    }

    public LdapAuthenticationConfig setUserContext(String userContext) {
        this.userContext = userContext;
        return this;
    }

    public String getUserFilter() {
        return userFilter;
    }

    public LdapAuthenticationConfig setUserFilter(String userFilter) {
        this.userFilter = userFilter;
        return this;
    }

    public LdapSearchScope getUserSearchScope() {
        return userSearchScope;
    }

    public LdapAuthenticationConfig setUserSearchScope(LdapSearchScope userSearchScope) {
        this.userSearchScope = userSearchScope;
        return this;
    }

    @Override
    public LoginModuleConfig[] asLoginModuleConfigs() {
        boolean useSystemUser = !isNullOrEmpty(systemUserDn);
        LoginModuleConfig loginModuleConfig = new LoginModuleConfig(
                useSystemUser ? "com.hazelcast.security.loginimpl.LdapLoginModule"
                        : "com.hazelcast.security.loginimpl.BasicLdapLoginModule",
                LoginModuleUsage.REQUIRED);

        Properties props = loginModuleConfig.getProperties();
        setIfConfigured(props, Context.PROVIDER_URL, url);
        setIfConfigured(props, "java.naming.ldap.factory.socket", socketFactoryClassName);
        props.setProperty("parseDN", String.valueOf(parseDn));
        setIfConfigured(props, "roleContext", roleContext);
        setIfConfigured(props, "roleFilter", roleFilter);
        setIfConfigured(props, "roleMappingAttribute", roleMappingAttribute);
        setIfConfigured(props, "roleMappingMode", roleMappingMode);
        setIfConfigured(props, "roleNameAttribute", roleNameAttribute);
        setIfConfigured(props, "roleRecursionMaxDepth", roleRecursionMaxDepth);
        setIfConfigured(props, "roleSearchScope", roleSearchScope);
        setIfConfigured(props, "userNameAttribute", userNameAttribute);

        if (useSystemUser) {
            props.setProperty(Context.SECURITY_AUTHENTICATION, "simple");
            props.setProperty(Context.SECURITY_PRINCIPAL, systemUserDn);
            setIfConfigured(props, Context.SECURITY_CREDENTIALS, systemUserPassword);
            setIfConfigured(props, "passwordAttribute", passwordAttribute);
            setIfConfigured(props, "userContext", userContext);
            setIfConfigured(props, "userFilter", userFilter);
            setIfConfigured(props, "userSearchScope", userSearchScope);
        }

        return new LoginModuleConfig[] { loginModuleConfig };
    }

    @Override
    public String toString() {
        return "LdapAuthenticationConfig [url=" + url + ", socketFactoryClassName=" + socketFactoryClassName + ", systemUserDN="
                + systemUserDn + ", systemUserPassword=***, parseDN=" + parseDn + ", roleContext="
                + roleContext + ", roleFilter=" + roleFilter + ", roleMappingAttribute=" + roleMappingAttribute
                + ", roleMappingMode=" + roleMappingMode + ", roleNameAttribute=" + roleNameAttribute
                + ", roleRecursionMaxDepth=" + roleRecursionMaxDepth + ", roleSearchScope=" + roleSearchScope
                + ", userNameAttribute=" + userNameAttribute + ", passwordAttribute=" + passwordAttribute + ", userContext="
                + userContext + ", userFilter=" + userFilter + ", userSearchScope=" + userSearchScope + "]";
    }

    @Override
    public int hashCode() {
        return Objects.hash(parseDn, passwordAttribute, roleContext, roleFilter, roleMappingAttribute, roleMappingMode,
                roleNameAttribute, roleRecursionMaxDepth, roleSearchScope, socketFactoryClassName, systemUserDn,
                systemUserPassword, url, userContext, userFilter, userNameAttribute, userSearchScope);
    }

    @Override
    @SuppressWarnings("checkstyle:CyclomaticComplexity")
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
        LdapAuthenticationConfig other = (LdapAuthenticationConfig) obj;
        return parseDn == other.parseDn
                && Objects.equals(passwordAttribute, other.passwordAttribute)
                && Objects.equals(roleContext, other.roleContext)
                && Objects.equals(roleFilter, other.roleFilter)
                && Objects.equals(roleMappingAttribute, other.roleMappingAttribute)
                && roleMappingMode == other.roleMappingMode
                && Objects.equals(roleNameAttribute, other.roleNameAttribute)
                && Objects.equals(roleRecursionMaxDepth, other.roleRecursionMaxDepth)
                && roleSearchScope == other.roleSearchScope
                && Objects.equals(socketFactoryClassName, other.socketFactoryClassName)
                && Objects.equals(systemUserDn, other.systemUserDn)
                && Objects.equals(systemUserPassword, other.systemUserPassword)
                && Objects.equals(url, other.url)
                && Objects.equals(userContext, other.userContext)
                && Objects.equals(userFilter, other.userFilter)
                && Objects.equals(userNameAttribute, other.userNameAttribute)
                && userSearchScope == other.userSearchScope;
    }

    private void setIfConfigured(Properties props, String propertyName, String value) {
        if (!isNullOrEmpty(value)) {
            props.setProperty(propertyName, value);
        }
    }

    private void setIfConfigured(Properties props, String propertyName, Object value) {
        if (value != null) {
            props.setProperty(propertyName, value.toString());
        }
    }

    private void setIfConfigured(Properties props, String propertyName, Enum<?> value) {
        if (value != null) {
            props.setProperty(propertyName, value.toString());
        }
    }

}
