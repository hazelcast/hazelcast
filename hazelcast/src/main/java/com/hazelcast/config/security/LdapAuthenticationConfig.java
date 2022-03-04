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
public class LdapAuthenticationConfig extends AbstractClusterLoginConfig<LdapAuthenticationConfig> {

    private String url;
    private String socketFactoryClassName;
    private String systemUserDn;
    private String systemUserPassword;
    private String systemAuthentication;

    // BasicLdapLoginModule
    private Boolean parseDn;
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
    private Boolean skipAuthentication;
    private String securityRealm;

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

    public Boolean getParseDn() {
        return parseDn;
    }

    /**
     * Returns {@code true} iff parseDn is configured and {@code TRUE}.
     *
     * @deprecated Use {@link #getParseDn()}
     */
    public boolean isParseDn() {
        return parseDn != null && parseDn.booleanValue();
    }

    public LdapAuthenticationConfig setParseDn(Boolean parseDn) {
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

    public Boolean getSkipAuthentication() {
        return skipAuthentication;
    }

    public LdapAuthenticationConfig setSkipAuthentication(Boolean skipAuthentication) {
        this.skipAuthentication = skipAuthentication;
        return this;
    }

    public String getSecurityRealm() {
        return securityRealm;
    }

    public LdapAuthenticationConfig setSecurityRealm(String securityRealm) {
        this.securityRealm = securityRealm;
        return this;
    }

    public String getSystemAuthentication() {
        return systemAuthentication;
    }

    public LdapAuthenticationConfig setSystemAuthentication(String systemAuthentication) {
        this.systemAuthentication = systemAuthentication;
        return this;
    }

    @Override
    protected Properties initLoginModuleProperties() {
        Properties props = super.initLoginModuleProperties();
        setIfConfigured(props, Context.PROVIDER_URL, url);
        setIfConfigured(props, "java.naming.ldap.factory.socket", socketFactoryClassName);
        setIfConfigured(props, "parseDN", parseDn);
        setIfConfigured(props, "roleContext", roleContext);
        setIfConfigured(props, "roleFilter", roleFilter);
        setIfConfigured(props, "roleMappingAttribute", roleMappingAttribute);
        setIfConfigured(props, "roleMappingMode", roleMappingMode);
        setIfConfigured(props, "roleNameAttribute", roleNameAttribute);
        setIfConfigured(props, "roleRecursionMaxDepth", roleRecursionMaxDepth);
        setIfConfigured(props, "roleSearchScope", roleSearchScope);
        setIfConfigured(props, "userNameAttribute", userNameAttribute);

        boolean useSystemAccount = !isNullOrEmpty(systemUserDn) || !isNullOrEmpty(systemAuthentication);
        if (useSystemAccount) {
            setIfConfigured(props, Context.SECURITY_AUTHENTICATION, systemAuthentication);
            setIfConfigured(props, Context.SECURITY_PRINCIPAL, systemUserDn);
            setIfConfigured(props, Context.SECURITY_CREDENTIALS, systemUserPassword);
            setIfConfigured(props, "passwordAttribute", passwordAttribute);
            setIfConfigured(props, "userContext", userContext);
            setIfConfigured(props, "userFilter", userFilter);
            setIfConfigured(props, "userSearchScope", userSearchScope);
            setIfConfigured(props, "skipAuthentication", skipAuthentication);
            setIfConfigured(props, "securityRealm", securityRealm);
        }
        return props;
    }

    @Override
    public LoginModuleConfig[] asLoginModuleConfigs() {
        boolean useSystemAccount = !isNullOrEmpty(systemUserDn) || !isNullOrEmpty(systemAuthentication);
        LoginModuleConfig loginModuleConfig = new LoginModuleConfig(
                useSystemAccount ? "com.hazelcast.security.loginimpl.LdapLoginModule"
                        : "com.hazelcast.security.loginimpl.BasicLdapLoginModule",
                LoginModuleUsage.REQUIRED);

        loginModuleConfig.setProperties(initLoginModuleProperties());

        return new LoginModuleConfig[] { loginModuleConfig };
    }

    @Override
    public String toString() {
        return "LdapAuthenticationConfig [url=" + url + ", socketFactoryClassName=" + socketFactoryClassName + ", systemUserDn="
                + systemUserDn + ", systemUserPassword=***, parseDn=" + parseDn + ", roleContext=" + roleContext
                + ", roleFilter=" + roleFilter + ", roleMappingAttribute=" + roleMappingAttribute + ", roleMappingMode="
                + roleMappingMode + ", roleNameAttribute=" + roleNameAttribute + ", roleRecursionMaxDepth="
                + roleRecursionMaxDepth + ", roleSearchScope=" + roleSearchScope + ", userNameAttribute=" + userNameAttribute
                + ", passwordAttribute=" + passwordAttribute + ", userContext=" + userContext + ", userFilter=" + userFilter
                + ", userSearchScope=" + userSearchScope + ", skipAuthentication=" + skipAuthentication + ", securityRealm="
                + securityRealm + ", systemAuthentication=" + systemAuthentication + ", getSkipIdentity()=" + getSkipIdentity()
                + ", getSkipEndpoint()=" + getSkipEndpoint() + ", getSkipRole()=" + getSkipRole() + "]";
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = super.hashCode();
        result = prime * result + Objects.hash(parseDn, passwordAttribute, roleContext, roleFilter, roleMappingAttribute,
                roleMappingMode, roleNameAttribute, roleRecursionMaxDepth, roleSearchScope, skipAuthentication,
                socketFactoryClassName, systemUserDn, systemUserPassword, url, userContext, userFilter, userNameAttribute,
                userSearchScope, securityRealm, systemAuthentication);
        return result;
    }

    @Override
    @SuppressWarnings({"checkstyle:CyclomaticComplexity"})
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!super.equals(obj)) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        LdapAuthenticationConfig other = (LdapAuthenticationConfig) obj;
        return parseDn == other.parseDn && Objects.equals(passwordAttribute, other.passwordAttribute)
                && Objects.equals(roleContext, other.roleContext) && Objects.equals(roleFilter, other.roleFilter)
                && Objects.equals(roleMappingAttribute, other.roleMappingAttribute) && roleMappingMode == other.roleMappingMode
                && Objects.equals(roleNameAttribute, other.roleNameAttribute)
                && Objects.equals(roleRecursionMaxDepth, other.roleRecursionMaxDepth)
                && roleSearchScope == other.roleSearchScope && Objects.equals(skipAuthentication, other.skipAuthentication)
                && Objects.equals(socketFactoryClassName, other.socketFactoryClassName)
                && Objects.equals(systemUserDn, other.systemUserDn)
                && Objects.equals(systemUserPassword, other.systemUserPassword) && Objects.equals(url, other.url)
                && Objects.equals(userContext, other.userContext) && Objects.equals(userFilter, other.userFilter)
                && Objects.equals(securityRealm, other.securityRealm)
                && Objects.equals(systemAuthentication, other.systemAuthentication)
                && Objects.equals(userNameAttribute, other.userNameAttribute) && userSearchScope == other.userSearchScope;
    }

    @Override
    protected LdapAuthenticationConfig self() {
        return this;
    }

}
