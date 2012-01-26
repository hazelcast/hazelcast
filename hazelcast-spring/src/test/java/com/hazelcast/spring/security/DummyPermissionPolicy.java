package com.hazelcast.spring.security;

import com.hazelcast.config.SecurityConfig;
import com.hazelcast.security.IPermissionPolicy;

import javax.security.auth.Subject;
import java.security.Permission;
import java.security.PermissionCollection;
import java.util.Properties;

public class DummyPermissionPolicy implements IPermissionPolicy {

    public void configure(SecurityConfig securityConfig, Properties properties) {
    }

    public PermissionCollection getPermissions(Subject subject,
                                               Class<? extends Permission> type) {
        return null;
    }

    public void destroy() {
    }
}
