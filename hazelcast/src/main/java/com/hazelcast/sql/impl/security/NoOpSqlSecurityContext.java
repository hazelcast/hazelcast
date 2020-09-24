package com.hazelcast.sql.impl.security;

import com.hazelcast.security.SecurityContext;

public final class NoOpSqlSecurityContext implements SqlSecurityContext {

    public static final NoOpSqlSecurityContext INSTANCE = new NoOpSqlSecurityContext();

    private NoOpSqlSecurityContext() {
        // No-op.
    }

    @Override
    public boolean isSecurityEnabled() {
        return false;
    }

    @Override
    public void checkMapReadPermission(SecurityContext securityContext, String mapName) {
        // No-op.
    }
}
