package com.hazelcast.sql.impl.security;

import java.security.Permission;

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
    public void checkPermission(Permission permission) {
        // No-op.
    }
}
