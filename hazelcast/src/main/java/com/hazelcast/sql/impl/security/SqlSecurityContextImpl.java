package com.hazelcast.sql.impl.security;

import com.hazelcast.security.SecurityContext;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.MapPermission;

import javax.security.auth.Subject;

public class SqlSecurityContextImpl implements SqlSecurityContext {

    private final Subject subject;

    public SqlSecurityContextImpl(Subject subject) {
        this.subject = subject;
    }

    @Override
    public boolean isSecurityEnabled() {
        return true;
    }

    @Override
    public void checkMapReadPermission(SecurityContext securityContext, String mapName) {
        MapPermission permission = new MapPermission(mapName, ActionConstants.ACTION_READ);

        securityContext.checkPermission(subject, permission);
    }
}
