package com.hazelcast.security.permission;

import java.security.Permission;
import java.security.PermissionCollection;
import java.util.*;

public class ClusterPermissionCollection extends PermissionCollection {

    final Set<Permission> perms = new HashSet<Permission>();
    final Class<? extends Permission> permClass;

    public ClusterPermissionCollection() {
        super();
        permClass = null;
    }

    public ClusterPermissionCollection(Class<? extends Permission> permClass) {
        super();
        this.permClass = permClass;
    }

    public void add(Permission permission) {
        if (isReadOnly()) {
            throw new SecurityException("ClusterPermissionCollection is read-only!");
        }
        boolean shouldAdd = (permClass != null && permClass.equals(permission.getClass()))
                || (permission instanceof ClusterPermission);

        if (shouldAdd && !implies(permission)) {
            perms.add(permission);
        }
    }

    public void add(PermissionCollection permissions) {
        if (isReadOnly()) {
            throw new SecurityException("ClusterPermissionCollection is read-only!");
        }
        if (permissions instanceof ClusterPermissionCollection) {
            for (Permission p : ((ClusterPermissionCollection) permissions).perms) {
                add(p);
            }
        }
    }

    public boolean implies(Permission permission) {
        for (Permission p : perms) {
            if (p.implies(permission)) {
                return true;
            }
        }
        return false;
    }

    public void compact() {
        if (isReadOnly()) {
            throw new SecurityException("ClusterPermissionCollection is read-only!");
        }
        final Iterator<Permission> iter = perms.iterator();
        while (iter.hasNext()) {
            final Permission perm = iter.next();
            boolean implies = false;
            for (Permission p : perms) {
                if (p != perm && p.implies(perm)) {
                    implies = true;
                    break;
                }
            }
            if (implies) {
                iter.remove();
            }
        }
        setReadOnly();
    }

    public Enumeration<Permission> elements() {
        return Collections.enumeration(perms);
    }

    public Set<Permission> getPermissions() {
        return Collections.unmodifiableSet(perms);
    }

    @Override
    public String toString() {
        return "ClusterPermissionCollection [permClass=" + permClass + "]";
    }
}
