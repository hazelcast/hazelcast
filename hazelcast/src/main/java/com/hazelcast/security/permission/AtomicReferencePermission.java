package com.hazelcast.security.permission;

public class AtomicReferencePermission extends InstancePermission {

    private final static int READ = 0x4;
    private final static int MODIFY = 0x8;

    private final static int ALL = READ | MODIFY | CREATE | DESTROY;

    public AtomicReferencePermission(String name, String... actions) {
        super(name, actions);
    }

    protected int initMask(String[] actions) {
        int mask = NONE;
        for (int i = 0; i < actions.length; i++) {
            if (ActionConstants.ACTION_ALL.equals(actions[i])) {
                return ALL;
            }

            if (ActionConstants.ACTION_CREATE.equals(actions[i])) {
                mask |= CREATE;
            } else if (ActionConstants.ACTION_READ.equals(actions[i])) {
                mask |= READ;
            } else if (ActionConstants.ACTION_MODIFY.equals(actions[i])) {
                mask |= MODIFY;
            } else if (ActionConstants.ACTION_DESTROY.equals(actions[i])) {
                mask |= DESTROY;
            }
        }
        return mask;
    }
}