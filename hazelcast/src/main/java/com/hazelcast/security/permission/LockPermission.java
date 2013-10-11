package com.hazelcast.security.permission;


public class LockPermission extends InstancePermission {
	
	private final static int LOCK	 		= 0x4;
    private final static int READ	 		= 0x8;
	private final static int ALL 			= CREATE | DESTROY | LOCK | READ;

	public LockPermission(String key, String... actions) {
		super(key, actions);
	}

	protected int initMask(String[] actions) {
		int mask = NONE;
		for (int i = 0; i < actions.length; i++) {
			if(ActionConstants.ACTION_ALL.equals(actions[i])) {
				return ALL;
			}
			
			if(ActionConstants.ACTION_CREATE.equals(actions[i])) {
				mask |= CREATE;
			} else if(ActionConstants.ACTION_DESTROY.equals(actions[i])) {
				mask |= DESTROY;
			} else if(ActionConstants.ACTION_LOCK.equals(actions[i])) {
				mask |= LOCK;
            } else if(ActionConstants.ACTION_READ.equals(actions[i])) {
                mask |= READ;
			}
		}
		return mask;
	}
}
