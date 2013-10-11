package com.hazelcast.security.permission;


public class SemaphorePermission extends InstancePermission {
	
	private final static int ACQUIRE 		= 0x4;
	private final static int RELEASE 		= 0x8;
    private final static int READ	 		= 0x16;
	
	private final static int ALL 			= CREATE | DESTROY | ACQUIRE | RELEASE | READ;

	public SemaphorePermission(String name, String... actions) {
		super(name, actions);
	}

	protected int initMask(String[] actions) {
		int mask = NONE;
		for (int i = 0; i < actions.length; i++) {
			if(ActionConstants.ACTION_ALL.equals(actions[i])) {
				return ALL;
			}
			
			if(ActionConstants.ACTION_CREATE.equals(actions[i])) {
				mask |= CREATE;
			} else if(ActionConstants.ACTION_ACQUIRE.equals(actions[i])) {
				mask |= ACQUIRE;
			} else if(ActionConstants.ACTION_RELEASE.equals(actions[i])) {
				mask |= RELEASE;
			} else if(ActionConstants.ACTION_DESTROY.equals(actions[i])) {
				mask |= DESTROY;
			} else if(ActionConstants.ACTION_READ.equals(actions[i])) {
				mask |= READ;
            }
		}
		return mask;
	}
}
