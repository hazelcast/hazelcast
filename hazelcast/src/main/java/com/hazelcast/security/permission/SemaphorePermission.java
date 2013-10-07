package com.hazelcast.security.permission;


public class SemaphorePermission extends InstancePermission {
	
	private final static int ACQUIRE 		= 0x4;
	private final static int RELEASE 		= 0x8;
	private final static int DRAIN 			= 0x16;
	private final static int STATS	 		= 0x32;
    private final static int GET	 		= 0x64;
	
	private final static int ALL 			= CREATE | DESTROY | ACQUIRE | RELEASE | DRAIN | STATS | GET;

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
			} else if(ActionConstants.ACTION_DRAIN.equals(actions[i])) {
				mask |= DRAIN;
			} else if(ActionConstants.ACTION_STATISTICS.equals(actions[i])) {
				mask |= STATS;
			} else if(ActionConstants.ACTION_GET.equals(actions[i])) {
                mask |= GET;
            }
		}
		return mask;
	}
}
