package com.hazelcast.security.permission;

import static com.hazelcast.security.SecurityConstants.*; 

public class SemaphorePermission extends InstancePermission {
	
	private final static int ACQUIRE 		= 0x4;
	private final static int RELEASE 		= 0x8;
	private final static int DRAIN 			= 0x16;
	private final static int STATS	 		= 0x32;
	
	private final static int ALL 			= CREATE | DESTROY | ACQUIRE | RELEASE | DRAIN | STATS;

	public SemaphorePermission(String name, String... actions) {
		super(name, actions);
	}

	protected int initMask(String[] actions) {
		int mask = NONE;
		for (int i = 0; i < actions.length; i++) {
			if(SecurityConstants.ACTION_ALL.equals(actions[i])) {
				return ALL;
			}
			
			if(SecurityConstants.ACTION_CREATE.equals(actions[i])) {
				mask |= CREATE;
			} else if(SecurityConstants.ACTION_ACQUIRE.equals(actions[i])) {
				mask |= ACQUIRE;
			} else if(SecurityConstants.ACTION_RELEASE.equals(actions[i])) {
				mask |= RELEASE;
			} else if(SecurityConstants.ACTION_DESTROY.equals(actions[i])) {
				mask |= DESTROY;
			} else if(SecurityConstants.ACTION_DRAIN.equals(actions[i])) {
				mask |= DRAIN;
			} else if(SecurityConstants.ACTION_STATISTICS.equals(actions[i])) {
				mask |= STATS;
			}
		}
		return mask;
	}
}
