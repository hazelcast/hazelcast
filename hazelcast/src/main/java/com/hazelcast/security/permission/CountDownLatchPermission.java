package com.hazelcast.security.permission;

import static com.hazelcast.security.SecurityConstants.*; 

public class CountDownLatchPermission extends InstancePermission {
	
	private final static int COUNTDOWN 		= 0x4;
	private final static int SET	 		= 0x8;
	private final static int STATS	 		= 0x16;
	private final static int ALL 			= CREATE | DESTROY | COUNTDOWN | STATS | SET;

	public CountDownLatchPermission(String name, String... actions) {
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
			} else if(SecurityConstants.ACTION_DESTROY.equals(actions[i])) {
				mask |= DESTROY;
			} else if(SecurityConstants.ACTION_COUNTDOWN.equals(actions[i])) {
				mask |= COUNTDOWN;
			} else if(SecurityConstants.ACTION_STATISTICS.equals(actions[i])) {
				mask |= STATS;
			} else if(SecurityConstants.ACTION_SET.equals(actions[i])) {
				mask |= SET;
			}
		}
		return mask;
	}
}
