package com.hazelcast.security.permission;

import static com.hazelcast.security.SecurityConstants.*; 

public class AtomicNumberPermission extends InstancePermission {
	
	private final static int INCREMENT 		= 0x4;
	private final static int DECREMENT 		= 0x8;
	private final static int GET 			= 0x16;
	private final static int SET 			= 0x32;
	private final static int ADD 			= 0x64;
	private final static int STATS	 		= 0x128;
	
	private final static int ALL 			= ADD | GET | SET | CREATE | DESTROY | INCREMENT | DECREMENT | STATS;

	public AtomicNumberPermission(String name, String... actions) {
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
			} else if(SecurityConstants.ACTION_SET.equals(actions[i])) {
				mask |= SET;
			} else if(SecurityConstants.ACTION_GET.equals(actions[i])) {
				mask |= GET;
			} else if(SecurityConstants.ACTION_INCREMENT.equals(actions[i])) {
				mask |= INCREMENT;
			} else if(SecurityConstants.ACTION_DESTROY.equals(actions[i])) {
				mask |= DESTROY;
			} else if(SecurityConstants.ACTION_DECREMENT.equals(actions[i])) {
				mask |= DECREMENT;
			} else if(SecurityConstants.ACTION_ADD.equals(actions[i])) {
				mask |= ADD;
			} else if(SecurityConstants.ACTION_STATISTICS.equals(actions[i])) {
				mask |= STATS;
			}
		}
		return mask;
	}
}
