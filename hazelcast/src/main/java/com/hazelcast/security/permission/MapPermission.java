package com.hazelcast.security.permission;

import static com.hazelcast.security.SecurityConstants.*; 

public class MapPermission extends InstancePermission {
	
	private final static int PUT 			= 0x4;
	private final static int GET 			= 0x8;
	private final static int REMOVE 		= 0x16;
	private final static int LISTEN 		= 0x32;
	private final static int LOCK	 		= 0x64;
	private final static int STATS	 		= 0x128;
	private final static int ALL 			= PUT | GET | REMOVE | CREATE | DESTROY | LISTEN | LOCK | STATS;

	public MapPermission(String name, String... actions) {
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
			} else if(SecurityConstants.ACTION_PUT.equals(actions[i])) {
				mask |= PUT;
			} else if(SecurityConstants.ACTION_GET.equals(actions[i])) {
				mask |= GET;
			} else if(SecurityConstants.ACTION_REMOVE.equals(actions[i])) {
				mask |= REMOVE;
			} else if(SecurityConstants.ACTION_DESTROY.equals(actions[i])) {
				mask |= DESTROY;
			} else if(SecurityConstants.ACTION_LISTEN.equals(actions[i])) {
				mask |= LISTEN;
			} else if(SecurityConstants.ACTION_LOCK.equals(actions[i])) {
				mask |= LOCK;
			} else if(SecurityConstants.ACTION_STATISTICS.equals(actions[i])) {
				mask |= STATS;
			}
		}
		return mask;
	}
}
