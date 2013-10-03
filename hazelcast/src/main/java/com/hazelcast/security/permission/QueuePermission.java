package com.hazelcast.security.permission;

import static com.hazelcast.security.SecurityConstants.*;

public class QueuePermission extends InstancePermission {
	
	private final static int OFFER 			= 0x4;
	private final static int POLL 			= 0x8;
	private final static int REMOVE			= 0x16;
	private final static int GET 			= 0x32;
	private final static int LISTEN 		= 0x64;
	private final static int STATS 			= 0x128; 
	private final static int ALL 			= OFFER | POLL | REMOVE | GET | CREATE | DESTROY | LISTEN | STATS;

	public QueuePermission(String name, String... actions) {
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
			} else if(SecurityConstants.ACTION_OFFER.equals(actions[i])) {
				mask |= OFFER;
			} else if(SecurityConstants.ACTION_GET.equals(actions[i])) {
				mask |= GET;
			} else if(SecurityConstants.ACTION_REMOVE.equals(actions[i])) {
				mask |= REMOVE;
			} else if(SecurityConstants.ACTION_POLL.equals(actions[i])) {
				mask |= POLL;
			} else if(SecurityConstants.ACTION_DESTROY.equals(actions[i])) {
				mask |= DESTROY;
			} else if(SecurityConstants.ACTION_LISTEN.equals(actions[i])) {
				mask |= LISTEN;
			} else if(SecurityConstants.ACTION_STATISTICS.equals(actions[i])) {
				mask |= STATS;
			}
		}
		return mask;
	}
}
