package com.hazelcast.security.permission;


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
			if(ActionConstants.ACTION_ALL.equals(actions[i])) {
				return ALL;
			}
			
			if(ActionConstants.ACTION_CREATE.equals(actions[i])) {
				mask |= CREATE;
			} else if(ActionConstants.ACTION_DESTROY.equals(actions[i])) {
				mask |= DESTROY;
			} else if(ActionConstants.ACTION_COUNTDOWN.equals(actions[i])) {
				mask |= COUNTDOWN;
			} else if(ActionConstants.ACTION_STATISTICS.equals(actions[i])) {
				mask |= STATS;
			} else if(ActionConstants.ACTION_SET.equals(actions[i])) {
				mask |= SET;
			}
		}
		return mask;
	}
}
