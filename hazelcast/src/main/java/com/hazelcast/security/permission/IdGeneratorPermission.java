package com.hazelcast.security.permission;


public class IdGeneratorPermission extends InstancePermission {
	
	private final static int INCREMENT 		= 0x4;
	private final static int ALL 			= CREATE | DESTROY | INCREMENT ;

	public IdGeneratorPermission(String name, String... actions) {
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
			} else if(ActionConstants.ACTION_INCREMENT.equals(actions[i])) {
				mask |= INCREMENT;
			} else if(ActionConstants.ACTION_DESTROY.equals(actions[i])) {
				mask |= DESTROY;
			}
		}
		return mask;
	}
}
