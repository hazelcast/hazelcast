package com.hazelcast.security.permission;

public class ListPermission extends InstancePermission {
	
	private final static int ADD 			= 0x4;
	private final static int SET 			= 0x8;
	private final static int REMOVE			= 0x16;
	private final static int GET 			= 0x32;
	private final static int LISTEN 		= 0x64;
	private final static int ALL 			= ADD | SET | REMOVE | GET | CREATE | DESTROY | LISTEN ;

	public ListPermission(String name, String... actions) {
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
			} else if(ActionConstants.ACTION_ADD.equals(actions[i])) {
				mask |= ADD;
			} else if(ActionConstants.ACTION_GET.equals(actions[i])) {
				mask |= GET;
			} else if(ActionConstants.ACTION_REMOVE.equals(actions[i])) {
				mask |= REMOVE;
			} else if(ActionConstants.ACTION_SET.equals(actions[i])) {
				mask |= SET;
			} else if(ActionConstants.ACTION_DESTROY.equals(actions[i])) {
				mask |= DESTROY;
			} else if(ActionConstants.ACTION_LISTEN.equals(actions[i])) {
				mask |= LISTEN;
			}
		}
		return mask;
	}
}
