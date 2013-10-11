package com.hazelcast.security.permission;

public class TopicPermission extends InstancePermission {
	
	private final static int PUBLISH 		= 0x4;
	private final static int LISTEN 		= 0x8;
	private final static int ALL 			= CREATE | DESTROY | LISTEN | PUBLISH ;

	public TopicPermission(String name, String... actions) {
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
			} else if(ActionConstants.ACTION_PUBLISH.equals(actions[i])) {
				mask |= PUBLISH;
			} else if(ActionConstants.ACTION_DESTROY.equals(actions[i])) {
				mask |= DESTROY;
			} else if(ActionConstants.ACTION_LISTEN.equals(actions[i])) {
				mask |= LISTEN;
			}
		}
		return mask;
	}
}
