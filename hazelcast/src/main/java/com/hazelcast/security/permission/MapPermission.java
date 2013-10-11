package com.hazelcast.security.permission;

public class MapPermission extends InstancePermission {
	
	private final static int PUT 			= 0x4;
	private final static int REMOVE 		= 0x8;
	private final static int READ 			= 0x16;
	private final static int LISTEN 		= 0x32;
	private final static int LOCK	 		= 0x64;
	private final static int INDEX	 		= 0x128;
    private final static int INTERCEPT	 	= 0x256;
	private final static int ALL 			= CREATE | DESTROY | PUT | REMOVE | READ | LISTEN | LOCK | INDEX | INTERCEPT;

	public MapPermission(String name, String... actions) {
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
			} else if(ActionConstants.ACTION_PUT.equals(actions[i])) {
				mask |= PUT;
			} else if(ActionConstants.ACTION_REMOVE.equals(actions[i])) {
				mask |= REMOVE;
            } else if(ActionConstants.ACTION_READ.equals(actions[i])) {
                mask |= READ;
			} else if(ActionConstants.ACTION_LISTEN.equals(actions[i])) {
				mask |= LISTEN;
			} else if(ActionConstants.ACTION_LOCK.equals(actions[i])) {
				mask |= LOCK;
            } else if(ActionConstants.ACTION_INDEX.equals(actions[i])) {
                mask |= INDEX;
            } else if(ActionConstants.ACTION_INTERCEPT.equals(actions[i])) {
                mask |= INTERCEPT;
			}
		}
		return mask;
	}

}
