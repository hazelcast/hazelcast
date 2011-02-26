package com.hazelcast.impl.management;

public final class ConsoleRequestConstants {

	public static final byte STATE_OUT_OF_MEMORY = 0;
	public static final byte STATE_ACTIVE = 1;
	
	public static final int REQUEST_TYPE_CLUSTER_STATE = 0;
	public static final int REQUEST_TYPE_GET_THREAD_DUMP = 1;
	public static final int REQUEST_TYPE_EXECUTE_SCRIPT = 2;
	public static final int REQUEST_TYPE_EVICT_LOCAL_MAP = 3;
	
	
	public static final String SCRIPT_JAVASCRIPT = "javascript";
	public static final String SCRIPT_GROOVY = "groovy";
	public static final String SCRIPT_JRUBY = "jruby";
	public static final String SCRIPT_BEANSHELL = "beanshell";

	private ConsoleRequestConstants() {
	}
}
