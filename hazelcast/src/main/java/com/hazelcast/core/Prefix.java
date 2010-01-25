package com.hazelcast.core;

/**
 * Holds the prefix constants used by Hazelcast.
 */
public final class Prefix {
	
	/** The Constant MAP_BASED: "m:" */
	public static final String MAP_BASED = "m:";
	
	/** The Constant MAP: "c:" */
	public static final String MAP = "c:";
	
	/** The Constant AS_LIST: "l:" */
	public static final String AS_LIST = "l:";
	
	/** The Constant LIST: "m:l:" */
	public static final String LIST = MAP_BASED + AS_LIST;
	
	/** The Constant AS_SET: "s:" */
	public static final String AS_SET = "s:";
	
	/** The Constant SET: "m:s:" */
	public static final String SET = MAP_BASED + AS_SET;
	
	/** The Constant QUEUE: "q:" */
	public static final String QUEUE = "q:";
	
	/** The Constant TOPIC: "t:" */
	public static final String TOPIC = "t:";
	
	/** The Constant IDGEN: "i:" */
	public static final String IDGEN = "i:";
	
	/** The Constant AS_MULTIMAP: "u:" */
	public static final String AS_MULTIMAP = "u:";
	
	/** The Constant MULTIMAP: "m:u:" */
	public static final String MULTIMAP = MAP_BASED + AS_MULTIMAP;

	/**
	 * Private constructor to avoid instances of the class.
	 */
	private Prefix() {
	}

}
