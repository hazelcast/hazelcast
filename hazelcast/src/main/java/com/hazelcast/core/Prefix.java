package com.hazelcast.core;

/**
 * Holds the prefix constants used by Hazelcast.
 */
public final class Prefix {
	
	/** The Constant MAP: "c:" */
	public static final String MAP_BASED = "m:";
	
	/** The Constant MAP: "c:" */
	public static final String MAP = "c:";
	
	/** The Constant LIST: "m:l:" */
	public static final String LIST = MAP_BASED + "l:";
	
	/** The Constant SET: "m:s:" */
	public static final String SET = MAP_BASED + "s:";
	
	/** The Constant QUEUE: "q:" */
	public static final String QUEUE = "q:";
	
	/** The Constant TOPIC: "t:" */
	public static final String TOPIC = "t:";
	
	/** The Constant IDGEN: "i:" */
	public static final String IDGEN = "i:";
	
	/** The Constant MULTIMAP: "m:u:" */
	public static final String MULTIMAP = MAP_BASED + "u:";

	/**
	 * Private constructor to avoid instances of the class.
	 */
	private Prefix() {
	}

}
