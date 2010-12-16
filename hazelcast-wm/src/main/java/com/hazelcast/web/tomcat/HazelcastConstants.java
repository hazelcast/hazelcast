package com.hazelcast.web.tomcat;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.IMap;

/**
 * @author ali
 *
 */

public interface HazelcastConstants {
	
	public static final IMap hazelAttributes = Hazelcast.getMap("__hz_ses_attrs");
	
	public static final String HAZEL_SESSION_MARK = "__hz_ses_mark";
	
	public static final String HAZEL_MARK_EXCEPTION = "'"+HAZEL_SESSION_MARK + "' is a reserved key for Hazelcast!";

}
