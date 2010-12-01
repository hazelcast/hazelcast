/* 
 * Copyright (c) 2008-2010, Hazel Ltd. All Rights Reserved.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at 
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.hazelcast.web.tomcat;

import org.apache.catalina.Session;
import org.apache.catalina.session.StandardManager;
import org.apache.catalina.session.StandardSession;
import org.apache.juli.logging.Log;
import org.apache.juli.logging.LogFactory;

/**
 * @author ali
 *
 */

public class HazelManager extends StandardManager {
	
	private final Log log = LogFactory.getLog(HazelManager.class); // must not be static
	
    /**
     * The descriptive information string for this implementation.
     */
	private static final String info = "HazelManager/1.0";
	
    /**
     * The descriptive name of this Manager implementation (for logging).
     */
    protected static String name = "HazelManager";
	

    /**
     * Get new session class to be used in the doLoad() method.
     */
	protected StandardSession getNewSession() {
		return new HazelSession(this);
	}
	
	@Override
	public void remove(Session session) {
		
		super.remove(session);
	}
	
	//TODO implement new session generator
	@Override
	protected synchronized String generateSessionId() {
		return super.generateSessionId();
	}
	
}
