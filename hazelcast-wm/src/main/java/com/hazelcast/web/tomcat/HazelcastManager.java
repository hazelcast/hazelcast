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

import java.io.IOException;

import org.apache.catalina.Session;
import org.apache.catalina.session.StandardManager;
import org.apache.catalina.session.StandardSession;
import org.apache.juli.logging.Log;
import org.apache.juli.logging.LogFactory;

/**
 * @author ali
 *
 */

public class HazelcastManager extends StandardManager implements HazelcastConstants {
	
	private final Log log = LogFactory.getLog(HazelcastManager.class); // must not be static
	
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
		return new HazelcastSession(this);
	}
	
	@Override
	public Session findSession(String id) throws IOException {
		Session session = super.findSession(id);
		if(session != null){
			return session;
		}
		
		HazelcastAttribute mark = (HazelcastAttribute)hazelAttributes.get(id +"_" + HAZEL_SESSION_MARK);
		if(mark != null && mark.getValue() != null){
			session = createSession(id);
		}
		return session;
	}
	
}
