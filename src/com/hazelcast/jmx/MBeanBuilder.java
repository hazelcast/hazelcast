/* 
 * Copyright (c) 2007-2009, Hazel Ltd. All Rights Reserved.
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
package com.hazelcast.jmx;

import java.util.regex.Pattern;

import javax.management.DynamicMBean;
import javax.management.ObjectName;

import com.hazelcast.core.ICommon;
import com.hazelcast.core.IList;
import com.hazelcast.core.ILock;
import com.hazelcast.core.IMap;
import com.hazelcast.core.IQueue;
import com.hazelcast.core.ISet;
import com.hazelcast.core.ITopic;
import com.hazelcast.core.MultiMap;

public class MBeanBuilder {

	/**
	 * MBean name domain
	 */
	static String NAME_DOMAIN = "com.hazelcast:";
	static ObjectName buildObjectName(String... parts) throws Exception {
		if ((parts.length % 2) != 0) {
			throw new IllegalArgumentException("Name requires a sequence of pair");
		}
		StringBuffer sb = new StringBuffer(NAME_DOMAIN);
		for (int i = 0; i < parts.length; i=i+2) {
			sb.append(parts[i]).append('=');
			if (Pattern.matches(":\",=\\*\\?", parts[i+1])) {
				sb.append(ObjectName.quote(parts[i+1]));
			}
			else {
				sb.append(parts[i+1]);
			}
			if (i+2 < parts.length) {
				sb.append(',');
			}
		}
		return new ObjectName(sb.toString());
	}
	
	@SuppressWarnings("unchecked")
	public static ObjectName buildName(Object object) throws Exception {
		if (object instanceof ITopic) {
			// Topic
			ITopic topic = (ITopic)object;
			String type = topic.getInstanceType().toString().toLowerCase();
			return buildObjectName("type", type,
					type, topic.getName());
		}
		if (object instanceof IQueue) {
			// Queue
			IQueue queue = (IQueue)object;
			String type = queue.getInstanceType().toString().toLowerCase();
			return buildObjectName("type", type,
					type, queue.getName());
		}
		if (object instanceof IList) {
			// List
			IList list = (IList)object;
			String type = list.getInstanceType().toString().toLowerCase();
			return buildObjectName("type", type,
					type, list.getName());
		}
		if (object instanceof ISet) {
			// Set
			ISet set = (ISet)object;
			String type = set.getInstanceType().toString().toLowerCase();
			return buildObjectName("type", type,
					type, set.getName());
		}
		if (object instanceof MultiMap) {
			// MultiMap
			MultiMap map = (MultiMap)object;
			String type = map.getInstanceType().toString().toLowerCase();
			return buildObjectName("type", type,
					type, map.getName());
		}
		if (object instanceof IMap) {
			// Map
			IMap map = (IMap)object;
			String type = map.getInstanceType().toString().toLowerCase();
			return buildObjectName("type", type,
					type, map.getName());
		}
		if (object instanceof ILock) {
			// Lock
			ILock lock = (ILock)object;
			String type = lock.getInstanceType().toString().toLowerCase();
			return buildObjectName("type", type,
					type, lock.toString());
		}
		
		return null;
	}
	
	/**
	 * Return the instrumentation wrapper to a instance.
	 * See http://java.sun.com/javase/technologies/core/mntr-mgmt/javamanagement/best-practices.jsp
	 * 
	 * @param instance
	 * @return
	 * @throws Exception
	 */
	@SuppressWarnings("unchecked")
	public static DynamicMBean buildMBean(ICommon instance) throws Exception {
		if (instance instanceof ITopic) {
	        // Topic
			TopicMBean mbean = new TopicMBean((ITopic)instance);
			return mbean;
		}
		if (instance instanceof IQueue) {
			// Queue
			QueueMBean mbean = new QueueMBean((IQueue)instance);
	    	return mbean;
		}
		if (instance instanceof IList) {
			// List
			ListMBean mbean = new ListMBean((IList)instance);
	    	return mbean;
		}
		if (instance instanceof ISet) {
			// Set
			SetMBean mbean = new SetMBean((ISet)instance);
	    	return mbean;
		}
		if (instance instanceof MultiMap) {
			// Map
			MultiMapMBean mbean = new MultiMapMBean((MultiMap)instance);
			return mbean;
		}
		if (instance instanceof IMap) {
			// Map
			MapMBean mbean = new MapMBean((IMap)instance);
			return mbean;
		}
		if (instance instanceof ILock) {
			// Lock
			LockMBean mbean = new LockMBean((ILock)instance);
			return mbean;
		}
		
		return null;
	}

	
}
