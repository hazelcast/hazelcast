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
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import javax.servlet.ServletException;

import org.apache.catalina.connector.Request;
import org.apache.catalina.connector.Response;
import org.apache.catalina.valves.ValveBase;

/**
 * @author ali
 *
 */

public class HazelValve extends ValveBase implements HazelConstants {
	
	public static ThreadLocal<Long> requestLocal = new ThreadLocal<Long>();
	
	private static AtomicLong lastRequestId = new AtomicLong(0);

	@Override
	public void invoke(Request request, Response response) throws IOException, ServletException {
		try{
			long requestId = lastRequestId.addAndGet(1);
			requestLocal.set(requestId);
			getNext().invoke(request, response);
			HazelSessionFacade ses =  (HazelSessionFacade)request.getSession();
			List<HazelAttribute> touchedList = ses.getTouchedAttributes(requestId);
			for (HazelAttribute hattribute : touchedList) {
				hazelAttributes.put(hattribute.getKey(), hattribute);
			}
		}
		finally{
			requestLocal.remove();
		}
	}

}
