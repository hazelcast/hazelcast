/*
 * Copyright (c) 2007-2008, Hazel Ltd. All Rights Reserved.
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

package com.hazelcast.client;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public abstract class IORunnable extends ClientRunnable{

	protected Map<Long, Call> callMap;
	protected HazelcastClient client;
	

	public IORunnable(HazelcastClient client, Map<Long,Call> calls) {
		this.client = client;
		this.callMap = calls;
	}
	public void setCallMap(Map<Long, Call> calls) {
		this.callMap = calls;
	}
	public void interruptWaitingCalls() {
		Collection<Call> cc = callMap.values();
		List<Call> waitingCalls = new ArrayList<Call>();
		waitingCalls.addAll(cc);
		for (Iterator<Call> iterator = waitingCalls.iterator(); iterator.hasNext();) {
			Call call =  iterator.next();
			synchronized (call) {
				call.setException(new RuntimeException("No cluster member available to connect"));
				call.notify();
			}
		}
	}
	
	public void run() {
		while(running){
			try {
				customRun();
			} catch (InterruptedException e) {
				return;
			}
		}
		notifyMonitor();
	}

}
