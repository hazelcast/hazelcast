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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import com.hazelcast.client.impl.ClusterOperation;
import com.hazelcast.client.impl.ListenerManager;


public class InRunnable extends NetworkRunnable implements Runnable{
	final PacketReader reader;
	ListenerManager listenerManager;
	public InRunnable(ListenerManager listenerManager, PacketReader reader) {
		this.reader = reader;
		this.listenerManager = listenerManager;
	}
	public void notifyWaitingCalls() {
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
		while(true){
			Connection connection = null;
			Packet packet=null;
			try {
				connection = connectionManager.getConnection();
				if(connection == null){
					notifyWaitingCalls();
					continue;
				}
				packet = reader.readPacket(connection);
				Call c = null;
				if(packet.isRedoOnDisConnect()){
					c = callMap.get(packet.getCallId());
				}
				else{
					c = callMap.remove(packet.getCallId());
				}
				if(c!=null){
					synchronized (c) {
						c.setResponse(packet);
						c.notify();
					}
				} else {
					if(packet.getOperation().equals(ClusterOperation.EVENT)){
						listenerManager.enqueue(packet);
					}
				}
			
			} catch (IOException e) {
				connectionManager.destroyConnection(connection);
				continue;
			}
		}
	}
}
