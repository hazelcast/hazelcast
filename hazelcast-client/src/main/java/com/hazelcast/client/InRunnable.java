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

import com.hazelcast.client.impl.ClusterOperation;


public class InRunnable extends NetworkRunnable implements Runnable{
	final PacketReader reader;
	final HazelcastClient hazelcastClient;
	public InRunnable(HazelcastClient hazelcastClient, PacketReader reader) {
		this.reader = reader;
		this.hazelcastClient = hazelcastClient;
	}

	public void run() {
		int counter=0;
		while(true){
			counter++;
			Packet packet=null;
			try {
				packet = reader.readPacket();
			} catch (IOException e) {
				e.printStackTrace();
				hazelcastClient.attachedClusterMemeberConnectionLost();
				continue;
			}
			Call c = callMap.remove(packet.getCallId());
			if(c!=null){
				synchronized (c) {
					c.setResponse(packet);
					c.notify();
				}
			} else {
				if(packet.getOperation().equals(ClusterOperation.EVENT)){
					hazelcastClient.listenerManager.enqueue(packet);
				}
			}
		}
	}
}
