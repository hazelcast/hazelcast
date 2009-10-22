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
import java.util.Map;
import java.util.logging.Logger;

import com.hazelcast.client.impl.ClusterOperation;


public class InRunnable extends IORunnable implements Runnable{
	final PacketReader reader;
	Logger logger = Logger.getLogger(this.getClass().getName());
	public InRunnable(HazelcastClient client, Map<Long,Call> calls, PacketReader reader) {
		super(client,calls);
		this.reader = reader;
	}

	protected void customRun() {
		Connection connection = null;
		Packet packet=null;
		try {
			connection = client.connectionManager.getConnection();
			if(connection == null){
				interruptWaitingCalls();
			}else{
				packet = reader.readPacket(connection);
				Call c = null;
				c = callMap.remove(packet.getCallId());
				if(c!=null){
					synchronized (c) {
						c.setResponse(packet);
						c.notify();
					}
				} else {
					if(packet.getOperation().equals(ClusterOperation.EVENT)){
						client.listenerManager.enqueue(packet);
					}
					else{
						throw new RuntimeException("In Thread can not handle: "+packet.getOperation() + " : " +packet.getCallId() );
					}
				}
			}
		
		} catch (IOException e) {
			client.connectionManager.destroyConnection(connection);
		}
	}
	public void shutdown(){
		synchronized (monitor) {
			if(running){
				this.running = false;
				try {
					Connection connection = client.connectionManager.getConnection();
					if(connection!=null){
						connection.getSocket().close();
					}
				} catch (IOException ignored) {}
		
				try {
					monitor.wait();
				} catch (InterruptedException ignored) {
				}
			}
		}
	}
}
