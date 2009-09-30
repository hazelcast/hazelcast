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
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;


public class OutRunnable extends NetworkRunnable implements Runnable{
	PacketWriter writer = new PacketWriter();
	BlockingQueue<Call> queue = new LinkedBlockingQueue<Call>();
	
	public OutRunnable(final PacketWriter writer) {
		this.writer = writer;
	}
	
	public void run() {
		while(true){
			Connection connection = null;
			Call c = null;
			try{
				try {
					c = queue.take();
					callMap.put(c.getId(), c);
					connection = connectionManager.getConnection();
					writer.write(connection,c.getRequest());
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			} catch (IOException io) {
				io.printStackTrace();
				connectionManager.destroyConnection(connection);
				continue;
			}
			
		}
		
	}
	
	public void enQueue(Call call){
		try {
			queue.put(call);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	public void setPacketWriter(PacketWriter writer) {
		this.writer = writer;
		
	}
	public void redoWaitingCalls() {
		Collection<Call> cc = callMap.values();
		List<Call> waitingCalls = new ArrayList<Call>();
		waitingCalls.addAll(cc);
		for (Iterator<Call> iterator = waitingCalls.iterator(); iterator.hasNext();) {
			Call call =  iterator.next();
			redo(call);
		}
	}

	private void redo(Call call) {
		callMap.remove(call.getId());
		enQueue(call);
	}


}
