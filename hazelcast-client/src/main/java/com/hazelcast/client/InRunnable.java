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


public class InRunnable extends NetworkRunnable implements Runnable{
	PacketReader reader;

	public void run() {
		int counter=0;
		while(true){
			counter++;
			System.out.println("Counter:" + counter);
			Packet response=null;
			try {
				response = reader.readPacket();
			} catch (IOException e) {
				e.printStackTrace();
				return;
			}
			if(response!=null)
				System.out.println("Packet > " + response.getOperation());
			Call c = callMap.get(response.getCallId());
			if(c!=null){
				synchronized (c) {
					c.setResponse(response);
					c.notify();
				}
			}
		}
	}

	public void setPacketReader(PacketReader reader) {
		this.reader = reader;
		
	}

}
