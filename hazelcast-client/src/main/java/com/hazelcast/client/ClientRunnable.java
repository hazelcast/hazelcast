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

package com.hazelcast.client;

public abstract class ClientRunnable implements Runnable{
	protected volatile boolean running = true;
	protected final Object monitor = new Object(); 
	
	
	protected abstract void customRun() throws InterruptedException;
	
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

	public void shutdown(){
		synchronized (monitor) {
			if(running){
				this.running = false;
				try {
					monitor.wait();
				} catch (InterruptedException ignored) {
				}
			}
		}
	}
	
	protected void notifyMonitor() {
		synchronized (monitor) {
			monitor.notifyAll();
		}
	}
}
