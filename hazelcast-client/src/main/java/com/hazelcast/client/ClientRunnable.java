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

	public void shutdown() throws InterruptedException{
		synchronized (monitor) {
			if(running){
				this.running = false;
				monitor.wait();
			}
		}
	}
	
	protected void notifyMonitor() {
		synchronized (monitor) {
			monitor.notifyAll();
		}
	}
}
