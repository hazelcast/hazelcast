package com.hazelcast.client;

import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

import com.hazelcast.client.cluster.Bind;
import com.hazelcast.client.impl.ClusterOperation;
import com.hazelcast.client.nio.Address;

public class ConnectionManager{
	private volatile Connection currentConnection;
	private volatile AtomicInteger connectionIdGenerator = new AtomicInteger(-1);
	private OutRunnable out;
	private List<InetSocketAddress> clusterMembers = new ArrayList<InetSocketAddress>();
	Logger logger = Logger.getLogger(getClass().toString());
	

	public ConnectionManager(InetSocketAddress[] clusterMembers) {
		this.clusterMembers.addAll(Arrays.asList(clusterMembers));
		Collections.shuffle(this.clusterMembers);
	}
	
	public void setOutRunnable(OutRunnable out){
		this.out = out;
	}
	
	public Connection getConnection(){
		if(currentConnection == null){
			synchronized (ConnectionManager.class) {
				if(currentConnection == null){
					currentConnection = searchForAvailableConnection();
					if(currentConnection!=null){
						logger.info("Connection to " + currentConnection);
						bind(currentConnection);
						out.redoWaitingCalls();
					}
				}
			}
		}
		
		return currentConnection;
	}
	public synchronized void destroyConnection(Connection connection){
		if(currentConnection!=null && currentConnection.getVersion()== connection.getVersion()){
			logger.warning("Connection to " + currentConnection +" is lost");
			currentConnection = null;
		}
	}	
	private void bind(Connection connection) {
		Bind b = null;
		try {
			b = new Bind(new Address(connection.getAddress().getHostName(),connection.getSocket().getLocalPort()));
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
		Packet bind = new Packet();
		bind.set("remotelyProcess", ClusterOperation.REMOTELY_PROCESS, Serializer.toByte(null), Serializer.toByte(b));
		Call cBind = new Call();
		cBind.setRequest(bind);
		out.enQueue(cBind);
	}

	private void popAndPush(List<InetSocketAddress> clusterMembers) {
		InetSocketAddress address =clusterMembers.remove(0); 
		clusterMembers.add(address);
	}

	private Connection searchForAvailableConnection() {
		Connection connection =null;
		popAndPush(clusterMembers);
		int counter = clusterMembers.size();
		while(counter>0){
			try{
				connection = getNextConnection();
				break;
			}catch(Exception e){
				popAndPush(clusterMembers);
				counter--;
			}
		}
//		if(counter == 0){
//			throw new RuntimeException("No cluster member available to connect");
//		}
		return connection;
	}

	private Connection getNextConnection(){
		InetSocketAddress address = clusterMembers.get(0);
		Connection connection  = new Connection(address,connectionIdGenerator.incrementAndGet());
		return connection;
	}
	
}
