package com.hazelcast.client;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

import com.hazelcast.client.cluster.Bind;
import com.hazelcast.impl.ClusterOperation;
import com.hazelcast.client.nio.Address;

public class ConnectionManager{
	private volatile Connection currentConnection;
	private final AtomicInteger connectionIdGenerator = new AtomicInteger(-1);
	private final List<InetSocketAddress> clusterMembers = new ArrayList<InetSocketAddress>();
	private final Logger logger = Logger.getLogger(getClass().toString());
	private final HazelcastClient client;
	

	public ConnectionManager(HazelcastClient client, InetSocketAddress[] clusterMembers) {
		this.client = client;
		this.clusterMembers.addAll(Arrays.asList(clusterMembers));
		Collections.shuffle(this.clusterMembers);
	}
	
	public Connection getConnection() throws IOException{
		Connection connection = null;
		if(currentConnection == null){
			synchronized (this) {
				if(currentConnection == null){
					connection = searchForAvailableConnection();
					if(connection!=null){
						logger.info("Client is connecting to " + connection);
						bind(connection);
						currentConnection = connection;
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
	private void bind(Connection connection) throws IOException {
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
        client.out.callMap.put(cBind.getId(), cBind);
		client.out.writer.write(connection, bind);
		try {
			Thread.sleep(10);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
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
		return connection;
	}

	private Connection getNextConnection(){
		InetSocketAddress address = clusterMembers.get(0);
		Connection connection  = new Connection(address,connectionIdGenerator.incrementAndGet());
		return connection;
	}
	
}
