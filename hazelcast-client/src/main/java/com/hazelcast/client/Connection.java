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

import java.io.*;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.UnknownHostException;

import javax.net.SocketFactory;

/**
 * Holds the socket to one of the members of Hazelcast Clustor.
 * 
 * 
 * @author fuad-malikov
 *
 */
public class Connection {
	private static final int BUFFER_SIZE = 1*1024;
	private Socket socket;
	private InetSocketAddress address;
	private int id = -1;
	private DataOutputStream dos;
    private DataInputStream dis;

    /**
	 * Creates the Socket to the given host and port
	 * 
	 * @param host ip address of the host
	 * @param port port of the host
	 * @throws UnknownHostException
	 * @throws IOException
	 */
	public Connection(String host, int port,int id){
		try {
			setSocket(SocketFactory.getDefault().createSocket(host, port));
			socket.setKeepAlive(true);
			dos = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream(),BUFFER_SIZE));
            dis = new DataInputStream(new BufferedInputStream(socket.getInputStream(), BUFFER_SIZE));
			this.id = id;
		} catch (UnknownHostException e) {
			e.printStackTrace();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}	
	}

	public Connection(InetSocketAddress address,int version) {
		this(address.getAddress().getHostAddress(), address.getPort(),version);
		this.address = address;
	}

	public void setSocket(Socket socket) {
		this.socket = socket;
	}

	public Socket getSocket() {
		return socket;
	}
	
	public InetSocketAddress getAddress() {
		return address;
	}

	public void setVersion(int version) {
		this.id = version;
	}

	public int getVersion() {
		return id;
	}
	@Override
	public String toString() {
		
		return "Connection ["+id+"]" + " ["+address+"]"; 
	}

	public DataOutputStream getOutputStream() {
		return dos;
    }

    public DataInputStream getInputStream(){
        return dis;
    }
}
