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

package com.hazelcast.nio;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.StringTokenizer;

public class Address implements DataSerializable {

	private String host = null;

	private byte[] ip;

	private int port = -1;

	private transient InetAddress inetAddress;

	private int hash = -1;

	private boolean dead = false;

	private boolean thisAddress = false;

	public Address() {
		this.ip = new byte[4];
	}

	public Address(InetAddress inetAddress, int port) {
		this.ip = inetAddress.getAddress();
		this.port = port;
		this.inetAddress = inetAddress;
	}

	public Address(Address address) {
		ip = new byte[4];
		System.arraycopy(address.getIP(), 0, ip, 0, 4);
		port = address.getPort();
	}

	public Address(String address, int port) throws UnknownHostException {
		this(address, port, false);
	}

	public Address(String address, int port, boolean ipAddress) throws UnknownHostException {
		this.port = port;
		if (!ipAddress) {
			this.ip = InetAddress.getByName(address).getAddress();
		} else {
			ip = new byte[4];
			StringTokenizer stringTokenizer = new StringTokenizer(address, ".");
			int index = 0;
			while (stringTokenizer.hasMoreTokens()) {
				String token = stringTokenizer.nextToken();
				int addressByte = Integer.parseInt(token);
				ip[index++] = (byte) addressByte;
			}
		}
	}

	public boolean isThisAddress() {
		return thisAddress;
	}

	public void setThisAddress(boolean thisAddress) {
		this.thisAddress = thisAddress;
	}

	public Address(byte[] ip, int port) {
		this.ip = ip;
		this.port = port;
	}

	public static String toString(byte[] ip) {
		return (ip[0] & 0xff) + "." + (ip[1] & 0xff) + "." + (ip[2] & 0xff) + "." + (ip[3] & 0xff);
	}

	private void setHost() {
		this.host = toString(ip);
	}

	public void writeData(DataOutput out) throws IOException {
		out.write(ip);
		out.writeInt(port);
	}

	public void readData(DataInput in) throws IOException {
		in.readFully(ip);
		port = in.readInt();
		// setHost();
	}

	public void readObject(ByteBuffer buffer) {
		buffer.get(ip);
		port = buffer.getInt();

	}

	public void writeObject(ByteBuffer buffer) {
		buffer.put(ip);
		buffer.putInt(port);
	}

	public String getHost() {
		if (host == null)
			setHost();
		return host;
	}

	@Override
	public String toString() {
		if (host == null)
			setHost();
		return "Address[" + host + ":" + port + "]";
	}

	public int getPort() {
		return port;
	}

	public String addressToString() {
		if (host == null)
			setHost();
		return host;
	}

	public InetAddress getInetAddress() throws UnknownHostException {
		if (host == null)
			setHost();
		if (inetAddress == null) {
			inetAddress = InetAddress.getByName(host);
		}
		return inetAddress;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o)
			return true;
		if (o == null)
			return false;
		if (!(o instanceof Address))
			return false;

		final Address address = (Address) o;

		if (port != address.port)
			return false;
		if (!Arrays.equals(ip, address.ip))
			return false;

		return true;
	}

	@Override
	public int hashCode() {
		if (hash == -1)
			setHashCode();
		return hash;
	}

	private void setHashCode() {
		this.hash = hash(ip) * 29 + port;
	}

	private int hash(byte[] id) {
		int hash = 0;
		for (int i = 0; i < id.length; i++) {
			hash = (hash * 29) + id[i];
		}
		return hash;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public byte[] getIP() {
		return ip;
	}

	public void setDead() {
		this.dead = true;
	}

	public boolean isDead() {
		return dead;
	}  

}
