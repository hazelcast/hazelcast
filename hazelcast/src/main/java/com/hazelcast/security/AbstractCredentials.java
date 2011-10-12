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

package com.hazelcast.security;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.hazelcast.nio.DataSerializable;

/**
 * Absract implementation of {@link Credentials}
 */
public abstract class AbstractCredentials implements Credentials, DataSerializable {

	private static final long serialVersionUID = 3587995040707072446L;

	private transient String endpoint;
	private String principal;

	public AbstractCredentials() {
	}
	
	public AbstractCredentials(String principal) {
		super();
		this.principal = principal;
	}

	public final String getEndpoint() {
		return endpoint;
	}

	public final void setEndpoint(String endpoint) {
		this.endpoint = endpoint;
	}
	
	public String getPrincipal() {
		return principal;
	}
	
	public void setPrincipal(String principal) {
		this.principal = principal;
	}
	
	public final void writeData(DataOutput out) throws IOException {
		out.writeUTF(principal);
		writeDataInternal(out);
	}
	
	public final void readData(DataInput in) throws IOException {
		principal = in.readUTF();
		readDataInternal(in);
	}
	
	protected abstract void writeDataInternal(DataOutput out) throws IOException ;
	
	protected abstract void readDataInternal(DataInput in) throws IOException ;
}
