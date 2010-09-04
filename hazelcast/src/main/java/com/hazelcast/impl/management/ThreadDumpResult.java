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

package com.hazelcast.impl.management;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.hazelcast.core.Member;
import com.hazelcast.impl.MemberImpl;
import com.hazelcast.nio.DataSerializable;

public class ThreadDumpResult implements DataSerializable {
	
	private static final long serialVersionUID = -3773059443581186911L;
	
	private Member member;
	private String result;
	
	// for serialization
	public ThreadDumpResult() {
		super();
	}
	
	public ThreadDumpResult(Member member, String result) {
		super();
		this.member = member;
		this.result = result;
	}

	public Member getMember() {
		return member;
	}

	public String getResult() {
		return result;
	}

	public void writeData(DataOutput out) throws IOException {
		member.writeData(out);
		out.writeUTF(result);
	}

	public void readData(DataInput in) throws IOException {
		member = new MemberImpl();
		member.readData(in);
		result = in.readUTF();
	}
}
