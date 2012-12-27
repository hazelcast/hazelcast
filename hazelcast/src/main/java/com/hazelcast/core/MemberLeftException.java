/*
 * Copyright (c) 2008-2012, Hazelcast, Inc. All Rights Reserved.
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
 */

package com.hazelcast.core;

import com.hazelcast.instance.MemberImpl;
import com.hazelcast.nio.DataSerializable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.concurrent.ExecutionException;

public class MemberLeftException extends ExecutionException implements DataSerializable {
    volatile Member member;

    public MemberLeftException() {
    }

    public MemberLeftException(Member member) {
        this.member = member;
    }

    public Member getMember() {
        return member;
    }

    public String getMessage() {
        return member + " has left cluster!";
    }

    public void writeData(DataOutput out) throws IOException {
        member.writeData(out);
    }

    public void readData(DataInput in) throws IOException {
        Member member = new MemberImpl();
        member.readData(in);
    }
}
