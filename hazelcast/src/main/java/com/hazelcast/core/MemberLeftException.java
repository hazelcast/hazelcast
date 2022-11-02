/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.cluster.Member;
import com.hazelcast.cluster.impl.MemberImpl;
import com.hazelcast.internal.util.UUIDSerializationUtil;
import com.hazelcast.cluster.Address;
import com.hazelcast.spi.exception.RetryableException;
import com.hazelcast.version.MemberVersion;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

/**
 * A {@link ExecutionException} thrown when a member left during an invocation or execution.
 */
public class MemberLeftException extends ExecutionException implements RetryableException, IndeterminateOperationState {

    private transient Member member;

    public MemberLeftException() {
    }


    public MemberLeftException(String message) {
        super(message);
    }

    public MemberLeftException(Member member) {
        super(member + " has left cluster!");
        this.member = member;
    }

    public MemberLeftException(Throwable cause) {
        super(cause);
    }

    /**
     * Returns the member that left the cluster
     * @return the member that left the cluster
     */
    public Member getMember() {
        return member;
    }

    private void writeObject(ObjectOutputStream out) throws IOException {
        out.defaultWriteObject();

        Address address = member.getAddress();
        String host = address.getHost();
        int port = address.getPort();

        UUIDSerializationUtil.writeUUID(out, member.getUuid());
        out.writeUTF(host);
        out.writeInt(port);
        out.writeBoolean(member.isLiteMember());
        out.writeObject(member.getVersion());
    }

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();

        UUID uuid = UUIDSerializationUtil.readUUID(in);
        String host = in.readUTF();
        int port = in.readInt();
        boolean liteMember = in.readBoolean();
        MemberVersion version = (MemberVersion) in.readObject();

        member = new MemberImpl.Builder(new Address(host, port))
                .version(version)
                .uuid(uuid)
                .liteMember(liteMember)
                .build();
    }
}
