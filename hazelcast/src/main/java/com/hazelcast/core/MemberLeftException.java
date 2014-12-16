/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.nio.Address;
import com.hazelcast.spi.exception.RetryableException;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetSocketAddress;
import java.util.concurrent.ExecutionException;

/**
 * A {@link ExecutionException} thrown when a member left during an invocation or execution.
 */
public class MemberLeftException extends ExecutionException implements RetryableException {

    private transient Member member;

    public MemberLeftException() {
    }

    public MemberLeftException(Member member) {
        super(member + " has left cluster!");
        this.member = member;
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

        out.writeUTF(member.getUuid());

        boolean isImpl = member instanceof MemberImpl;
        out.writeBoolean(isImpl);

        if (isImpl) {
            MemberImpl memberImpl = (MemberImpl) member;
            Address address = memberImpl.getAddress();
            String host  = address.getHost();
            int port = address.getPort();
            out.writeUTF(host);
            out.writeInt(port);
        } else {
            InetSocketAddress socketAddress = member.getSocketAddress();
            out.writeObject(socketAddress);
        }
    }

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();

        String uuid = in.readUTF();

        boolean isImpl = in.readBoolean();
        if (isImpl) {
            String host = in.readUTF();
            int port = in.readInt();
            member = new MemberImpl(new Address(host, port), false, uuid, null);
        } else {
            InetSocketAddress socketAddress = (InetSocketAddress) in.readObject();
            member = new MemberImpl(new Address(socketAddress), false, uuid, null);
        }
    }
}
