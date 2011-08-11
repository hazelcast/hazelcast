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

package com.hazelcast.impl.base;

import com.hazelcast.nio.Address;
import com.hazelcast.nio.Data;
import com.hazelcast.nio.DataSerializable;
import com.hazelcast.nio.IOUtil;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class DistributedCountDownLatch implements DataSerializable  {
    public final static Data newInstanceData = IOUtil.toData(new DistributedCountDownLatch());

    int count;
    Address memberAddress = new Address();
    Address ownerAddress = new Address();

    public DistributedCountDownLatch() {
    }

    public void readData(DataInput in) throws IOException {
        count = in.readInt();
        if (count > 0) {
            memberAddress.readData(in);
            ownerAddress.readData(in);
        }
    }

    public void writeData(DataOutput out) throws IOException {
        out.writeInt(count);
        if (count > 0) {
            memberAddress.writeData(out);
            ownerAddress.writeData(out);
        }
    }

    public boolean countDown() {
        return count > 0 && count-- > 0;
    }

    public int getCount() {
        return count;
    }

    public Address getOwnerAddress() {
        return (count > 0) ? ownerAddress : null;
    }

    public boolean isOwnerOrMemberAddress(Address deadAddress) {
        return deadAddress.equals(ownerAddress) || deadAddress.equals(memberAddress);
    }

    public boolean setCount(int count, Address memberAddress, Address ownerAddress) {
        if (this.count != 0 || count == 0) {
            return false;
        }
        this.count = count;
        this.memberAddress = memberAddress;
        this.ownerAddress = ownerAddress;
        return true;
    }

    @Override
    public String toString() {
        return String.format("CountDownLatch{count=%d, memberAddress=%s, ownerAddress=%s}", count, memberAddress, ownerAddress);
    }
}
