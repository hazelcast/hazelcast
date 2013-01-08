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

package com.hazelcast.impl.base;

import com.hazelcast.nio.Address;
import com.hazelcast.nio.DataSerializable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class DistributedSemaphore implements DataSerializable {

    Map<Address, Integer> attachedPermits = new HashMap<Address, Integer>();
    int available;

    public DistributedSemaphore() {
    }

    public DistributedSemaphore(int initialPermits) {
        available = initialPermits;
    }

    public void readData(DataInput in) throws IOException {
        Address address;
        attachedPermits.clear();
        available = in.readInt();
        int entries = in.readInt();
        while (entries-- > 0) {
            (address = new Address()).readData(in);
            attachedPermits.put(address, in.readInt());
        }
    }

    public void writeData(DataOutput out) throws IOException {
        out.writeInt(available);
        out.writeInt(attachedPermits.size());
        for (Map.Entry<Address, Integer> entry : attachedPermits.entrySet()) {
            entry.getKey().writeData(out);
            out.writeInt(entry.getValue());
        }
    }

    public void attachDetach(Integer permitsDelta, Address address) {
        if (permitsDelta != 0 && address != null) {
            int newValue = permitsDelta + getAttached(address);
            if (newValue != 0) {
                attachedPermits.put(address, newValue);
            } else {
                attachedPermits.remove(address);
            }
        }
    }

    public int drain() {
        int drained = available;
        available = 0;
        return drained;
    }

    public int getAttached() {
        int total = 0;
        for (Integer permits : attachedPermits.values()) {
            total += permits;
        }
        return total;
    }

    public int getAttached(Address address) {
        return attachedPermits.containsKey(address) ? attachedPermits.get(address) : 0;
    }

    public int getAvailable() {
        return available;
    }

    public void reduce(int permits) {
        available -= permits;
    }

    public void release(int permits, Address address) {
        available += permits;
        int attachDetachDelta = permits * -1;
        attachDetach(attachDetachDelta, address);
    }

    public boolean tryAcquire(int permits, Address address) {
        if (available >= permits) {
            available -= permits;
            attachDetach(permits, address);
            return true;
        }
        return false;
    }

    public boolean onDisconnect(Address deadAddress) {
        Integer attached = attachedPermits.remove(deadAddress);
        if (attached != null && attached != 0) {
            available += attached;
            return true;
        }
        return false;
    }

    @Override
    public String toString() {
        return String.format("Semahpore{available=%d, global attached=%d}", available, getAttached());
    }
}
