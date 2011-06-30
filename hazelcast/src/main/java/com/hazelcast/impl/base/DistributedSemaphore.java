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

import java.util.HashMap;
import java.util.Map;

public class DistributedSemaphore {

    Map<Address, Integer> lockAddresses = new HashMap<Address, Integer>();
    int permits = 1;

    /**
     * Constructor
     */
    public DistributedSemaphore() {
    }

    /**
     * Constructor
     *
     * @param permits
     */
    public DistributedSemaphore(int permits) {
        this.permits = permits;
    }

    /**
     * Constructor
     *
     * @param copy
     */
    public DistributedSemaphore(DistributedSemaphore copy) {
        this.lockAddresses = copy.lockAddresses;
        this.permits = copy.permits;
    }

    public boolean isLocked() {
        return permits > 0;
    }

    public boolean hasPermits(Address address) {
        return this.lockAddresses.containsKey(address);
    }

    public boolean acquire(Address address) {
        if (this.permits > 0) {
            int acquiredPermits = addressPermits(address);
            this.lockAddresses.put(address, acquiredPermits + 1);
            this.permits--;
            return true;
        }
        return false;
    }

    public boolean release(Address address) {
        if (hasPermits(address)) {
            this.permits++;
            int addressPermits = addressPermits(address);
            addressPermits--;
            if (addressPermits > 0)
                this.lockAddresses.put(address, addressPermits);
            else this.lockAddresses.remove(address);
            return true;
        }
        return false;
    }

    public void reducePermits(int permits) {
        this.permits -= permits;
    }

    public boolean releaseAll(Address address) {
        if (hasPermits(address)) {
            int addressPermits = addressPermits(address);
            this.permits += addressPermits;
            this.lockAddresses.remove(address);
            return true;
        }
        return false;
    }

    /**
     * Returns the number of permits an address has acquired.
     *
     * @param address
     * @return
     */
    protected int addressPermits(Address address) {
        int result = 0;
        Integer acquiredPermits = this.lockAddresses.get(address);
        if (acquiredPermits != null) {
            result = acquiredPermits;
        }
        return result;
    }

    public boolean testAcquire(Address address) {
        return permits > 0;
    }

    public void clear() {
        permits = 0;
        lockAddresses = null;
    }

    public Map<Address, Integer> getLockAddresses() {
        return lockAddresses;
    }

    public int getPermits() {
        return permits;
    }

    @Override
    public String toString() {
        return "Semahpore{" +
                "lockAddresses=" + lockAddresses +
                ", permits=" + permits +
                '}';
    }
}
