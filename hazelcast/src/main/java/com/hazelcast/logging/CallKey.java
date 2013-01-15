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

package com.hazelcast.logging;

import com.hazelcast.nio.Address;

public class CallKey {
    final Address callerAddress;
    final int callerThreadId;

    public CallKey(Address callerAddress, int callerThreadId) {
        this.callerAddress = callerAddress;
        this.callerThreadId = callerThreadId;
    }

    public Address getCallerAddress() {
        return callerAddress;
    }

    public int getCallerThreadId() {
        return callerThreadId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CallKey that = (CallKey) o;
        if (callerThreadId != that.callerThreadId) return false;
        if (!callerAddress.equals(that.callerAddress)) return false;
        return true;
    }

    @Override
    public int hashCode() {
        int result = callerAddress.hashCode();
        result = 31 * result + callerThreadId;
        return result;
    }

    @Override
    public String toString() {
        return "CallKey{" +
                "callerAddress=" + callerAddress +
                ", callerThreadId=" + callerThreadId +
                '}';
    }
}
