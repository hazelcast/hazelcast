/*
 * Copyright (c) 2008-2012, Hazel Bilisim Ltd. All Rights Reserved.
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

public class CallKey {
    final Address remoteCallerAddress;
    final int callerThreadId;

    public CallKey(Address remoteCallerAddress, int callerThreadId) {
        this.remoteCallerAddress = remoteCallerAddress;
        this.callerThreadId = callerThreadId;
    }

    public Address getRemoteCallerAddress() {
        return remoteCallerAddress;
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
        if (!remoteCallerAddress.equals(that.remoteCallerAddress)) return false;
        return true;
    }

    @Override
    public int hashCode() {
        int result = remoteCallerAddress.hashCode();
        result = 31 * result + callerThreadId;
        return result;
    }
}
