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

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class CallStateService {
    private final ConcurrentMap<Long, CallState> mapLocalCallStates = new ConcurrentHashMap<Long, CallState>(100);
    private final ConcurrentMap<RemoteCallKey, CallState> mapRemoteCallStates = new ConcurrentHashMap<RemoteCallKey, CallState>(100);

    public CallState newRemoteCallState(long callId, Address remoteCallerAddress, int callerThreadId) {
        RemoteCallKey remoteCallKey = new RemoteCallKey(remoteCallerAddress, callerThreadId);
        CallState callStateNew = new CallState(callId, remoteCallerAddress, callerThreadId);
        CallState callBefore = mapRemoteCallStates.put(remoteCallKey, callStateNew);
        return callStateNew;
    }

    public CallState getRemoteCallState(Address remoteCallerAddress, int callerThreadId) {
        RemoteCallKey remoteCallKey = new RemoteCallKey(remoteCallerAddress, callerThreadId);
        return mapRemoteCallStates.get(remoteCallKey);
    }

    public CallState newLocalCallState(long callId, Address thisAddress, int callerThreadId) {
        CallState callStateNew = new CallState(callId, thisAddress, callerThreadId);
        CallState callBefore = mapLocalCallStates.put(callId, callStateNew);
        return callStateNew;
    }

    public CallState getLocalCallState(long callId) {
        return mapLocalCallStates.get(callId);
    }

    public CallState getLocalCallStateByThreadId(int callerThreadId) {
        for (CallState callState : mapLocalCallStates.values()) {
            if (callerThreadId == callState.getCallerThreadId()) {
                return callState;
            }
        }
        return null;
    }

    public void shutdown() {
        mapLocalCallStates.clear();
        mapRemoteCallStates.clear();
    }
}
