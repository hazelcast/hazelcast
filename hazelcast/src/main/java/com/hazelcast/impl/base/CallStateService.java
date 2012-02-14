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

    public enum Level {
        CS_NONE,
        CS_INFO,
        CS_TRACE
    }

    private final ConcurrentMap<CallKey, CallState> mapCallStates = new ConcurrentHashMap<CallKey, CallState>(100, 0.75f, 32);

    private volatile Level currentLevel = Level.CS_NONE;

    public CallState newCallState(long callId, Address callerAddress, int callerThreadId) {
        if (currentLevel == Level.CS_NONE) return null;
        CallKey callKey = new CallKey(callerAddress, callerThreadId);
        CallState callBefore = mapCallStates.get(callKey);
        if (callBefore == null || callBefore.getCallId() != callId) {
            CallState callStateNew = new CallState(callId, callerAddress, callerThreadId);
            mapCallStates.put(callKey, callStateNew);
            return callStateNew;
        } else {
            return callBefore;
        }
    }

    public CallState getCallState(Address remoteCallerAddress, int callerThreadId) {
        if (currentLevel == Level.CS_NONE) return null;
        return mapCallStates.get(new CallKey(remoteCallerAddress, callerThreadId));
    }

    public void shutdown() {
        mapCallStates.clear();
    }

    public void setLogLevel(Level level) {
        this.currentLevel = level;
    }

    public boolean shouldLog(Level csInfo) {
        return currentLevel.ordinal() >= csInfo.ordinal();
    }

    public void logObject(CallStateAware callStateAware, Level level, Object obj) {
        if (currentLevel.ordinal() >= level.ordinal()) {
            CallState callState = callStateAware.getCallState();
            if (callState != null) {
                callState.logObject(obj);
            }
        }
    }
}
