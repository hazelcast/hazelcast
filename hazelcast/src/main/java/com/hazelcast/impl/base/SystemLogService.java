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

import com.hazelcast.impl.Node;
import com.hazelcast.nio.Address;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static java.util.logging.Level.WARNING;

public class SystemLogService {

    public enum Level {
        CS_NONE,
        CS_EMPTY,
        CS_INFO,
        CS_TRACE
    }

    private final ConcurrentMap<CallKey, CallState> mapCallStates = new ConcurrentHashMap<CallKey, CallState>(100, 0.75f, 32);

    private volatile Level currentLevel = Level.CS_NONE;

    private Node node;

    public SystemLogService(Node node) {
        this.node = node;
    }

    public CallState getOrCreateCallState(long callId, Address callerAddress, int callerThreadId) {
        if (currentLevel == Level.CS_NONE) return null;
        CallKey callKey = new CallKey(callerAddress, callerThreadId);
        CallState callBefore = mapCallStates.get(callKey);
        if (callBefore == null) {
            CallState callStateNew = new CallState(callId, callerAddress, callerThreadId);
            mapCallStates.put(callKey, callStateNew);
            int callStatesCount = mapCallStates.size();
            if (callStatesCount > 10000) {
                String msg = " CallStates created! You might have too many threads accessing Hazelcast!";
                node.getLogger(SystemLogService.class.getName()).log(WARNING, callStatesCount + msg);
            }
            return callStateNew;
        } else {
            if (callBefore.getCallId() != callId) {
                callBefore.reset(callId);
            }
            return callBefore;
        }
    }

    public CallState getCallState(Address callerAddress, int callerThreadId) {
        if (currentLevel == Level.CS_NONE) return null;
        return mapCallStates.get(new CallKey(callerAddress, callerThreadId));
    }

    public CallState getCallStateForCallId(long callId, Address callerAddress, int callerThreadId) {
        if (currentLevel == Level.CS_NONE) return null;
        CallState callState = mapCallStates.get(new CallKey(callerAddress, callerThreadId));
        if (callState != null && callState.getCallId() == callId) {
            return callState;
        }
        return null;
    }

    public void shutdown() {
        mapCallStates.clear();
    }

    public void setLogLevel(Level level) {
        this.currentLevel = level;
    }

    public boolean shouldLog(Level level) {
        return currentLevel != Level.CS_NONE && currentLevel.ordinal() >= level.ordinal();
    }

    public boolean shouldTrace() {
        return currentLevel != Level.CS_NONE && currentLevel.ordinal() >= Level.CS_TRACE.ordinal();
    }

    public boolean shouldInfo() {
        return currentLevel != Level.CS_NONE && currentLevel.ordinal() >= Level.CS_INFO.ordinal();
    }

    public void info(CallStateAware callStateAware, SystemLog callStateLog) {
        logState(callStateAware, Level.CS_INFO, callStateLog);
    }

    public void trace(CallStateAware callStateAware, SystemLog callStateLog) {
        logState(callStateAware, Level.CS_TRACE, callStateLog);
    }

    public void info(CallStateAware callStateAware, String msg) {
        logObject(callStateAware, Level.CS_INFO, msg);
    }

    public void trace(CallStateAware callStateAware, String msg) {
        logObject(callStateAware, Level.CS_TRACE, msg);
    }

    public void info(CallStateAware callStateAware, String msg, Object arg1) {
        logState(callStateAware, Level.CS_INFO, new SystemArgsLog(msg, arg1));
    }

    public void info(CallStateAware callStateAware, String msg, Object arg1, Object arg2) {
        logState(callStateAware, Level.CS_INFO, new SystemArgsLog(msg, arg1, arg2));
    }

    public void trace(CallStateAware callStateAware, String msg, Object arg1) {
        logState(callStateAware, Level.CS_TRACE, new SystemArgsLog(msg, arg1));
    }

    public void trace(CallStateAware callStateAware, String msg, Object arg1, Object arg2) {
        logState(callStateAware, Level.CS_TRACE, new SystemArgsLog(msg, arg1, arg2));
    }

    public void logObject(CallStateAware callStateAware, Level level, Object obj) {
        if (currentLevel.ordinal() >= level.ordinal()) {
            if (callStateAware != null) {
                CallState callState = callStateAware.getCallState();
                if (callState != null) {
                    callState.logObject(obj);
                }
            }
        }
    }

    public void logState(CallStateAware callStateAware, Level level, SystemLog callStateLog) {
        if (currentLevel.ordinal() >= level.ordinal()) {
            if (callStateAware != null) {
                CallState callState = callStateAware.getCallState();
                if (callState != null) {
                    callState.log(callStateLog);
                }
            }
        }
    }
}
