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

package com.hazelcast.impl;

import com.hazelcast.nio.Address;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class CallHistory {

    Map<Address, CallerCallHistory> mapCallerHistory = new HashMap<Address, CallerCallHistory>(100);

    CallerCallHistory getOrCreateCallerCallHistory(Address address) {
        CallerCallHistory ch = mapCallerHistory.get(address);
        if (ch == null) {
            ch = new CallerCallHistory(address);
            mapCallerHistory.put(address, ch);
        }
        return ch;
    }

    CallerCallHistory getCallerCallHistory(Address address) {
        return mapCallerHistory.get(address);
    }

    public CallerThreadState getOrCreateCallerThreadState(Address caller, int threadId) {
        CallerCallHistory ch = getOrCreateCallerCallHistory(caller);
        return ch.getOrCreateCallerThreadState(threadId);
    }

    public class CallerCallHistory {
        final Address caller;
        final Map<Integer, CallerThreadState> mapThreadIdCallStatus = new HashMap<Integer, CallerThreadState>(100);

        CallerCallHistory(Address caller) {
            this.caller = caller;
        }

        public CallerThreadState getOrCreateCallerThreadState(int threadId) {
            CallerThreadState cts = mapThreadIdCallStatus.get(threadId);
            if (cts == null) {
                cts = new CallerThreadState(caller, threadId);
                mapThreadIdCallStatus.put(threadId, cts);
            }
            return cts;
        }

        public CallerThreadState getCallerThreadState(int threadId) {
            return mapThreadIdCallStatus.get(threadId);
        }
    }

    enum CallState {
        RECEIVED,
        SCHEDULED,
        REPLIED,
        UNKNOWN
    }

    public static class CallerThreadState implements Serializable {
        final Address caller;
        final int threadId;
        ClusterOperation operation;
        long callId;
        CallState callState = CallState.RECEIVED;
        Object response;
        String name;

        CallerThreadState(Address caller, int threadId) {
            this.caller = caller;
            this.threadId = threadId;
        }

        void set(String name, long callId, ClusterOperation operation) {
            this.name = name;
            this.callId = callId;
            this.operation = operation;
        }

        public CallState getCallState() {
            return callState;
        }

        public void setCallState(CallState callState) {
            this.callState = callState;
        }

        public void setScheduled() {
            callState = CallHistory.CallState.SCHEDULED;
        }

        public void setReplied() {
            callState = CallHistory.CallState.REPLIED;
        }

        public long getCallId() {
            return callId;
        }

        public ClusterOperation getOperation() {
            return operation;
        }

        public int getThreadId() {
            return threadId;
        }

        public Address getCaller() {
            return caller;
        }

        public Object getResponse() {
            return response;
        }

        public void setResponse(Object response) {
            this.response = response;
        }

        @Override
        public String toString() {
            return "CallerThreadState{" +
                    "name=" + name +
                    ", caller=" + caller +
                    ", threadId=" + threadId +
                    ", callId=" + callId +
                    ", operation=" + operation +
                    ", callState=" + callState +
                    ", response=" + response +
                    '}';
        }
    }
}
