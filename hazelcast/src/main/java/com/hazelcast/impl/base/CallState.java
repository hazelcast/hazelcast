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

import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

public class CallState {
    private final long callId;
    private final Address caller;
    private final int callerThreadId;
    private final Queue<CallStateLog> logQ = new LinkedBlockingQueue<CallStateLog>(1000);
    private final Queue<Address> targets = new LinkedBlockingQueue<Address>(10);

    public CallState(long callId, Address caller, int callerThreadId) {
        this.callId = callId;
        this.caller = caller;
        this.callerThreadId = callerThreadId;
    }

    public void addLog(CallStateLog log) {
        logQ.offer(log);
    }

    public void addTarget(Address target) {
        targets.offer(target);
    }

    public Address getCaller() {
        return caller;
    }

    public int getCallerThreadId() {
        return callerThreadId;
    }

    public Queue<Address> getTargets() {
        return targets;
    }

    public long getCallId() {
        return callId;
    }

    public Queue<CallStateLog> getLogQ() {
        return logQ;
    }
}
