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

package com.hazelcast.client;

import com.hazelcast.impl.ClusterOperation;

import java.util.*;
import java.util.concurrent.ExecutionException;

public abstract class IORunnable extends ClientRunnable {

    protected Map<Long, Call> callMap;
    protected final HazelcastClient client;

    public IORunnable(HazelcastClient client, Map<Long, Call> calls) {
        this.client = client;
        this.callMap = calls;
    }

    public void interruptWaitingCalls() {
        Collection<Call> cc = callMap.values();
        List<Call> waitingCalls = new ArrayList<Call>();
        waitingCalls.addAll(cc);
        for (Iterator<Call> iterator = waitingCalls.iterator(); iterator.hasNext();) {
            Call call = iterator.next();
            synchronized (call) {
                RuntimeException exception = new NoMemberAvailableException();
                if (call.getRequest().getOperation().equals(ClusterOperation.REMOTELY_EXECUTE)) {
                    client.executorServiceManager.endFutureWithException(call, new ExecutionException(exception));
                    iterator.remove();
                }
                call.setRuntimeException(exception);
                call.notify();
            }
        }
    }

    public void run() {
        while (running) {
            try {
                customRun();
            } catch (InterruptedException e) {
                return;
            }
        }
        notifyMonitor();
    }

    protected boolean restoredConnection(Connection connection, boolean isOldConnectonNull, long oldConnectionId) {
        return !isOldConnectonNull && connection != null && connection.getVersion() != oldConnectionId;
    }

    protected boolean restoredConnection(Connection oldConnection, Connection newConnection) {
        long oldConnectionId = -1;
        boolean isOldConnectionNull = (oldConnection == null);
        if (!isOldConnectionNull) {
            oldConnectionId = oldConnection.getVersion();
        }
        return restoredConnection(newConnection, isOldConnectionNull, oldConnectionId);
    }
}
