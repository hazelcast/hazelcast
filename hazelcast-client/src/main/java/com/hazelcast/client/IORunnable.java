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

import com.hazelcast.core.Member;

import java.util.Collection;
import java.util.Map;

public abstract class IORunnable extends ClientRunnable {

    protected Map<Long, Call> callMap;
    protected final HazelcastClient client;

    public IORunnable(HazelcastClient client, Map<Long, Call> calls) {
        this.client = client;
        this.callMap = calls;
    }

    public void interruptWaitingCalls() {
        Collection<Call> calls = callMap.values();
        for (Call call : calls) {
            call.setResponse(new NoMemberAvailableException());
        }
        calls.clear();
        new Thread(new Runnable() {
            public void run() {
                client.shutdown();
            }
        }).start();
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

    protected void onDisconnect(Connection oldConnection) {
        boolean shouldExecuteOnDisconnect = client.getConnectionManager().shouldExecuteOnDisconnect(oldConnection);
        if(!shouldExecuteOnDisconnect){
            return;
        }
        Member memberLeft = oldConnection.getMember();
        Collection<Call> calls = callMap.values();
        for (Call call : calls) {
            Call removed = callMap.remove(call.getId());
            if(removed!=null){
                removed.onDisconnect(memberLeft);
            }
        }
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
