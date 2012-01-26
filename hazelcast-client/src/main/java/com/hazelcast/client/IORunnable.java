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
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.logging.Level;

public abstract class IORunnable extends ClientRunnable {

    protected Map<Long, Call> callMap;
    protected final HazelcastClient client;
    final ILogger logger = Logger.getLogger(this.getClass().getName());
    protected static final Call RECONNECT_CALL = new Call(-1L, null);

    public IORunnable(HazelcastClient client, Map<Long, Call> calls) {
        this.client = client;
        this.callMap = calls;
    }

    @Override
    public void run() {
        try {
            super.run();
            logger.log(Level.INFO, getClass().getSimpleName() + " is finished ok.");
        } catch (Throwable e) {
            logger.log(Level.WARNING, getClass().getSimpleName()
                    + " got exception:" + e.getMessage() + ", shutdown client.", e);
            interruptWaitingCallsAndShutdown(true);
        } finally {
            logger.log(Level.INFO, getClass().getSimpleName() + " is finished.");
        }
    }

    public void interruptWaitingCallsAndShutdown(boolean shutdown) {
        interruptWaitingCalls();
        if (shutdown) {
            client.shutdown();
        }
    }

    public void interruptWaitingCalls() {
        final Collection<Call> values = callMap.values();
        for (Iterator<Call> it = values.iterator(); it.hasNext(); ) {
            Call call = it.next();
            if (call == RECONNECT_CALL) continue;
            logger.log(Level.INFO, "Cancel " + call);
            call.setResponse(new NoMemberAvailableException());
            it.remove();
        }
    }

    protected boolean onDisconnect(Connection oldConnection) {
        synchronized (callMap) {
            boolean shouldExecuteOnDisconnect = client.getConnectionManager().shouldExecuteOnDisconnect(oldConnection);
            if (!shouldExecuteOnDisconnect) {
                return false;
            }
            Member leftMember = oldConnection.getMember();
            Collection<Call> calls = callMap.values();
            for (Call call : calls) {
                if (call == RECONNECT_CALL) continue;
                Call removed = callMap.remove(call.getId());
                if (removed != null) {
                    if (!client.getOutRunnable().queue.contains(removed)) {
                        logger.log(Level.FINE, Thread.currentThread() + ": Calling on disconnect " + leftMember);
                        removed.onDisconnect(leftMember);
                    }
                }
            }
            return true;
        }
    }

    private boolean restoredConnection(Connection connection, long oldConnectionId) {
        return connection != null && connection.getVersion() != oldConnectionId;
    }

    protected boolean restoredConnection(Connection oldConnection, Connection newConnection) {
        final long oldConnectionId = oldConnection == null ? -1 : oldConnection.getVersion();
        return restoredConnection(newConnection, oldConnectionId);
    }
}
