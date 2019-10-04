/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.impl.spi.impl.listener;

import com.hazelcast.client.impl.clientside.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.spi.ClientClusterService;
import com.hazelcast.client.impl.spi.EventHandler;
import com.hazelcast.client.impl.spi.impl.ListenerMessageCodec;
import com.hazelcast.cluster.Member;
import com.hazelcast.core.OperationTimeoutException;
import com.hazelcast.internal.util.EmptyStatement;
import com.hazelcast.internal.util.ExceptionUtil;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.internal.util.StringUtil.timeToString;

public class SmartClientListenerService extends AbstractClientListenerService  {

    public SmartClientListenerService(HazelcastClientInstanceImpl client) {
        super(client);
    }

    @Override
    public void start() {
        super.start();
        final ClientClusterService clientClusterService = client.getClientClusterService();
        registrationExecutor.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                Collection<Member> memberList = clientClusterService.getMemberList();
                for (Member member : memberList) {
                    try {
                        clientConnectionManager.getOrTriggerConnect(member.getAddress());
                    } catch (IOException e) {
                        EmptyStatement.ignore(e);
                        return;
                    }
                }
            }
        }, 1, 1, TimeUnit.SECONDS);
    }

    @Nonnull
    @Override
    public UUID registerListener(final ListenerMessageCodec codec, final EventHandler handler) {
        trySyncConnectToAllMembers();
        return super.registerListener(codec, handler);
    }

    @Override
    boolean registersLocalOnly() {
        return true;
    }

    private void trySyncConnectToAllMembers() {
        ClientClusterService clientClusterService = client.getClientClusterService();
        long startMillis = System.currentTimeMillis();

        do {
            Member lastFailedMember = null;
            Exception lastException = null;

            for (Member member : clientClusterService.getMemberList()) {
                try {
                    clientConnectionManager.getOrConnect(member.getAddress());
                } catch (Exception e) {
                    lastFailedMember = member;
                    lastException = e;
                }
            }

            if (lastException == null) {
                // successfully connected to all members, break loop.
                break;
            }

            timeOutOrSleepBeforeNextTry(startMillis, lastFailedMember, lastException);

        } while (client.getLifecycleService().isRunning());
    }

    private void timeOutOrSleepBeforeNextTry(long startMillis, Member lastFailedMember, Exception lastException) {
        long nowInMillis = System.currentTimeMillis();
        long elapsedMillis = nowInMillis - startMillis;
        boolean timedOut = elapsedMillis > invocationTimeoutMillis;

        if (timedOut) {
            throwOperationTimeoutException(startMillis, nowInMillis, elapsedMillis, lastFailedMember, lastException);
        } else {
            sleepBeforeNextTry();
        }
    }

    private void sleepBeforeNextTry() {
        try {
            Thread.sleep(invocationRetryPauseMillis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw ExceptionUtil.rethrow(e);
        }
    }

    private void throwOperationTimeoutException(long startMillis, long nowInMillis,
                                                long elapsedMillis, Member lastFailedMember, Exception lastException) {
        throw new OperationTimeoutException("Registering listeners is timed out."
                + " Last failed member : " + lastFailedMember + ", "
                + " Current time: " + timeToString(nowInMillis) + ", "
                + " Start time : " + timeToString(startMillis) + ", "
                + " Client invocation timeout : " + invocationTimeoutMillis + " ms, "
                + " Elapsed time : " + elapsedMillis + " ms. ", lastException);
    }
}
