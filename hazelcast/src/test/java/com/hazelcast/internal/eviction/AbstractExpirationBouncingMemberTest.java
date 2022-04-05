/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.eviction;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.core.IExecutorService;
import com.hazelcast.core.IFunction;
import com.hazelcast.cluster.Member;
import com.hazelcast.core.MultiExecutionCallback;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.bounce.BounceMemberRule;
import com.hazelcast.test.bounce.BounceTestConfiguration;
import org.junit.Rule;
import org.junit.Test;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static com.hazelcast.internal.util.StringUtil.isNullOrEmpty;
import static java.lang.String.format;
import static org.junit.Assert.assertEquals;

public abstract class AbstractExpirationBouncingMemberTest extends HazelcastTestSupport {

    protected static final int ONE_SECOND = 1;
    protected static final long FIVE_MINUTES_IN_SECONDS = 5 * 60;
    protected static final long TEN_MINUTES_IN_MILLIS = 600000;

    protected int keySpace = 10000;
    protected int backupCount = 3;
    protected String name = getClass().getName();

    @Rule
    public BounceMemberRule bounceMemberRule = BounceMemberRule.with(getConfig())
            .clusterSize(4)
            .driverCount(1)
            .bouncingIntervalSeconds(3 * ONE_SECOND)
            .driverType(BounceTestConfiguration.DriverType.ALWAYS_UP_MEMBER)
            .build();

    protected abstract Runnable[] getTasks();

    protected abstract IFunction<HazelcastInstance, List> newExceptionMsgCreator();

    @Test(timeout = TEN_MINUTES_IN_MILLIS)
    public final void backups_should_be_empty_after_expiration() {
        Runnable[] methods = getTasks();

        bounceMemberRule.testRepeatedly(methods, 20);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                RemainingCacheSize remainingCacheSize = findRemainingCacheSize();
                assertEquals(remainingCacheSize.getMsgUnexpired(),
                        0, remainingCacheSize.getTotalUnexpired());
            }

            private RemainingCacheSize findRemainingCacheSize() {
                CountDownLatch latch = new CountDownLatch(1);
                HazelcastInstance steadyMember = bounceMemberRule.getSteadyMember();
                IExecutorService executorService = steadyMember.getExecutorService("test");
                RemainingCacheSize remainingCacheSize = new RemainingCacheSize(latch);
                UnexpiredRecordStoreCollector storeCollector = new UnexpiredRecordStoreCollector(newExceptionMsgCreator());
                executorService.submitToAllMembers(storeCollector, remainingCacheSize);
                assertOpenEventually(latch);
                return remainingCacheSize;
            }
        }, FIVE_MINUTES_IN_SECONDS);
    }

    private static final class RemainingCacheSize implements MultiExecutionCallback {
        private final CountDownLatch latch;
        private final AtomicInteger totalUnexpired = new AtomicInteger();
        private final AtomicReference<String> msgUnexpired = new AtomicReference();

        RemainingCacheSize(CountDownLatch latch) {
            this.latch = latch;
        }

        @Override
        public void onResponse(Member member, Object value) {
            //Intentionally empty method body.
        }

        @Override
        public void onComplete(Map<Member, Object> values) {
            try {
                String formatStr = "";
                ArrayList params = new ArrayList();
                int sumUnexpired = 0;
                for (Map.Entry<Member, Object> entry : values.entrySet()) {
                    List info = (List) entry.getValue();
                    for (int i = 0; i < info.size(); i += 6) {
                        sumUnexpired += (Integer) info.get(i + 1);

                        formatStr += "%n[id: %d, size: %d, sizeQueued: %d, expirable: %b, primary: %b, address: %s]";

                        params.add(info.get(i));
                        params.add(info.get(i + 1));
                        params.add(info.get(i + 2));
                        params.add(info.get(i + 3));
                        params.add(info.get(i + 4));
                        params.add(info.get(i + 5));
                    }
                }
                totalUnexpired.set(sumUnexpired);
                msgUnexpired.set(isNullOrEmpty(formatStr) ? "" : format(formatStr + "%n", params.toArray()));
            } finally {
                latch.countDown();
            }
        }

        public int getTotalUnexpired() {
            return totalUnexpired.get();
        }

        public String getMsgUnexpired() {
            return "Unexpired partitions found:" + msgUnexpired;
        }
    }

    public static class UnexpiredRecordStoreCollector
            implements Callable<List>, HazelcastInstanceAware, Serializable {

        private HazelcastInstance hazelcastInstance;

        private final IFunction<HazelcastInstance, List> msgCreator;

        public UnexpiredRecordStoreCollector(IFunction msgCreator) {
            this.msgCreator = msgCreator;
        }

        @Override
        public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
            this.hazelcastInstance = hazelcastInstance;
        }

        @Override
        public List call() throws Exception {
            return msgCreator.apply(hazelcastInstance);
        }
    }
}
