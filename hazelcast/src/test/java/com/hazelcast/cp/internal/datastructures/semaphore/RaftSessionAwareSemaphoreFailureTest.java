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

package com.hazelcast.cp.internal.datastructures.semaphore;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ISemaphore;
import com.hazelcast.cp.internal.RaftGroupId;
import com.hazelcast.cp.internal.datastructures.semaphore.proxy.RaftSessionAwareSemaphoreProxy;
import com.hazelcast.cp.internal.session.ProxySessionManagerService;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.util.ThreadUtil;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.cp.internal.session.ProxySessionManagerService.NO_SESSION_ID;
import static com.hazelcast.cp.internal.session.ProxySessionManagerService.SERVICE_NAME;
import static org.junit.Assert.assertNotEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class RaftSessionAwareSemaphoreFailureTest extends RaftSemaphoreFailureTest {

    @Override
    boolean isJDKCompatible() {
        return false;
    }

    @Override
    RaftGroupId getGroupId(ISemaphore semaphore) {
        return ((RaftSessionAwareSemaphoreProxy) semaphore).getGroupId();
    }

    @Override
    long getSessionId(HazelcastInstance semaphoreInstance, RaftGroupId groupId) {
        ProxySessionManagerService service = getNodeEngineImpl(semaphoreInstance).getService(SERVICE_NAME);
        long sessionId = service.getSession(groupId);
        assertNotEquals(NO_SESSION_ID, sessionId);

        return sessionId;
    }

    @Override
    long getThreadId(RaftGroupId groupId) {
        return ThreadUtil.getThreadId();
    }


}
