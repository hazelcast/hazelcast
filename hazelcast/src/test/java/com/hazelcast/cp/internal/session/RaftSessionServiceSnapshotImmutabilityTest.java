/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cp.internal.session;

import com.hazelcast.cluster.Address;
import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.internal.RaftGroupId;
import com.hazelcast.cp.internal.snapshot.NonChunkedSnapshotImmutabilityTest;
import com.hazelcast.cp.session.CPSession;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import java.util.stream.Collectors;


@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class RaftSessionServiceSnapshotImmutabilityTest
        extends NonChunkedSnapshotImmutabilityTest<RaftSessionRegistry, RaftSessionService> {

    private NodeEngineImpl nodeEngine;
    private CPGroupId groupId;

    @Before
    public void setUp() {
        this.nodeEngine = mockNodeEngine();
        this.groupId = new RaftGroupId("default", 1, 1);
    }

    @Override
    protected RaftSessionService newService() {
        RaftSessionService service = new RaftSessionService(nodeEngine);
        service.init(nodeEngine, new Properties());
        return service;
    }

    @Override
    protected CPGroupId groupId() {
        return groupId;
    }

    @Override
    protected void populateInitialState(RaftSessionService service, CPGroupId groupId) {
        Address ep = new Address();

        long session1 = service.createNewSession(groupId, ep, "ep",
                CPSession.CPSessionOwnerType.CLIENT).getSessionId();
        long session2 = service.createNewSession(groupId, ep, "ep",
                CPSession.CPSessionOwnerType.CLIENT).getSessionId();

        if (!(session2 > session1 && session1 > 0)) {
            throw new AssertionError("Session ids are not increasing as expected");
        }
    }

    @Override
    protected void mutateService(RaftSessionService service, CPGroupId groupId) {
        // Typical mutation: create another session on the same group.
        Address ep = new Address();
        service.createNewSession(groupId, ep, "ep",
                CPSession.CPSessionOwnerType.CLIENT);
    }

    @Override
    protected Object snapshotFingerprint(RaftSessionRegistry snapshot) {
        Map<String, Object> root = new TreeMap<>();

        // Sort sessions by id for determinism
        List<CPSession> sessions = snapshot.getSessions().stream()
                .sorted(Comparator.comparingLong(CPSession::id))
                .collect(Collectors.toList());

        // sessionId -> map of basic properties
        Map<Long, Map<String, Object>> sessionMap = new LinkedHashMap<>();
        for (CPSession s : sessions) {
            Map<String, Object> sInfo = new TreeMap<>();
            sInfo.put("creationTime", s.creationTime());
            sInfo.put("expirationTime", s.expirationTime());
            sInfo.put("version", s.version());
            sInfo.put("endpoint", String.valueOf(s.endpoint()));
            sInfo.put("endpointType", s.endpointType());
            sInfo.put("endpointName", s.endpointName());
            sessionMap.put(s.id(), sInfo);
        }

        root.put("sessions", sessionMap);
        return root;
    }
}
