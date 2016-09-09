/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.impl.runtime;

import com.hazelcast.core.Member;
import com.hazelcast.jet.impl.executor.Task;
import com.hazelcast.jet.impl.job.JobContext;
import com.hazelcast.jet.impl.job.JobService;
import com.hazelcast.jet.impl.operation.DiscoveryOperation;
import com.hazelcast.jet.impl.runtime.task.nio.SocketReader;
import com.hazelcast.jet.impl.runtime.task.nio.SocketWriter;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.NodeEngine;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

import static com.hazelcast.jet.impl.util.JetUtil.unchecked;

/**
 * Abstract discovery-service interface
 * <p/>
 * The goal is to find JET-nodes
 * <p/>
 * After discovery it created corresponding writers and readers
 */
public class DiscoveryService {
    private final NodeEngine nodeEngine;

    private final JobContext jobContext;

    private final Map<Address, SocketWriter> socketWriters;

    private final Map<Address, SocketReader> socketReaders;

    private final Map<Address, Address> hzToAddressMapping;

    public DiscoveryService(JobContext jobContext,
                            NodeEngine nodeEngine,
                            Map<Address, SocketWriter> socketWriters,
                            Map<Address, SocketReader> socketReaders,
                            Map<Address, Address> hzToAddressMapping) {
        this.nodeEngine = nodeEngine;
        this.socketReaders = socketReaders;
        this.socketWriters = socketWriters;
        this.hzToAddressMapping = hzToAddressMapping;
        this.jobContext = jobContext;
    }


    private Map<Member, Address> findMembers() {
        Map<Member, Address> memberMap = new HashMap<>();

        try {
            for (Member member : nodeEngine.getClusterService().getMembers()) {
                if (!member.localMember()) {
                    Future<Address> future = nodeEngine.getOperationService().invokeOnTarget(
                            JobService.SERVICE_NAME,
                            new DiscoveryOperation(),
                            member.getAddress()
                    );

                    Address remoteAddress = future.get();

                    memberMap.put(member, remoteAddress);
                    hzToAddressMapping.put(member.getAddress(), remoteAddress);
                }
            }

            hzToAddressMapping.put(
                    nodeEngine.getLocalMember().getAddress(),
                    jobContext.getLocalJetAddress()
            );

            return memberMap;
        } catch (Exception e) {
            throw unchecked(e);
        }
    }

    private void registerIOTasks(Map<Member, Address> map) {
        List<Task> tasks = new ArrayList<Task>();

        for (Member member : nodeEngine.getClusterService().getMembers()) {
            if (!member.localMember()) {
                Address jetAddress = map.get(member);

                SocketReader socketReader = new SocketReader(
                        jobContext,
                        jetAddress
                );

                tasks.add(
                        socketReader
                );

                SocketWriter socketWriter = new SocketWriter(
                        jobContext,
                        jetAddress
                );

                tasks.add(
                        socketWriter
                );

                socketWriters.put(jetAddress, socketWriter);
                socketReaders.put(jetAddress, socketReader);
            }
        }

        for (Task task : tasks) {
            jobContext.getExecutorContext().getNetworkTasks().add(task);
        }

        for (Map.Entry<Address, SocketReader> readerEntry : socketReaders.entrySet()) {
            for (Map.Entry<Address, SocketWriter> writerEntry : socketWriters.entrySet()) {
                SocketReader reader = readerEntry.getValue();
                reader.assignWriter(writerEntry.getKey(), writerEntry.getValue());
            }
        }
    }

    /**
     * Executes discovery process
     */
    public void executeDiscovery() {
        Map<Member, Address> memberAddressMap = findMembers();
        registerIOTasks(memberAddressMap);
    }

    /**
     * @return discovered socket writers
     */
    public Map<Address, SocketWriter> getSocketWriters() {
        return socketWriters;
    }

    /**
     * @return discovered socket readers
     */
    public Map<Address, SocketReader> getSocketReaders() {
        return socketReaders;
    }
}
