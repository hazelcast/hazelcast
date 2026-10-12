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

package com.hazelcast.cp.internal.client;

import com.hazelcast.client.impl.protocol.MessageTaskFactory;
import com.hazelcast.client.impl.protocol.MessageTaskFactoryProvider;
import com.hazelcast.client.impl.protocol.codec.AtomicLongAddAndGetCodec;
import com.hazelcast.client.impl.protocol.codec.AtomicLongAlterCodec;
import com.hazelcast.client.impl.protocol.codec.AtomicLongApplyCodec;
import com.hazelcast.client.impl.protocol.codec.AtomicLongCompareAndSetCodec;
import com.hazelcast.client.impl.protocol.codec.AtomicLongGetAndAddCodec;
import com.hazelcast.client.impl.protocol.codec.AtomicLongGetAndSetCodec;
import com.hazelcast.client.impl.protocol.codec.AtomicLongGetCodec;
import com.hazelcast.client.impl.protocol.codec.AtomicRefApplyCodec;
import com.hazelcast.client.impl.protocol.codec.AtomicRefCompareAndSetCodec;
import com.hazelcast.client.impl.protocol.codec.AtomicRefContainsCodec;
import com.hazelcast.client.impl.protocol.codec.AtomicRefGetCodec;
import com.hazelcast.client.impl.protocol.codec.AtomicRefSetCodec;
import com.hazelcast.client.impl.protocol.codec.CPGroupCreateCPGroupCodec;
import com.hazelcast.client.impl.protocol.codec.CPGroupDestroyCPObjectCodec;
import com.hazelcast.client.impl.protocol.codec.CPMapCompareAndSetCodec;
import com.hazelcast.client.impl.protocol.codec.CPMapDeleteCodec;
import com.hazelcast.client.impl.protocol.codec.CPMapGetCodec;
import com.hazelcast.client.impl.protocol.codec.CPMapPutCodec;
import com.hazelcast.client.impl.protocol.codec.CPMapPutIfAbsentCodec;
import com.hazelcast.client.impl.protocol.codec.CPMapRemoveCodec;
import com.hazelcast.client.impl.protocol.codec.CPMapSetCodec;
import com.hazelcast.client.impl.protocol.codec.CPSessionCloseSessionCodec;
import com.hazelcast.client.impl.protocol.codec.CPSessionCreateSessionCodec;
import com.hazelcast.client.impl.protocol.codec.CPSessionGenerateThreadIdCodec;
import com.hazelcast.client.impl.protocol.codec.CPSessionHeartbeatSessionCodec;
import com.hazelcast.client.impl.protocol.codec.CPSubsystemAddGroupAvailabilityListenerCodec;
import com.hazelcast.client.impl.protocol.codec.CPSubsystemAddMembershipListenerCodec;
import com.hazelcast.client.impl.protocol.codec.CPSubsystemGetCPGroupIdsCodec;
import com.hazelcast.client.impl.protocol.codec.CPSubsystemGetCPObjectInfosCodec;
import com.hazelcast.client.impl.protocol.codec.CPSubsystemRemoveGroupAvailabilityListenerCodec;
import com.hazelcast.client.impl.protocol.codec.CPSubsystemRemoveMembershipListenerCodec;
import com.hazelcast.client.impl.protocol.codec.CountDownLatchAwaitCodec;
import com.hazelcast.client.impl.protocol.codec.CountDownLatchCountDownCodec;
import com.hazelcast.client.impl.protocol.codec.CountDownLatchGetCountCodec;
import com.hazelcast.client.impl.protocol.codec.CountDownLatchGetRoundCodec;
import com.hazelcast.client.impl.protocol.codec.CountDownLatchTrySetCountCodec;
import com.hazelcast.client.impl.protocol.codec.FencedLockGetLockOwnershipCodec;
import com.hazelcast.client.impl.protocol.codec.FencedLockLockCodec;
import com.hazelcast.client.impl.protocol.codec.FencedLockTryLockCodec;
import com.hazelcast.client.impl.protocol.codec.FencedLockUnlockCodec;
import com.hazelcast.client.impl.protocol.codec.SemaphoreAcquireCodec;
import com.hazelcast.client.impl.protocol.codec.SemaphoreAvailablePermitsCodec;
import com.hazelcast.client.impl.protocol.codec.SemaphoreChangeCodec;
import com.hazelcast.client.impl.protocol.codec.SemaphoreDrainCodec;
import com.hazelcast.client.impl.protocol.codec.SemaphoreGetSemaphoreTypeCodec;
import com.hazelcast.client.impl.protocol.codec.SemaphoreInitCodec;
import com.hazelcast.client.impl.protocol.codec.SemaphoreReleaseCodec;
import com.hazelcast.cp.internal.datastructures.atomiclong.client.AddAndGetMessageTask;
import com.hazelcast.cp.internal.datastructures.atomiclong.client.AlterMessageTask;
import com.hazelcast.cp.internal.datastructures.atomiclong.client.ApplyMessageTask;
import com.hazelcast.cp.internal.datastructures.atomiclong.client.GetAndAddMessageTask;
import com.hazelcast.cp.internal.datastructures.atomiclong.client.GetAndSetMessageTask;
import com.hazelcast.cp.internal.datastructures.atomicref.client.ContainsMessageTask;
import com.hazelcast.cp.internal.datastructures.countdownlatch.client.AwaitMessageTask;
import com.hazelcast.cp.internal.datastructures.countdownlatch.client.CountDownMessageTask;
import com.hazelcast.cp.internal.datastructures.countdownlatch.client.GetCountMessageTask;
import com.hazelcast.cp.internal.datastructures.countdownlatch.client.GetRoundMessageTask;
import com.hazelcast.cp.internal.datastructures.countdownlatch.client.TrySetCountMessageTask;
import com.hazelcast.cp.internal.datastructures.cpmap.client.CompareAndSetMessageTask;
import com.hazelcast.cp.internal.datastructures.cpmap.client.DeleteMessageTask;
import com.hazelcast.cp.internal.datastructures.cpmap.client.GetMessageTask;
import com.hazelcast.cp.internal.datastructures.cpmap.client.PutIfAbsentMessageTask;
import com.hazelcast.cp.internal.datastructures.cpmap.client.PutMessageTask;
import com.hazelcast.cp.internal.datastructures.cpmap.client.RemoveMessageTask;
import com.hazelcast.cp.internal.datastructures.cpmap.client.SetMessageTask;
import com.hazelcast.cp.internal.datastructures.lock.client.GetLockOwnershipStateMessageTask;
import com.hazelcast.cp.internal.datastructures.lock.client.LockMessageTask;
import com.hazelcast.cp.internal.datastructures.lock.client.TryLockMessageTask;
import com.hazelcast.cp.internal.datastructures.lock.client.UnlockMessageTask;
import com.hazelcast.cp.internal.datastructures.semaphore.client.AcquirePermitsMessageTask;
import com.hazelcast.cp.internal.datastructures.semaphore.client.AvailablePermitsMessageTask;
import com.hazelcast.cp.internal.datastructures.semaphore.client.ChangePermitsMessageTask;
import com.hazelcast.cp.internal.datastructures.semaphore.client.DrainPermitsMessageTask;
import com.hazelcast.cp.internal.datastructures.semaphore.client.GetSemaphoreTypeMessageTask;
import com.hazelcast.cp.internal.datastructures.semaphore.client.InitSemaphoreMessageTask;
import com.hazelcast.cp.internal.datastructures.semaphore.client.ReleasePermitsMessageTask;
import com.hazelcast.cp.internal.datastructures.spi.client.CreateRaftGroupMessageTask;
import com.hazelcast.cp.internal.datastructures.spi.client.DestroyRaftObjectMessageTask;
import com.hazelcast.cp.internal.session.client.CloseSessionMessageTask;
import com.hazelcast.cp.internal.session.client.CreateSessionMessageTask;
import com.hazelcast.cp.internal.session.client.GenerateThreadIdMessageTask;
import com.hazelcast.cp.internal.session.client.HeartbeatSessionMessageTask;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.util.collection.Int2ObjectHashMap;
import com.hazelcast.spi.impl.NodeEngine;

@SuppressWarnings({"ClassDataAbstractionCoupling", "ClassFanOutComplexity"})
public class CPMessageTaskFactoryProvider implements MessageTaskFactoryProvider {
    private final Node node;
    private final Int2ObjectHashMap<MessageTaskFactory> factories;

    public CPMessageTaskFactoryProvider(NodeEngine nodeEngine) {
        node = nodeEngine.getNode();
        factories = new Int2ObjectHashMap<>();
        initializeCPGroupTaskFactories();
        initializeCPSubsystemMessageTaskFactories();
        initializeAtomicLongTaskFactories();
        initializeAtomicReferenceTaskFactories();
        initializeCountDownLatchTaskFactories();
        initializeFencedLockTaskFactories();
        initializeSemaphoreTaskFactories();
        initializeCPMapTaskFactories();
    }

    private void initializeCPGroupTaskFactories() {
        factories.put(CPGroupCreateCPGroupCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new CreateRaftGroupMessageTask(cm, node, con));
        factories.put(CPGroupDestroyCPObjectCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new DestroyRaftObjectMessageTask(cm, node, con));
        factories.put(CPSessionCreateSessionCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new CreateSessionMessageTask(cm, node, con));
        factories.put(CPSessionHeartbeatSessionCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new HeartbeatSessionMessageTask(cm, node, con));
        factories.put(CPSessionCloseSessionCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new CloseSessionMessageTask(cm, node, con));
        factories.put(CPSessionGenerateThreadIdCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new GenerateThreadIdMessageTask(cm, node, con));
    }

    private void initializeCPSubsystemMessageTaskFactories() {
        factories.put(CPSubsystemAddMembershipListenerCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new AddCPMembershipListenerMessageTask(cm, node, con));
        factories.put(CPSubsystemRemoveMembershipListenerCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new RemoveCPMembershipListenerMessageTask(cm, node, con));
        factories.put(CPSubsystemAddGroupAvailabilityListenerCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new AddCPGroupAvailabilityListenerMessageTask(cm, node, con));
        factories.put(CPSubsystemRemoveGroupAvailabilityListenerCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new RemoveCPGroupAvailabilityListenerMessageTask(cm, node, con));
        factories.put(CPSubsystemGetCPGroupIdsCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new CPSubsystemGetCPGroupIdsMessageTask(cm, node, con));
        factories.put(CPSubsystemGetCPObjectInfosCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new CPSubsystemGetCPObjectInfosMessageTask(cm, node, con));
    }

    private void initializeAtomicLongTaskFactories() {
        factories.put(AtomicLongAddAndGetCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new AddAndGetMessageTask(cm, node, con));
        factories.put(AtomicLongCompareAndSetCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new com.hazelcast.cp.internal.datastructures.atomiclong.client
                        .CompareAndSetMessageTask(cm, node, con));
        factories.put(AtomicLongGetAndAddCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new GetAndAddMessageTask(cm, node, con));
        factories.put(AtomicLongGetCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new com.hazelcast.cp.internal.datastructures.atomiclong.client
                        .GetMessageTask(cm, node, con));
        factories.put(AtomicLongGetAndSetCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new GetAndSetMessageTask(cm, node, con));
        factories.put(AtomicLongApplyCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new ApplyMessageTask(cm, node, con));
        factories.put(AtomicLongAlterCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new AlterMessageTask(cm, node, con));
    }

    private void initializeAtomicReferenceTaskFactories() {
        factories.put(AtomicRefApplyCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new com.hazelcast.cp.internal.datastructures.atomicref.client
                        .ApplyMessageTask(cm, node, con));
        factories.put(AtomicRefSetCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new com.hazelcast.cp.internal.datastructures.atomicref.client
                        .SetMessageTask(cm, node, con));
        factories.put(AtomicRefContainsCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new ContainsMessageTask(cm, node, con));
        factories.put(AtomicRefGetCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new com.hazelcast.cp.internal.datastructures.atomicref.client
                        .GetMessageTask(cm, node, con));
        factories.put(AtomicRefCompareAndSetCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new com.hazelcast.cp.internal.datastructures.atomicref.client
                        .CompareAndSetMessageTask(cm, node, con));
    }

    private void initializeCountDownLatchTaskFactories() {
        factories.put(CountDownLatchAwaitCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new AwaitMessageTask(cm, node, con));
        factories.put(CountDownLatchCountDownCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new CountDownMessageTask(cm, node, con));
        factories.put(CountDownLatchGetCountCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new GetCountMessageTask(cm, node, con));
        factories.put(CountDownLatchGetRoundCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new GetRoundMessageTask(cm, node, con));
        factories.put(CountDownLatchTrySetCountCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new TrySetCountMessageTask(cm, node, con));
    }

    private void initializeFencedLockTaskFactories() {
        factories.put(FencedLockLockCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new LockMessageTask(cm, node, con));
        factories.put(FencedLockTryLockCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new TryLockMessageTask(cm, node, con));
        factories.put(FencedLockUnlockCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new UnlockMessageTask(cm, node, con));
        factories.put(FencedLockGetLockOwnershipCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new GetLockOwnershipStateMessageTask(cm, node, con));
    }

    private void initializeSemaphoreTaskFactories() {
        factories.put(SemaphoreAcquireCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new AcquirePermitsMessageTask(cm, node, con));
        factories.put(SemaphoreAvailablePermitsCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new AvailablePermitsMessageTask(cm, node, con));
        factories.put(SemaphoreChangeCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new ChangePermitsMessageTask(cm, node, con));
        factories.put(SemaphoreDrainCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new DrainPermitsMessageTask(cm, node, con));
        factories.put(SemaphoreGetSemaphoreTypeCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new GetSemaphoreTypeMessageTask(cm, node, con));
        factories.put(SemaphoreInitCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new InitSemaphoreMessageTask(cm, node, con));
        factories.put(SemaphoreReleaseCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new ReleasePermitsMessageTask(cm, node, con));
    }

    public void initializeCPMapTaskFactories() {
        factories.put(CPMapGetCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new GetMessageTask(cm, node, con));
        factories.put(CPMapPutCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new PutMessageTask(cm, node, con));
        factories.put(CPMapSetCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new SetMessageTask(cm, node, con));
        factories.put(CPMapRemoveCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new RemoveMessageTask(cm, node, con));
        factories.put(CPMapDeleteCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new DeleteMessageTask(cm, node, con));
        factories.put(CPMapCompareAndSetCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new CompareAndSetMessageTask(cm, node, con));
        factories.put(CPMapPutIfAbsentCodec.REQUEST_MESSAGE_TYPE,
                (cm, con) -> new PutIfAbsentMessageTask(cm, node, con));
    }

    @Override
    public Int2ObjectHashMap<MessageTaskFactory> getFactories() {
        return factories;
    }
}
