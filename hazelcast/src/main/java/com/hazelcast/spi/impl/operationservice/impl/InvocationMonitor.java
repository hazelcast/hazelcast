/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.spi.impl.operationservice.impl;

import com.hazelcast.core.MemberLeftException;
import com.hazelcast.instance.GroupProperties;
import com.hazelcast.instance.GroupProperty;
import com.hazelcast.instance.HazelcastThreadGroup;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.internal.metrics.MetricsRegistry;
import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.ExecutionService;
import com.hazelcast.spi.impl.operationexecutor.OperationHostileThread;
import com.hazelcast.util.Clock;
import com.hazelcast.util.EmptyStatement;
import com.hazelcast.util.counters.SwCounter;

import java.util.concurrent.TimeUnit;

import static com.hazelcast.instance.OutOfMemoryErrorDispatcher.inspectOutputMemoryError;
import static com.hazelcast.internal.metrics.ProbeLevel.MANDATORY;
import static com.hazelcast.util.counters.SwCounter.newSwCounter;

/**
 * The InvocationMonitor monitors all pending invocations and determines if there are any problems like timeouts. It uses the
 * {@link InvocationRegistry} to access the pending invocations.
 *
 * An experimental feature to support debugging is the slow invocation detector. So it can log any invocation that takes
 * more than x seconds. See {@link GroupProperty#SLOW_INVOCATION_DETECTOR_THRESHOLD_MILLIS} for more information.
 */
public class InvocationMonitor {

    private static final long ON_MEMBER_LEFT_DELAY_MS = 1111;
    private static final int SCAN_DELAY_MILLIS = 1000;

    private final long backupTimeoutMillis;
    private final long slowInvocationThresholdMs;
    private final InvocationRegistry invocationRegistry;
    private final ExecutionService executionService;
    private final MonitorThread monitorThread;
    private final ILogger logger;
    @Probe(name = "invocations.backupTimeouts", level = MANDATORY)
    private final SwCounter backupTimeoutsCount = newSwCounter();
    @Probe(name = "invocations.normalTimeouts", level = MANDATORY)
    private final SwCounter normalTimeoutsCount = newSwCounter();

    public InvocationMonitor(InvocationRegistry invocationRegistry, ILogger logger, GroupProperties props,
                             HazelcastThreadGroup hzThreadGroup, ExecutionService executionService,
                             MetricsRegistry metricsRegistry) {
        this.invocationRegistry = invocationRegistry;
        this.logger = logger;
        this.executionService = executionService;
        this.backupTimeoutMillis = props.getMillis(GroupProperty.OPERATION_BACKUP_TIMEOUT_MILLIS);
        this.slowInvocationThresholdMs = initSlowInvocationThresholdMs(props);
        this.monitorThread = new MonitorThread(hzThreadGroup);

        metricsRegistry.scanAndRegister(this, "operation");

        monitorThread.start();
    }

    private long initSlowInvocationThresholdMs(GroupProperties props) {
        long thresholdMs = props.getMillis(GroupProperty.SLOW_INVOCATION_DETECTOR_THRESHOLD_MILLIS);
        if (thresholdMs > -1) {
            logger.info("Slow invocation detector enabled, using threshold: " + thresholdMs + " ms");
        }
        return thresholdMs;
    }

    public void shutdown() {
        monitorThread.shutdown();
    }

    public void awaitTermination(long timeoutMillis) throws InterruptedException {
        monitorThread.join(timeoutMillis);
    }

    public void onMemberLeft(MemberImpl member) {
        // postpone notifying invocations since real response may arrive in the mean time.
        Runnable task = new OnMemberLeftTask(member);
        executionService.schedule(task, ON_MEMBER_LEFT_DELAY_MS, TimeUnit.MILLISECONDS);
    }

    /**
     * The MonitorThread iterates over all pending invocations and sees what needs to be done
     *
     * But it should also check if a 'is still running' check needs to be done. This removed complexity from
     * the invocation.waitForResponse which is too complicated too understand.
     *
     * This class needs to implement the {@link OperationHostileThread} interface to make sure that the OperationExecutor
     * is not going to schedule any operations on this task due to retry.
     */
    private final class MonitorThread extends Thread implements OperationHostileThread {

        private volatile boolean shutdown;

        private MonitorThread(HazelcastThreadGroup hzThreadGroup) {
            super(hzThreadGroup.getInternalThreadGroup(), hzThreadGroup.getThreadNamePrefix("InvocationMonitorThread"));
        }

        public void shutdown() {
            shutdown = true;
            interrupt();
        }

        @Override
        public void run() {
            try {
                while (!shutdown) {
                    scan();
                    if (!shutdown) {
                        sleep();
                    }
                }
            } catch (Throwable t) {
                inspectOutputMemoryError(t);
                logger.severe("Failed to run", t);
            }
        }

        private void sleep() {
            try {
                Thread.sleep(SCAN_DELAY_MILLIS);
            } catch (InterruptedException ignore) {
                // can safely be ignored. If this thread wants to shut down, it will read the shutdown variable.
                EmptyStatement.ignore(ignore);
            }
        }

        private void scan() {
            if (invocationRegistry.size() == 0) {
                return;
            }

            long now = Clock.currentTimeMillis();
            int backupTimeouts = 0;
            int invocationTimeouts = 0;
            for (Invocation invocation : invocationRegistry.invocations()) {
                if (shutdown) {
                    return;
                }

                detectSlowInvocation(now, invocation);

                if (checkInvocationTimeout(invocation)) {
                    invocationTimeouts++;
                }

                if (checkBackupTimeout(invocation)) {
                    backupTimeouts++;
                }
            }

            backupTimeoutsCount.inc(backupTimeouts);
            normalTimeoutsCount.inc(invocationTimeouts);
            log(backupTimeouts, invocationTimeouts);
        }

        private void detectSlowInvocation(long now, Invocation invocation) {
            if (slowInvocationThresholdMs > 0) {
                long durationMs = now - invocation.op.getInvocationTime();
                if (durationMs > slowInvocationThresholdMs) {
                    logger.info("Slow invocation: duration=" + durationMs + " ms, operation="
                            + invocation.op.getClass().getName() + " inv:" + invocation);
                }
            }
        }

        private boolean checkInvocationTimeout(Invocation invocation) {
            try {
                return invocation.checkInvocationTimeout();
            } catch (Throwable t) {
                inspectOutputMemoryError(t);
                logger.severe("Failed to handle operation timeout of invocation:" + invocation, t);
                return false;
            }
        }

        private boolean checkBackupTimeout(Invocation invocation) {
            try {
                return invocation.checkBackupTimeout(backupTimeoutMillis);
            } catch (Throwable t) {
                inspectOutputMemoryError(t);
                logger.severe("Failed to handle backup timeout of invocation:" + invocation, t);
                return false;
            }
        }

        private void log(int backupTimeouts, int invocationTimeouts) {
            if (backupTimeouts > 0 || invocationTimeouts > 0) {
                logger.info("Handled " + invocationTimeouts + " invocation timeouts and " + backupTimeouts + " backupTimeouts");
            }
        }
    }

    private final class OnMemberLeftTask implements Runnable {
        private final MemberImpl leftMember;

        public OnMemberLeftTask(MemberImpl leftMember) {
            this.leftMember = leftMember;
        }

        @Override
        public void run() {
            for (Invocation invocation : invocationRegistry.invocations()) {
                if (hasMemberLeft(invocation)) {
                    invocation.notifyError(new MemberLeftException(leftMember));
                }
            }
        }

        private boolean hasMemberLeft(Invocation invocation) {
            MemberImpl targetMember = invocation.targetMember;
            if (targetMember == null) {
                Address invTarget = invocation.invTarget;
                return leftMember.getAddress().equals(invTarget);
            } else {
                return leftMember.getUuid().equals(targetMember.getUuid());
            }
        }
    }
}
