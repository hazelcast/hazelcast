/*
 * Copyright (c) 2008-2024, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.util;

import com.hazelcast.client.properties.ClientProperty;
import com.hazelcast.cluster.Member;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.LoggingService;
import com.hazelcast.spi.impl.executionservice.TaskScheduler;
import com.hazelcast.spi.properties.HazelcastProperties;

import javax.annotation.concurrent.NotThreadSafe;
import java.util.Collection;
import java.util.concurrent.Future;

import static java.lang.System.lineSeparator;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Logs the connectivity of the client to the cluster.
 * <p>
 * A task will log the connectivity of the client with a delay of
 * {@link ClientProperty#CLIENT_CONNECTIVITY_LOGGING_DELAY_SECONDS}
 * seconds.
 * If a logging task is already submitted, then the current caller will cancel
 * the task and resubmit.
 * <p>
 * The logger will log the connectivity of the client to the cluster in
 * the following format:
 * <pre>
 * Client Connectivity [4] {
 *     Member [127.0.0.1]:5701 - 676956a0-0b86-4097-a1e1-42b01b0a664c - disconnected
 *     Member [127.0.0.1]:5702 - c1a6449f-432f-45ba-9570-52b8ec90ce89 - disconnected
 *     Member [127.0.0.1]:5703 - 76463a61-0c4b-4180-8970-56b14b156f4c - connected
 *     Member [127.0.0.1]:5704 - 7a172832-8734-4eef-80a9-656bf1e28c2a - connected
 * }
 * </pre>
 */
@NotThreadSafe
public class ClientConnectivityLogger {

    private final ILogger logger;
    private final TaskScheduler executor;
    private final int delayPeriodSeconds;
    private Future<?> submittedLoggingTask;

    /**
     * Constructs a new ClientConnectivityLogger.
     * @param loggingService the logging service.
     * @param executor the scheduled executor this class will use to
     *                 run logging tasks.
     * @param properties the hazelcast properties, providing the
     *                   logPeriodSeconds.
     */
    public ClientConnectivityLogger(LoggingService loggingService, TaskScheduler executor,
                                    HazelcastProperties properties) {
        this.logger = loggingService.getLogger(ClientConnectivityLogger.class);
        this.executor = executor;
        int periodSeconds = properties.getInteger(ClientProperty.CLIENT_CONNECTIVITY_LOGGING_DELAY_SECONDS);
        if (periodSeconds <= 0 && logger.isFineEnabled()) {
            logger.fine("Client connectivity logging is disabled.");
        }
        this.delayPeriodSeconds = periodSeconds;
    }

    /**
     * Submits a logging task to the executor that logs the connectivity of the client to the cluster.
     * The task will log the connectivity of the client with a delay of {@code logPeriodSeconds} seconds.
     * If a logging task is already submitted, then the current caller will cancel the task and resubmit.
     * <p>
     * Called from {@link com.hazelcast.client.impl.connection.tcp.TcpClientConnectionManager} under mutex.
     *
     * @param connectedMembers the list of members the client is connected to.
     * @param allMembers the list of all members the client has knowledge of,
     *                           including those with no active connection.
     */
    public void submitLoggingTask(Collection<Member> connectedMembers, Collection<Member> allMembers) {
        if (delayPeriodSeconds <= 0) {
            return;
        }

        if ((submittedLoggingTask != null && !submittedLoggingTask.isDone())) {
            submittedLoggingTask.cancel(true);
        }

        submittedLoggingTask = executor.schedule(()
                        -> logger.info(connectivityLog(connectedMembers, allMembers)),
                delayPeriodSeconds, SECONDS);

    }

    /**
     * Returns a string representation of the connectivity of the client to the cluster.
     * <p>
     * If a member is not on the effective member list, it is marked "- disconnected".
     * If a member is on the effective member list, it is marked "- connected".
     * <pre>
     * Client Connectivity [4] {
     *     Member [127.0.0.1]:5701 - 676956a0-0b86-4097-a1e1-42b01b0a664c - disconnected
     *     Member [127.0.0.1]:5702 - c1a6449f-432f-45ba-9570-52b8ec90ce89 - disconnected
     *     Member [127.0.0.1]:5703 - 76463a61-0c4b-4180-8970-56b14b156f4c - connected
     *     Member [127.0.0.1]:5704 - 7a172832-8734-4eef-80a9-656bf1e28c2a - connected
     * }
     * </pre>
     *
     * @param connectedMembers the list of members the client is connected to.
     * @param allMembers the list of all members the client has knowledge of
     * @return a string representation of the connectivity of the client to the cluster.
     */
    private String connectivityLog(Collection<Member> connectedMembers, Collection<Member> allMembers) {
        StringBuilder sb = new StringBuilder(lineSeparator());
        sb.append(lineSeparator());
        sb.append("Client Connectivity [");
        sb.append(allMembers.size());
        sb.append("] {");
        for (Member member : allMembers) {
            if (connectedMembers.contains(member)) {
                sb.append(lineSeparator()).append("\t").append(member).append(" - connected");
            } else {
                sb.append(lineSeparator()).append("\t").append(member).append(" - disconnected");
            }
        }
        sb.append(lineSeparator()).append("}").append(lineSeparator());
        return sb.toString();
    }

    /**
     * Terminates a submitted logging task.
     */
    public void terminate() {
        if (submittedLoggingTask != null && !submittedLoggingTask.isDone()) {
            submittedLoggingTask.cancel(true);
        }
    }
}
