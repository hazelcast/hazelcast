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

package com.hazelcast.spi.impl.operationservice.impl;

import com.hazelcast.internal.metrics.MetricsProvider;
import com.hazelcast.internal.metrics.MetricsRegistry;
import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.internal.partition.ReplicaErrorLogger;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.util.counters.MwCounter;
import com.hazelcast.internal.util.counters.SwCounter;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Packet;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.PacketHandler;
import com.hazelcast.spi.impl.operationservice.impl.responses.BackupResponse;
import com.hazelcast.spi.impl.operationservice.impl.responses.CallTimeoutResponse;
import com.hazelcast.spi.impl.operationservice.impl.responses.ErrorResponse;
import com.hazelcast.spi.impl.operationservice.impl.responses.NormalResponse;
import com.hazelcast.spi.impl.operationservice.impl.responses.Response;

import static com.hazelcast.internal.metrics.ProbeLevel.MANDATORY;
import static com.hazelcast.internal.util.counters.MwCounter.newMwCounter;
import static com.hazelcast.internal.util.counters.SwCounter.newSwCounter;

/**
 * Responsible for handling responses for invocations. Based on the content of the response packet, it will lookup the
 * Invocation from the InvocationRegistry and notify the Invocation.
 */
public final class ResponseHandler implements PacketHandler, MetricsProvider {

    private final ILogger logger;
    private final InternalSerializationService serializationService;
    private final InvocationRegistry invocationRegistry;
    private final NodeEngineImpl nodeEngine;
    @Probe(name = "responses[normal]", level = MANDATORY)
    private final SwCounter responsesNormal = newSwCounter();
    @Probe(name = "responses[timeout]", level = MANDATORY)
    private final SwCounter responsesTimeout = newSwCounter();
    @Probe(name = "responses[backup]", level = MANDATORY)
    private final MwCounter responsesBackup = newMwCounter();
    @Probe(name = "responses[error]", level = MANDATORY)
    private final SwCounter responsesError = newSwCounter();
    @Probe(name = "responses[missing]", level = MANDATORY)
    private final MwCounter responsesMissing = newMwCounter();

    public ResponseHandler(ILogger logger,
                           InternalSerializationService serializationService,
                           InvocationRegistry invocationRegistry,
                           NodeEngineImpl nodeEngine) {
        this.logger = logger;
        this.serializationService = serializationService;
        this.invocationRegistry = invocationRegistry;
        this.nodeEngine = nodeEngine;
    }

    @Override
    public void provideMetrics(MetricsRegistry metricsRegistry) {
        metricsRegistry.scanAndRegister(this, "operation.invocations");
    }

    @Override
    public void handle(Packet packet) throws Exception {
        Response response = serializationService.toObject(packet);
        Address sender = packet.getConn().getEndPoint();
        try {
            if (response instanceof NormalResponse) {
                NormalResponse normalResponse = (NormalResponse) response;
                notifyNormalResponse(
                        normalResponse.getCallId(),
                        normalResponse.getValue(),
                        normalResponse.getBackupCount(),
                        sender);
            } else if (response instanceof BackupResponse) {
                notifyBackupComplete(response.getCallId());
            } else if (response instanceof CallTimeoutResponse) {
                notifyCallTimeout(response.getCallId(), sender);
            } else if (response instanceof ErrorResponse) {
                ErrorResponse errorResponse = (ErrorResponse) response;
                notifyErrorResponse(
                        errorResponse.getCallId(),
                        errorResponse.getCause(),
                        sender);
            } else {
                logger.severe("Unrecognized response: " + response);
            }
        } catch (Throwable e) {
            logger.severe("While processing response...", e);
        }
    }

    public void notifyBackupComplete(long callId) {
        responsesBackup.inc();

        try {
            Invocation invocation = invocationRegistry.get(callId);

            // It can happen that a backup response is send without the Invocation being available anymore.
            // This is because the InvocationRegistry will automatically release invocations where the backup is
            // taking too much time.
            if (invocation == null) {
                if (logger.isFinestEnabled()) {
                    logger.finest("No Invocation found for backup response with callId " + callId);
                }
                return;
            }

            invocation.notifyBackupComplete();
        } catch (Exception e) {
            ReplicaErrorLogger.log(e, logger);
        }
    }

    void notifyErrorResponse(long callId, Object cause, Address sender) {
        responsesError.inc();
        Invocation invocation = invocationRegistry.get(callId);

        if (invocation == null) {
            responsesMissing.inc();
            if (nodeEngine.isRunning()) {
                logger.warning("No Invocation found for error response with callId: " + callId + " sent from " + sender);
            }
            return;
        }

        invocation.notifyError(cause);
    }

    void notifyNormalResponse(long callId, Object value, int backupCount, Address sender) {
        responsesNormal.inc();
        Invocation invocation = invocationRegistry.get(callId);

        if (invocation == null) {
            responsesMissing.inc();
            if (nodeEngine.isRunning()) {
                logger.warning("No Invocation found for normal response with callId " + callId + " sent from " + sender);
            }
            return;
        }
        invocation.notifyNormalResponse(value, backupCount);
    }

    void notifyCallTimeout(long callId, Address sender) {
        responsesTimeout.inc();
        Invocation invocation = invocationRegistry.get(callId);

        if (invocation == null) {
            responsesMissing.inc();
            if (nodeEngine.isRunning()) {
                logger.warning("No Invocation found for call timeout response with callId" + callId + " sent from " + sender);
            }
            return;
        }
        invocation.notifyCallTimeout();
    }
}
