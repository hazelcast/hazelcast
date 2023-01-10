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

package com.hazelcast.jet.impl;

import com.hazelcast.client.impl.client.DistributedObjectInfo;
import com.hazelcast.client.impl.clientside.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.ClientGetDistributedObjectsCodec;
import com.hazelcast.client.impl.protocol.codec.JetExistsDistributedObjectCodec;
import com.hazelcast.client.impl.protocol.codec.JetGetJobAndSqlSummaryListCodec;
import com.hazelcast.client.impl.protocol.codec.JetGetJobIdsCodec;
import com.hazelcast.client.impl.protocol.codec.JetGetJobSummaryListCodec;
import com.hazelcast.client.impl.protocol.codec.JetUploadJobMetaDataCodec;
import com.hazelcast.client.impl.protocol.codec.JetUploadJobMultipartCodec;
import com.hazelcast.client.impl.spi.impl.ClientInvocation;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.util.Sha256Util;
import com.hazelcast.internal.util.UuidUtil;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.impl.operation.GetJobIdsOperation.GetJobIdsResult;
import com.hazelcast.jet.impl.util.ExceptionUtil;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.properties.HazelcastProperties;

import javax.annotation.Nonnull;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;

import static com.hazelcast.client.properties.ClientProperty.JOB_UPLOAD_PART_SIZE;
import static com.hazelcast.jet.impl.operation.GetJobIdsOperation.ALL_JOBS;
import static com.hazelcast.jet.impl.util.ExceptionUtil.rethrow;
import static com.hazelcast.jet.impl.util.ExceptionUtil.sneakyThrow;
import static com.hazelcast.jet.impl.util.LoggingUtil.logFine;

/**
 * Client-side {@code JetInstance} implementation
 */
public class JetClientInstanceImpl extends AbstractJetInstance<UUID> {

    private final HazelcastClientInstanceImpl client;
    private final SerializationService serializationService;

    public JetClientInstanceImpl(HazelcastClientInstanceImpl hazelcastInstance) {
        super(hazelcastInstance);
        this.client = hazelcastInstance;
        this.serializationService = client.getSerializationService();

        ExceptionUtil.registerJetExceptions(hazelcastInstance.getClientExceptionFactory());
    }

    @Override
    public UUID getMasterId() {
        return client.getClientClusterService().getMasterMember().getUuid();
    }

    @Override
    public Map<UUID, GetJobIdsResult> getJobsInt(String onlyName, Long onlyJobId) {
        return invokeRequestOnAnyMemberAndDecodeResponse(
                JetGetJobIdsCodec.encodeRequest(onlyName, onlyJobId == null ? ALL_JOBS : onlyJobId),
                resp -> {
                    Data responseSerialized = JetGetJobIdsCodec.decodeResponse(resp).response;
                    return serializationService.toObject(responseSerialized);
                });
    }

    @Nonnull
    @Override
    public JetConfig getConfig() {
        throw new UnsupportedOperationException("Jet Configuration is not available on the client");
    }

    /**
     * Returns a list of jobs and a summary of their details.
     */
    @Nonnull
    public List<JobSummary> getJobSummaryList() {
        return invokeRequestOnMasterAndDecodeResponse(JetGetJobSummaryListCodec.encodeRequest(),
                JetGetJobSummaryListCodec::decodeResponse);
    }

    /**
     * Returns a list of jobs and a summary of their details (including SQL ones).
     */
    @Nonnull
    public List<JobAndSqlSummary> getJobAndSqlSummaryList() {
        return invokeRequestOnMasterAndDecodeResponse(JetGetJobAndSqlSummaryListCodec.encodeRequest(),
                JetGetJobAndSqlSummaryListCodec::decodeResponse);
    }

    @Nonnull
    public HazelcastClientInstanceImpl getHazelcastClient() {
        return client;
    }

    @Override
    public boolean existsDistributedObject(@Nonnull String serviceName, @Nonnull String objectName) {
        return invokeRequestOnAnyMemberAndDecodeResponse(
                JetExistsDistributedObjectCodec.encodeRequest(serviceName, objectName),
                JetExistsDistributedObjectCodec::decodeResponse
        );
    }

    public List<DistributedObjectInfo> getDistributedObjects() {
        return invokeRequestOnAnyMemberAndDecodeResponse(
                ClientGetDistributedObjectsCodec.encodeRequest(),
                ClientGetDistributedObjectsCodec::decodeResponse
        );
    }

    @Override
    public Job newJobProxy(long jobId, UUID lightJobCoordinator) {
        return new ClientJobProxy(client, jobId, lightJobCoordinator);
    }

    public Job newJobProxy(long jobId, boolean isLightJob, @Nonnull Object jobDefinition, @Nonnull JobConfig config) {
        return new ClientJobProxy(client, jobId, isLightJob, jobDefinition, config);
    }

    @Override
    public void submitJobJar(@Nonnull Path jarPath, String snapshotName, String jobName, String mainClass,
                             @Nonnull List<String> jobParameters) {

        try {
            UUID sessionId = UuidUtil.newSecureUUID();

            String fileName = fileNameWithoutExtension(jarPath);
            String sha256Hex = calculateSha256Hex(jarPath);
            long jarSize = Files.size(jarPath);

            // Send job meta data
            boolean result = sendJobMetaData(sessionId, fileName, sha256Hex, snapshotName, jobName, mainClass,
                    jobParameters);
            if (result) {
                logFine(getLogger(), "Submitted JobMetaData successfully for jarPath: %s", jarPath);
            }
            // Send job parts
            sendJobMultipart(jarPath, sessionId, jarSize);

        } catch (IOException | NoSuchAlgorithmException exception) {
            sneakyThrow(exception);
        }
    }

    protected String fileNameWithoutExtension(Path jarPath) {
        String fileName = jarPath.getFileName().toString();
        if (fileName.endsWith(".jar")) {
            fileName = fileName.substring(0, fileName.lastIndexOf('.'));
        }
        return fileName;
    }

    public String calculateSha256Hex(Path jarPath) throws IOException, NoSuchAlgorithmException {
        return Sha256Util.calculateSha256Hex(jarPath);
    }

    private boolean sendJobMetaData(UUID sessionId, String fileName, String sha256Hex, String snapshotName,
                                    String jobName, String mainClass, List<String> jobParameters) {

        ClientMessage jobMetaDataRequest = JetUploadJobMetaDataCodec.encodeRequest(sessionId, fileName, sha256Hex,
                snapshotName, jobName, mainClass, jobParameters);
        return invokeRequestOnMasterAndDecodeResponse(jobMetaDataRequest, JetUploadJobMetaDataCodec::decodeResponse);
    }

    private void sendJobMultipart(Path jarPath, UUID sessionId, long jarSize) throws IOException {
        int partSize = calculatePartBufferSize();

        int totalParts = calculateTotalParts(jarSize, partSize);

        File file = jarPath.toFile();

        byte[] data = new byte[partSize];

        try (FileInputStream fileInputStream = new FileInputStream(file)) {

            for (int currentPartNumber = 1; currentPartNumber <= totalParts; currentPartNumber++) {

                // Read data
                int bytesRead = fileInputStream.read(data);

                //Send the part
                ClientMessage jobDataRequest = JetUploadJobMultipartCodec.encodeRequest(sessionId, currentPartNumber, totalParts,
                        data, bytesRead);
                invokeRequestOnMasterAndDecodeResponse(jobDataRequest, JetUploadJobMultipartCodec::decodeResponse);
            }
        }
    }

    @Override
    public ILogger getLogger() {
        return client.getLoggingService().getLogger(getClass());
    }

    private <S> S invokeRequestOnMasterAndDecodeResponse(ClientMessage request,
                                                         Function<ClientMessage, Object> decoder) {
        return invokeRequestAndDecodeResponse(getMasterId(), request, decoder);
    }

    private <S> S invokeRequestOnAnyMemberAndDecodeResponse(ClientMessage request,
                                                            Function<ClientMessage, Object> decoder) {
        return invokeRequestAndDecodeResponse(null, request, decoder);
    }

    private <S> S invokeRequestAndDecodeResponse(UUID uuid, ClientMessage request,
                                                 Function<ClientMessage, Object> decoder) {
        ClientInvocation invocation = new ClientInvocation(client, request, null, uuid);
        try {
            ClientMessage response = invocation.invoke().get();
            return serializationService.toObject(decoder.apply(response));
        } catch (Throwable t) {
            throw rethrow(t);
        }
    }

    // Calculate the buffer size from properties if defined, otherwise use default value
    protected int calculatePartBufferSize() {
        HazelcastProperties properties = client.getProperties();
        return properties.getIntegerOrDefault(JOB_UPLOAD_PART_SIZE);
    }

    protected int calculateTotalParts(long jarSize, int partSize) {
        return (int) Math.ceil(jarSize / (double) partSize);
    }
}
