/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.SubmitJobParameters;
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
import java.util.Objects;
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
     * @deprecated Since 5.3, to be removed in 6.0. Use {@link #getJobAndSqlSummaryList()} instead
     */
    @Nonnull
    @Deprecated
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
    public void submitJobFromJar(@Nonnull SubmitJobParameters submitJobParameters) {
        try {
            UUID sessionId = UuidUtil.newSecureUUID();

            validateParameterObject(submitJobParameters);

            Path jarPath = submitJobParameters.getJarPath();
            // Calculate some parameters for JobMetadata
            String fileNameWithoutExtension = getFileNameWithoutExtension(jarPath);
            String sha256Hex = calculateSha256Hex(jarPath);
            long jarSize = Files.size(jarPath);

            // Send job meta data
            boolean result = sendJobMetaData(sessionId, fileNameWithoutExtension, sha256Hex, submitJobParameters);
            if (result) {
                logFine(getLogger(), "Submitted JobMetaData successfully for jarPath: %s", jarPath);
            }
            // Send job parts
            sendJobMultipart(jarPath, sessionId, jarSize);

        } catch (IOException | NoSuchAlgorithmException exception) {
            sneakyThrow(exception);
        }
    }

    // Validate the parameters used by the client
    protected void validateParameterObject(SubmitJobParameters parameterObject) {
        // Check that parameter is not null, because it is used to access the file
        if (Objects.isNull(parameterObject.getJarPath())) {
            throw new JetException("jarPath can not be null");
        }

        // Check that parameter is not null, because it is used by the JetUploadJobMetaDataCodec
        if (Objects.isNull(parameterObject.getJobParameters())) {
            throw new JetException("jobParameters can not be null");
        }
    }

    // This method is public for testing purposes.
    public String getFileNameWithoutExtension(Path jarPath) {
        String fileName = jarPath.getFileName().toString();
        if (!fileName.endsWith(".jar")) {
            throw new JetException("File name extension should be .jar");
        }
        fileName = fileName.substring(0, fileName.lastIndexOf('.'));
        return fileName;
    }

    private boolean sendJobMetaData(UUID sessionId, String fileNameWithoutExtension, String sha256Hex,
                                    SubmitJobParameters submitJobParameters) {
        ClientMessage jobMetaDataRequest = JetUploadJobMetaDataCodec.encodeRequest(sessionId,
                fileNameWithoutExtension, sha256Hex,
                submitJobParameters.getSnapshotName(),
                submitJobParameters.getJobName(),
                submitJobParameters.getMainClass(),
                submitJobParameters.getJobParameters());

        return invokeRequestOnMasterAndDecodeResponse(jobMetaDataRequest, JetUploadJobMetaDataCodec::decodeResponse);
    }

    private void sendJobMultipart(Path jarPath, UUID sessionId, long jarSize)
            throws IOException, NoSuchAlgorithmException {
        int partSize = calculatePartBufferSize();

        int totalParts = calculateTotalParts(jarSize, partSize);

        File file = jarPath.toFile();

        byte[] data = new byte[partSize];

        try (FileInputStream fileInputStream = new FileInputStream(file)) {

            for (int currentPartNumber = 1; currentPartNumber <= totalParts; currentPartNumber++) {

                // Read data
                int bytesRead = fileInputStream.read(data);

                String sha256Hex = Sha256Util.calculateSha256Hex(data, bytesRead);
                //Send the part
                ClientMessage jobDataRequest = JetUploadJobMultipartCodec.encodeRequest(sessionId, currentPartNumber,
                        totalParts, data, bytesRead, sha256Hex);
                boolean result = invokeRequestOnMasterAndDecodeResponse(jobDataRequest,
                        JetUploadJobMultipartCodec::decodeResponse);
                if (result) {
                    logFine(getLogger(), "Submitted Job Part successfully for jarPath: %s PartNumber %d",
                            jarPath, currentPartNumber);
                }

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
        return properties.getInteger(JOB_UPLOAD_PART_SIZE);
    }

    protected int calculateTotalParts(long jarSize, int partSize) {
        return (int) Math.ceil(jarSize / (double) partSize);
    }

    // This method is public for testing purposes. Currently, we can not mock static methods with Mockito because
    // enabling "Mock Maker Inline" is breaking other tests.
    // When it is enabled in the future, remove this method and directly mock Sha256Util
    public String calculateSha256Hex(Path jarPath) throws IOException, NoSuchAlgorithmException {
        return Sha256Util.calculateSha256Hex(jarPath);
    }
}
