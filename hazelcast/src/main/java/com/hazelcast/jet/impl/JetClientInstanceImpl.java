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
import com.hazelcast.internal.util.UuidUtil;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.impl.operation.GetJobIdsOperation.GetJobIdsResult;
import com.hazelcast.jet.impl.util.ExceptionUtil;
import com.hazelcast.logging.ILogger;

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

import static com.hazelcast.internal.util.Sha256Util.calculateSha256Hex;
import static com.hazelcast.jet.impl.operation.GetJobIdsOperation.ALL_JOBS;
import static com.hazelcast.jet.impl.util.ExceptionUtil.rethrow;
import static com.hazelcast.jet.impl.util.ExceptionUtil.sneakyThrow;

/**
 * Client-side {@code JetInstance} implementation
 */
public class JetClientInstanceImpl extends AbstractJetInstance<UUID> {

    public static final String PART_SIZE = "hazelcast.jobupload.partsize";

    private static final int DEFAULT_PART_SIZE = 10_000_000;
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
    public void uploadJob(@Nonnull Path jarPath, String snapshotName, String jobName, String mainClass,
                          @Nonnull List<String> jobParameters) {

        try {
            UUID sessionId = UuidUtil.newSecureUUID();
            long jarSize = Files.size(jarPath);
            String sha256Hex = calculateSha256Hex(jarPath);

            // Send job meta data
            sendJobMetaData(sessionId, sha256Hex, jarSize, snapshotName, jobName, mainClass, jobParameters);

            sendJobMultipart(jarPath, sessionId, jarSize);

        } catch (IOException | NoSuchAlgorithmException exception) {
            sneakyThrow(exception);
        }
    }

    private void sendJobMetaData(UUID sessionId, String md5Hex, long jarSize, String snapshotName, String jobName,
                                 String mainClass, List<String> jobParameters) {

        ClientMessage jobMetaDataRequest = JetUploadJobMetaDataCodec.encodeRequest(sessionId, md5Hex, jarSize, snapshotName,
                jobName, mainClass, jobParameters);
        invokeRequestOnMasterAndDecodeResponse(jobMetaDataRequest, JetUploadJobMetaDataCodec::decodeResponse);
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

    // Calculate the buffer size from System.properties if defined, otherwise use default value
    protected int calculatePartBufferSize() {
        int partSize = 0;
        try {
            partSize = Integer.getInteger(PART_SIZE, DEFAULT_PART_SIZE);
        } catch (Exception ignored) {
            //ignore the exception
        }
        return partSize;
    }

    protected int calculateTotalParts(long jarSize, int partSize) {
        return (int) Math.ceil(jarSize / (double) partSize);
    }

    // Calculate the MD5 of given file

}
