/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.JetService;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.impl.operation.GetJobIdsOperation.GetJobIdsResult;
import com.hazelcast.jet.impl.submitjob.clientside.execute.JobExecuteCall;
import com.hazelcast.jet.impl.submitjob.clientside.upload.JobUploadCall;
import com.hazelcast.jet.impl.submitjob.clientside.validator.SubmitJobParametersValidator;
import com.hazelcast.jet.impl.util.ExceptionUtil;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.logging.ILogger;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.security.auth.Subject;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;

import static com.hazelcast.internal.util.ExceptionUtil.sneakyThrow;
import static com.hazelcast.jet.impl.operation.GetJobIdsOperation.ALL_JOBS;
import static com.hazelcast.jet.impl.util.ExceptionUtil.rethrow;

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

    private Map<UUID, GetJobIdsResult> getJobsInt(String onlyName, Long onlyJobId) {
        return invokeRequestOnAnyMemberAndDecodeResponse(
                JetGetJobIdsCodec.encodeRequest(onlyName, onlyJobId == null ? ALL_JOBS : onlyJobId),
                resp -> {
                    Data responseSerialized = JetGetJobIdsCodec.decodeResponse(resp).response;
                    return serializationService.toObject(responseSerialized);
                });
    }

    protected GetJobIdsResult getJobByName(String onlyName) {
        var result = getJobsInt(onlyName, null);
        // Only normal jobs can have a name.
        // Since information about normal jobs is requested from the master node only, a single result is expected.
        assert result.size() == 1 : "Exactly one result is expected";
        return result.values().stream().findFirst().orElseThrow();
    }

    protected Map<UUID, GetJobIdsResult> getJobsById(Long onlyJobId) {
        return getJobsInt(null, onlyJobId);
    }

    protected Map<UUID, GetJobIdsResult> getAllJobs() {
        return getJobsInt(null, null);
    }

    @Nonnull
    @Override
    public JetConfig getConfig() {
        throw new UnsupportedOperationException("Jet Configuration is not available on the client");
    }

    /**
     * Returns a list of jobs and a summary of their details.
     * @deprecated to be removed in 6.0. Use {@link #getJobAndSqlSummaryList()} instead
     */
    @Nonnull
    @Deprecated(since = "5.3", forRemoval = true)
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

    @Override
    public Job newJobProxy(long jobId,
                           boolean isLightJob,
                           @Nonnull Object jobDefinition,
                           @Nonnull JobConfig config,
                           @Nullable Subject subject) {
        if (subject != null) {
            throw new UnsupportedOperationException("Submitting a job with subject is not allowed for client");
        }
        return new ClientJobProxy(client, jobId, isLightJob, jobDefinition, config);
    }

    /**
     * For the client side, the jar is uploaded to a random cluster member and then this member runs the main method to
     * start the job.
     * The jar should have a main method that submits a job with {@link #newJob(Pipeline)} or
     * {@link #newLightJob(Pipeline)} methods
     * <p>
     * The upload operation is performed in parts to avoid OOM exceptions on the client and member.
     * For Java clients the part size is controlled by {@link com.hazelcast.client.properties.ClientProperty#JOB_UPLOAD_PART_SIZE}
     * property
     * <p>
     * Limitations for the member side jobs:
     * <ul>
     *     <li>The job can only access resources on the member or cluster. This is different from the jobs submitted from
     *     the hz-cli tool. A job submitted from hz-cli tool creates a local HazelcastInstance on the client JVM and
     *     connects to cluster. Therefore, the job can access local resources. This is not the case for the jar
     *     uploaded to a member.
     *     </li>
     * </ul>
     * <p>
     * Note : Job submission and job startup are two different things.
     * The job submission is a synchronous process but job startup is an asynchronous process. This call can only detect
     * failures during job submission. If you want to check for job startup failures, you need may use one of these
     * methods and check for Job state
     * <ul>
     * <li>{@link JetService#getJob(String)}</li>
     * <li>{@link JetService#getJobs()} </li>
     *
     * @throws JetException on submission error.
     */
    public void submitJobFromJar(@Nonnull SubmitJobParameters submitJobParameters) {
        if (submitJobParameters.isJarOnMember()) {
            executeJobFromJar(submitJobParameters);
        } else {
            uploadJobFromJar(submitJobParameters);
        }
    }

    private void executeJobFromJar(@Nonnull SubmitJobParameters submitJobParameters) {
        try {
            SubmitJobParametersValidator.validateJarOnMember(submitJobParameters);

            Path jarPath = submitJobParameters.getJarPath();
            JobExecuteCall jobExecuteCall = initializeJobExecuteCall(submitJobParameters.getJarPath());

            // Send only job metadata
            getLogger().fine("Submitting JobMetaData for jarPath: %s", jarPath);
            sendJobMetaDataForExecute(jobExecuteCall, submitJobParameters);
            getLogger().fine("Job execution from jar '%s' finished successfully", jarPath);
        } catch (Exception exception) {
            sneakyThrow(exception);
        }
    }

    private void uploadJobFromJar(@Nonnull SubmitJobParameters submitJobParameters) {
        try {
            SubmitJobParametersValidator.validateJarOnClient(submitJobParameters);

            Path jarPath = submitJobParameters.getJarPath();
            JobUploadCall jobUploadCall = initializeJobUploadCall(submitJobParameters.getJarPath());

            // First send job metadata
            getLogger().fine("Submitting JobMetaData for jarPath: %s", jarPath);
            sendJobMetaDataForUpload(jobUploadCall, submitJobParameters);

            // Then send job parts
            sendJobMultipart(jobUploadCall, jarPath);
            getLogger().fine("Job upload from jar '%s' finished successfully", jarPath);
        } catch (IOException | NoSuchAlgorithmException exception) {
            sneakyThrow(exception);
        }
    }

    // This method is public for testing purposes.
    public JobUploadCall initializeJobUploadCall(Path jarPath)
            throws IOException, NoSuchAlgorithmException {
        JobUploadCall jobUploadCall = new JobUploadCall();
        jobUploadCall.initializeJobUploadCall(client, jarPath);

        return jobUploadCall;
    }

    public JobExecuteCall initializeJobExecuteCall(Path jarPath) {
        JobExecuteCall jobExecuteCall = new JobExecuteCall();
        jobExecuteCall.initializeJobExecuteCall(client, jarPath);

        return jobExecuteCall;
    }

    private void sendJobMetaDataForUpload(JobUploadCall jobUploadCall,
                                          SubmitJobParameters submitJobParameters) {
        ClientMessage jobMetaDataRequest = JetUploadJobMetaDataCodec.encodeRequest(
                jobUploadCall.getSessionId(),
                submitJobParameters.isJarOnMember(),
                jobUploadCall.getFileNameWithoutExtension(),
                jobUploadCall.getSha256HexOfJar(),
                submitJobParameters.getSnapshotName(),
                submitJobParameters.getJobName(),
                submitJobParameters.getMainClass(),
                submitJobParameters.getJobParameters());

        invokeRequestNoRetryOnRandom(jobUploadCall.getMemberUuid(), jobMetaDataRequest);
    }

    private void sendJobMetaDataForExecute(JobExecuteCall jobExecuteCall, SubmitJobParameters submitJobParameters) {
        ClientMessage jobMetaDataRequest = JetUploadJobMetaDataCodec.encodeRequest(
                jobExecuteCall.getSessionId(),
                submitJobParameters.isJarOnMember(),
                jobExecuteCall.getJarPath(),
                jobExecuteCall.getSha256HexOfJar(),
                submitJobParameters.getSnapshotName(),
                submitJobParameters.getJobName(),
                submitJobParameters.getMainClass(),
                submitJobParameters.getJobParameters());

        invokeRequestNoRetryOnRandom(jobExecuteCall.getMemberUuid(), jobMetaDataRequest);
    }

    private void sendJobMultipart(JobUploadCall jobUploadCall, Path jarPath)
            throws IOException, NoSuchAlgorithmException {
        byte[] partBuffer = jobUploadCall.allocatePartBuffer();

        try (InputStream fileInputStream = Files.newInputStream(jarPath)) {

            // Start from part #1
            for (int currentPartNumber = 1; currentPartNumber <= jobUploadCall.getTotalParts(); currentPartNumber++) {

                int bytesRead = fileInputStream.read(partBuffer);

                byte[] dataToSend = jobUploadCall.getDataToSend(partBuffer, bytesRead);

                String sha256Hex = Sha256Util.calculateSha256Hex(dataToSend);
                //Send the part
                ClientMessage jobMultipartRequest = JetUploadJobMultipartCodec.encodeRequest(
                        jobUploadCall.getSessionId(),
                        currentPartNumber,
                        jobUploadCall.getTotalParts(), dataToSend, bytesRead, sha256Hex);

                getLogger().fine("Submitting Job Part for jarPath: %s PartNumber %d/%d",
                        jarPath, currentPartNumber, jobUploadCall.getTotalParts());

                invokeRequestNoRetryOnRandom(jobUploadCall.getMemberUuid(), jobMultipartRequest);
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

    // Do not retry on random member if invocation fails
    private void invokeRequestNoRetryOnRandom(UUID uuid, ClientMessage request) {
        ClientInvocation invocation = new ClientInvocation(client, request, null, uuid);
        invocation.disallowRetryOnRandom();
        invoke(invocation);
    }


    // Retry on random member if invocation fails
    private <S> S invokeRequestAndDecodeResponse(UUID uuid, ClientMessage request,
                                                 Function<ClientMessage, Object> decoder) {
        ClientInvocation invocation = new ClientInvocation(client, request, null, uuid);
        return invoke(decoder, invocation);
    }

    private <S> S invoke(Function<ClientMessage, Object> decoder, ClientInvocation invocation) {
        try {
            ClientMessage response = invocation.invoke().get();
            return serializationService.toObject(decoder.apply(response));
        } catch (Exception exception) {
            throw rethrow(exception);
        }
    }

    private void invoke(ClientInvocation invocation) {
        try {
            invocation.invoke().get();
        } catch (Exception exception) {
            throw rethrow(exception);
        }
    }
}
