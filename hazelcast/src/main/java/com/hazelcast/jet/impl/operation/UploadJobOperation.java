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

package com.hazelcast.jet.impl.operation;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.impl.HazelcastBootstrap;
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.impl.execution.init.JetInitDataSerializerHook;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static com.hazelcast.jet.impl.util.ExceptionUtil.peel;
import static com.hazelcast.jet.impl.util.ExceptionUtil.sneakyThrow;

/**
 * Resumes the execution of a suspended job.
 */
public class UploadJobOperation extends AsyncJobOperation {

    private String snapshotName;
    private String jobName;

    private String mainClass;

    private List<String> jobParameters;

    private byte[] jarData;

    public UploadJobOperation() {
    }

    public UploadJobOperation(String snapshotName, String jobName, String mainClass, List<String> jobParameters, byte[] jarData) {
        this.snapshotName = snapshotName;
        this.jobName = jobName;
        this.mainClass = mainClass;
        this.jobParameters = jobParameters;
        this.jarData = jarData;

    }


    @Override
    public CompletableFuture<Boolean> doRun() {

        return CompletableFuture.supplyAsync(() -> {
            try {

                Config config = getNodeEngine().getConfig();
                JetConfig jetConfig = config.getJetConfig();
                if (!jetConfig.isResourceUploadEnabled()) {
                    throw new JetException("Resource upload is not enabled");
                }
                Path tempFile = Files.createTempFile("runjob", ".jar");

                Files.write(tempFile, jarData, StandardOpenOption.TRUNCATE_EXISTING);

                HazelcastBootstrap.executeJar(this::getHazelcastClient,
                        tempFile.toString(),
                        snapshotName,
                        jobName,
                        mainClass,
                        jobParameters,
                        false
                );

            } catch (Exception exception) {
                Throwable peel = peel(exception);
                sneakyThrow(peel);
            }
            return true;
        });
    }


    private HazelcastInstance getHazelcastClient() {
        return getNodeEngine().getHazelcastInstance();
    }

    @Override
    public int getClassId() {
        return JetInitDataSerializerHook.UPLOAD_JOB_OP;
    }
}
