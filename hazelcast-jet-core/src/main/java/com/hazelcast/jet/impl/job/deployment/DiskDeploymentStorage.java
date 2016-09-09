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

package com.hazelcast.jet.impl.job.deployment;

import com.hazelcast.jet.JetException;
import com.hazelcast.jet.impl.job.JobContext;
import com.hazelcast.jet.impl.job.deployment.classloader.ResourceStream;
import com.hazelcast.logging.ILogger;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static com.hazelcast.jet.impl.util.JetUtil.unchecked;

public class DiskDeploymentStorage extends AbstractDeploymentStorage<File> {

    private final File jobDirectory;
    private final ILogger logger;

    private long fileNameCounter = 1;

    public DiskDeploymentStorage(JobContext jobContext, String jobName) {
        super(jobContext.getJobConfig());
        this.logger = jobContext.getNodeEngine().getLogger(getClass());
        this.jobDirectory = createJobDirectory(jobName, getDeploymentDirectory());
    }

    private File createJobDirectory(String jobName, String deploymentDirectory) {
        Path jobPath = Paths.get(deploymentDirectory + File.pathSeparator + "job_" + jobName);
        int directoryNameCounter = 0;
        do {
            try {
                jobPath = Files.createDirectory(jobPath);
            } catch (FileAlreadyExistsException e) {
                jobPath = Paths.get(deploymentDirectory + File.pathSeparator + "job_" + jobName + "_"
                        + directoryNameCounter++);
            } catch (IOException e) {
                throw unchecked(e);
            }

        } while (!jobPath.toFile().exists());
        return jobPath.toFile();
    }

    private String getDeploymentDirectory() {
        String deploymentDirectory = config.getDeploymentDirectory();
        if (deploymentDirectory == null) {
            try {
                deploymentDirectory = Files.createTempDirectory("hazelcast-jet-").toString();
            } catch (IOException e) {
                throw unchecked(e);
            }
        } else {
            Path path = Paths.get(deploymentDirectory);
            try {
                Files.createDirectories(path);
            } catch (IOException e) {
                throw unchecked(e);
            }
        }
        return deploymentDirectory;
    }

    @Override
    public ResourceStream asResourceStream(File resource) throws IOException {
        InputStream fileInputStream = new FileInputStream(resource);
        try {
            return new ResourceStream(fileInputStream, resource.toURI().toURL().toString());
        } catch (Throwable e) {
            fileInputStream.close();
            throw unchecked(e);
        }
    }

    @Override
    protected void setChunk(File file, Chunk chunk) {
        try (RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rws")) {
            int offset = (chunk.getSequence() - 1) * chunk.getChunkSize();
            randomAccessFile.seek(offset);
            randomAccessFile.write(chunk.getBytes());
        } catch (Exception e) {
            throw unchecked(e);
        }
    }

    @Override
    protected File createResource(DeploymentDescriptor descriptor) {
        String path = getPath();
        File file = new File(path);
        if (!file.exists()) {
            try {
                if (!file.createNewFile()) {
                    throw new JetException("Deployment failure, unable to create a file -> " + path);
                }
            } catch (IOException e) {
                throw new JetException("Deployment failure, unable to create a file -> " + path);
            }
        }
        if (!file.canWrite()) {
            throw new JetException("Unable to write to the file " + path + " - file is not permitted to write");
        }
        resources.put(descriptor, file);
        return file;
    }

    private String getPath() {
        return jobDirectory + File.pathSeparator + "resource" + fileNameCounter++;
    }

    @Override
    public void cleanup() {
        if (jobDirectory != null) {
            delete(jobDirectory);
        }
    }

    private void delete(File file) {
        if (file.isDirectory()) {
            File[] files = file.listFiles();
            if (files != null) {
                for (File c : files) {
                    delete(c);
                }
            }
        }

        if (!file.delete()) {
            logger.info("Can't delete file " + file.getName());
        }
    }
}
