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

import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.impl.job.deployment.classloader.JobClassLoader;
import com.hazelcast.jet.impl.job.deployment.classloader.ResourceStream;
import java.io.IOException;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public abstract class AbstractDeploymentStorage<S> implements DeploymentStorage {
    protected final Map<ResourceDescriptor, S> resources = new ConcurrentHashMap<>();

    protected final JobConfig config;

    private ClassLoader classLoader;

    private volatile boolean finalized;

    public AbstractDeploymentStorage(JobConfig config) {
        this.config = config;
    }

    public ClassLoader getClassLoader() {
        return classLoader;
    }

    public abstract ResourceStream asResourceStream(S resource) throws IOException;

    protected abstract void setChunk(S resource, Chunk chunk);

    protected abstract S createResource(ResourceDescriptor descriptor);

    public Map<ResourceDescriptor, ResourceStream> getResources() throws IOException {
        Map<ResourceDescriptor, ResourceStream> resourceStreams = new LinkedHashMap<>(resources.size());

        for (Map.Entry<ResourceDescriptor, S> entry : this.resources.entrySet()) {
            resourceStreams.put(entry.getKey(), asResourceStream(entry.getValue()));
        }

        return resourceStreams;
    }

    public synchronized void receiveChunk(Chunk chunk) {
        ResourceDescriptor descriptor = chunk.getDescriptor();
        if (!resources.containsKey(descriptor)) {
            createResource(descriptor);
        }
        S resource = resources.get(descriptor);
        setChunk(resource, chunk);
    }


    public void finish() {
        if (finalized) {
            return;
        }

        finalized = true;
        AccessController.doPrivileged((PrivilegedAction<Void>) () -> {
            classLoader = new JobClassLoader(AbstractDeploymentStorage.this);
            return null;
        });
    }

}
