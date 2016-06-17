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

package com.hazelcast.jet.impl.application.localization;

import com.hazelcast.jet.impl.application.LocalizationResourceDescriptor;
import com.hazelcast.jet.impl.application.localization.classloader.ApplicationClassLoader;
import com.hazelcast.jet.impl.application.localization.classloader.ResourceStream;
import com.hazelcast.jet.config.ApplicationConfig;

import java.io.IOException;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.LinkedHashMap;
import java.util.Map;

public abstract class AbstractLocalizationStorage<S> implements LocalizationStorage {
    protected final Map<LocalizationResourceDescriptor, S> resources = new LinkedHashMap<LocalizationResourceDescriptor, S>();

    protected final ApplicationConfig jetConfig;

    private ClassLoader classLoader;

    private volatile boolean accepted;

    public AbstractLocalizationStorage(ApplicationConfig config) {
        this.jetConfig = config;
    }

    public ClassLoader getClassLoader() {
        return classLoader;
    }

    public abstract ResourceStream asResourceStream(S resource) throws IOException;

    public Map<LocalizationResourceDescriptor, ResourceStream> getResources() throws IOException {
        Map<LocalizationResourceDescriptor, ResourceStream> resourceStreams =
                new LinkedHashMap<LocalizationResourceDescriptor, ResourceStream>(this.resources.size());

        for (Map.Entry<LocalizationResourceDescriptor, S> entry : this.resources.entrySet()) {
            resourceStreams.put(entry.getKey(), asResourceStream(entry.getValue()));
        }

        return resourceStreams;
    }

    public void receiveFileChunk(Chunk chunk) {
        LocalizationResourceDescriptor descriptor = chunk.getDescriptor();
        S resource = getResource(this.resources.get(descriptor), chunk);
        this.resources.put(descriptor, resource);
    }

    public ApplicationConfig getConfig() {
        return this.jetConfig;
    }

    protected abstract S getResource(S resource, Chunk chunk);

    public void accept() throws InvalidLocalizationException {
        if (this.accepted) {
            return;
        }

        this.accepted = true;
        AccessController.doPrivileged(new PrivilegedAction<Void>() {
            @Override
            public Void run() {
                classLoader = new ApplicationClassLoader(AbstractLocalizationStorage.this);
                return null;
            }
        });
    }
}
