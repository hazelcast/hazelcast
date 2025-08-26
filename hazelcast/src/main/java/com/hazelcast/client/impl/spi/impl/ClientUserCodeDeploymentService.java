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

package com.hazelcast.client.impl.spi.impl;

import static com.hazelcast.internal.util.EmptyStatement.ignore;

import com.hazelcast.client.config.ClientUserCodeDeploymentConfig;
import com.hazelcast.client.impl.clientside.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.ClientDeployClassesCodec;
import com.hazelcast.internal.namespace.UserCodeNamespaceService;
import com.hazelcast.internal.nio.ClassLoaderUtil;
import com.hazelcast.jet.impl.util.ReflectionUtils;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.jar.JarEntry;
import java.util.jar.JarInputStream;

/**
 * @deprecated "User Code Deployment" is replaced by the "User Code Namespaces" feature
 * @see UserCodeNamespaceService
 */
@Deprecated(since = "5.4", forRemoval = true)
public class ClientUserCodeDeploymentService {
    private final ClientUserCodeDeploymentConfig clientUserCodeDeploymentConfig;
    private final ClassLoader configClassLoader;
    //List<Map.Entry> is used instead of Map to comply with generated code of client protocol
    private final List<Map.Entry<String, byte[]>> classDefinitionList = new ArrayList<>();

    public ClientUserCodeDeploymentService(ClientUserCodeDeploymentConfig clientUserCodeDeploymentConfig,
                                           ClassLoader configClassLoader) {
        this.clientUserCodeDeploymentConfig = clientUserCodeDeploymentConfig;
        this.configClassLoader = configClassLoader != null ? configClassLoader : Thread.currentThread().getContextClassLoader();
    }

    public void start() throws IOException, ClassNotFoundException {
        if (!clientUserCodeDeploymentConfig.isEnabled()) {
            return;
        }
        loadClassesFromJars();
        loadClasses();
    }

    private void loadClasses() throws ClassNotFoundException {
        for (String className : clientUserCodeDeploymentConfig.getClassNames()) {
            String resource = ReflectionUtils.toClassResourceId(className);
            try (InputStream is = configClassLoader.getResourceAsStream(resource)) {
                if (is == null) {
                    throw new ClassNotFoundException(resource);
                }
                byte[] bytes = is.readAllBytes();
                classDefinitionList.add(new AbstractMap.SimpleEntry<>(className, bytes));
            } catch (IOException e) {
                ignore(e);
            }
        }
    }

    private void loadClassesFromJars() throws IOException {
        try (ByteArrayOutputStream os = new ByteArrayOutputStream()) {
            for (String jarPath : clientUserCodeDeploymentConfig.getJarPaths()) {
                loadClassesFromJar(os, jarPath);
            }
        }
    }

    private void loadClassesFromJar(ByteArrayOutputStream os, String jarPath) throws IOException {
        try (JarInputStream inputStream = getJarInputStream(jarPath)) {
            JarEntry entry;
            do {
                entry = inputStream.getNextJarEntry();
                if (entry == null) {
                    break;
                }

                String className = ClassLoaderUtil.extractClassName(entry.getName());
                if (className == null) {
                    continue;
                }
                byte[] classDefinition = readClassDefinition(inputStream, os);
                inputStream.closeEntry();
                classDefinitionList.add(new AbstractMap.SimpleEntry<>(className, classDefinition));
            } while (true);
        }
    }

    private JarInputStream getJarInputStream(String jarPath) throws IOException {
        File file = new File(jarPath);
        if (file.exists()) {
            return new JarInputStream(new FileInputStream(file));
        }

        try {
            URL url = URI.create(jarPath).toURL();
            return new JarInputStream(url.openStream());
        } catch (IllegalArgumentException | MalformedURLException e) {
            ignore(e);
        }

        InputStream inputStream = configClassLoader.getResourceAsStream(jarPath);
        if (inputStream == null) {
            throw new FileNotFoundException("File could not be found in " + jarPath + "  and resources/" + jarPath);
        }
        return new JarInputStream(inputStream);
    }

    private byte[] readClassDefinition(JarInputStream inputStream, ByteArrayOutputStream os) throws IOException {
        os.reset();
        while (true) {
            int v = inputStream.read();
            if (v == -1) {
                break;
            }
            os.write(v);
        }
        return os.toByteArray();
    }

    public void deploy(HazelcastClientInstanceImpl client) throws ExecutionException, InterruptedException {
        if (!clientUserCodeDeploymentConfig.isEnabled() || classDefinitionList.isEmpty()) {
            return;
        }

        ClientMessage request = ClientDeployClassesCodec.encodeRequest(classDefinitionList);
        ClientInvocation invocation = new ClientInvocation(client, request, null);
        ClientInvocationFuture future = invocation.invokeUrgent();
        future.get();
    }

    //testing purposes
    public List<Map.Entry<String, byte[]>> getClassDefinitionList() {
        return classDefinitionList;
    }
}
