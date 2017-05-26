/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.spi.impl;

import com.hazelcast.client.config.ClientUserCodeDeploymentConfig;
import com.hazelcast.client.impl.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.ClientDeployClassesCodec;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.IOUtil;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.jar.JarEntry;
import java.util.jar.JarInputStream;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.hazelcast.nio.IOUtil.toByteArray;

public class ClientUserCodeDeploymentService {

    private static final Pattern CLASS_PATTERN = Pattern.compile("(.*)\\.class$");
    private final ClientUserCodeDeploymentConfig clientUserCodeDeploymentConfig;
    private final ClassLoader configClassLoader;
    private final List<Map.Entry<String, byte[]>> classDefinitionList = new ArrayList<Map.Entry<String, byte[]>>();

    public ClientUserCodeDeploymentService(ClientUserCodeDeploymentConfig clientUserCodeDeploymentConfig,
                                           ClassLoader configClassLoader) {
        this.clientUserCodeDeploymentConfig = clientUserCodeDeploymentConfig;
        this.configClassLoader = configClassLoader != null ? configClassLoader : ClassLoader.getSystemClassLoader();
    }

    public void start() throws IOException {
        if (!clientUserCodeDeploymentConfig.isEnabled()) {
            return;
        }
        loadClassesFromJars();
        loadClasses();
    }

    private void loadClasses() throws IOException {
        for (String className : clientUserCodeDeploymentConfig.getClassNames()) {
            String resource = className.replace('.', '/').concat(".class");
            InputStream is = null;
            try {
                is = configClassLoader.getResourceAsStream(resource);
                if (is != null) {
                    byte[] bytes = toByteArray(is);
                    classDefinitionList.add(new AbstractMap.SimpleEntry<String, byte[]>(className, bytes));
                }
            } finally {
                IOUtil.closeResource(is);
            }
        }
    }

    private void loadClassesFromJars() throws IOException {
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        try {
            for (String jarPath : clientUserCodeDeploymentConfig.getJarPaths()) {
                loadClassesFromJar(os, jarPath);
            }
        } finally {
            IOUtil.closeResource(os);
        }
    }

    private void loadClassesFromJar(ByteArrayOutputStream os, String jarPath) throws IOException {
        JarInputStream inputStream = null;
        try {
            inputStream = getJarInputStream(jarPath);
            JarEntry entry;
            do {
                entry = inputStream.getNextJarEntry();
                if (entry == null) {
                    break;
                }

                String className = extractClassName(entry);
                if (className == null) {
                    continue;
                }
                byte[] classDefinition = readClassDefinition(inputStream, os);
                inputStream.closeEntry();
                classDefinitionList.add(new AbstractMap.SimpleEntry<String, byte[]>(className, classDefinition));
            } while (true);
        } finally {
            IOUtil.closeResource(inputStream);
        }
    }

    private JarInputStream getJarInputStream(String jarPath) throws IOException {
        File file = new File(jarPath);
        if (file.exists()) {
            return new JarInputStream(new FileInputStream(jarPath));
        }

        InputStream inputStream = configClassLoader.getResourceAsStream(jarPath);
        if (inputStream == null) {
            throw new FileNotFoundException("File could not be found in " + jarPath
                    + "  and resources/" + jarPath);
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

    private String extractClassName(JarEntry entry) {
        String entryName = entry.getName();
        Matcher matcher = CLASS_PATTERN.matcher(entryName.replace('/', '.'));
        if (matcher.matches()) {
            return matcher.group(1);
        }
        return null;
    }

    void deploy(HazelcastClientInstanceImpl client, Connection ownerConnection) throws ExecutionException, InterruptedException {
        if (!clientUserCodeDeploymentConfig.isEnabled()) {
            return;
        }
        ClientMessage request = ClientDeployClassesCodec.encodeRequest(classDefinitionList);
        ClientInvocation invocation = new ClientInvocation(client, request, ownerConnection);
        ClientInvocationFuture future = invocation.invoke();
        future.get();
    }

    //testing purposes
    public List<Map.Entry<String, byte[]>> getClassDefinitionList() {
        return classDefinitionList;
    }
}
