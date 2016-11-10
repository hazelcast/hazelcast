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

package com.hazelcast.jet.cascading;

import cascading.flow.FlowConnector;
import cascading.flow.FlowProcess;
import cascading.platform.TestPlatform;
import cascading.scheme.Scheme;
import cascading.scheme.util.DelimitedParser;
import cascading.scheme.util.FieldTypeResolver;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.partition.Partition;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import com.hazelcast.config.Config;
import com.hazelcast.config.SerializerConfig;
import com.hazelcast.core.DistributedObject;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.jet.cascading.runtime.TupleSerializer;
import com.hazelcast.jet.cascading.tap.InternalMapTap;
import com.hazelcast.jet2.JetEngineConfig;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static com.hazelcast.util.ExceptionUtil.rethrow;

public class JetPlatform extends TestPlatform {

    private static final int CLUSTER_SIZE = 4;
    private static HazelcastInstance instance;

    @Override
    public synchronized void setUp() throws IOException {
        Config config = new Config();
        config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        config.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(true);
        config.getNetworkConfig().getJoin().getTcpIpConfig().addMember("127.0.0.1");
        SerializerConfig tupleSerializer = new SerializerConfig();
        tupleSerializer.setTypeClass(Tuple.class);
        tupleSerializer.setClass(TupleSerializer.class);

        config.getSerializationConfig().addSerializerConfig(tupleSerializer);
        if (instance == null) {
            instance = buildCluster(CLUSTER_SIZE, config);
        }
        assert instance.getCluster().getMembers().size() == CLUSTER_SIZE;
    }

    @Override
    public Map<Object, Object> getProperties() {
        return new HashMap<>();
    }

    @Override
    public void tearDown() {
        instance.getDistributedObjects().forEach(DistributedObject::destroy);
    }

    @Override
    public void copyFromLocal(String inputFile) throws IOException {
        IMap<Long, String> map = instance.getMap(inputFile);
        FileInputStream fileInputStream = new FileInputStream(inputFile);
        LineNumberReader lineNumberReader = new LineNumberReader(new InputStreamReader(fileInputStream));

        String line;
        do {
            int lineNumber = lineNumberReader.getLineNumber();
            line = lineNumberReader.readLine();
            if (line != null) {
                map.put((long) lineNumber, line);
            }
        } while (line != null);
    }

    @Override
    public void copyToLocal(String outputFile) throws IOException {
        IMap<Integer, String> map = instance.getMap(outputFile);
        File file = new File(outputFile);
        if (!file.exists()) {
            file.getParentFile().mkdirs();
            file.createNewFile();
        }
        Files.write(file.toPath(), map.values());
    }

    @Override
    public boolean remoteExists(String outputFile) throws IOException {
        IMap<Object, Object> map = instance.getMap(outputFile);
        return !map.isEmpty();
    }

    @Override
    public boolean remoteRemove(String outputFile, boolean recursive) throws IOException {
        throw new UnsupportedOperationException();
    }


    @Override
    public FlowProcess getFlowProcess() {
        return new JetFlowProcess(new JetEngineConfig(), instance);
    }

    @Override
    public FlowConnector getFlowConnector(Map<Object, Object> properties) {
        JetEngineConfig config = new JetEngineConfig();
        config.getProperties().putAll(properties);
        return new JetFlowConnector(instance, config);
    }

    @Override
    public Tap getTap(Scheme scheme, String filename, SinkMode mode) {
        return new InternalMapTap(filename, scheme, mode);
    }

    @Override
    public Tap getTextFile(Fields sourceFields, Fields sinkFields, String filename, SinkMode mode) {
        if (sourceFields == null) {
            return new InternalMapTap(filename, new TextLine(), mode);
        }
        return new InternalMapTap(filename, new TextLine(sourceFields, sinkFields), mode);
    }

    @Override
    public Tap getDelimitedFile(Fields fields, boolean hasHeader, String delimiter,
                                String quote, Class[] types, String filename, SinkMode mode) {

        return new InternalMapTap(filename, new TextDelimited(fields,
                new DelimitedParser(delimiter, quote, types)), mode);
    }

    @Override
    public Tap getDelimitedFile(Fields fields, boolean skipHeader, boolean writeHeader, String delimiter,
                                String quote, Class[] types, String filename, SinkMode mode) {
        return new InternalMapTap(filename, new TextDelimited(fields, skipHeader,
                new DelimitedParser(delimiter, quote, types)), mode);
    }

    @Override
    public Tap getDelimitedFile(String delimiter, String quote, FieldTypeResolver fieldTypeResolver,
                                String filename, SinkMode mode) {
        return new InternalMapTap(filename, new TextDelimited(
                new DelimitedParser(delimiter, quote, fieldTypeResolver)), mode);
    }

    @Override
    public Tap getPartitionTap(Tap sink, Partition partition, int openThreshold) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Scheme getTestConfigDefScheme() {
        return new JetConfigDefScheme(new Fields("line"));
    }

    @Override
    public Scheme getTestFailScheme() {
        return new JetFailScheme(new Fields("line"));
    }

    @Override
    public Comparator getLongComparator(boolean reverseSort) {
        return new TestLongComparator(reverseSort);
    }

    @Override
    public Comparator getStringComparator(boolean reverseSort) {
        return new TestStringComparator(reverseSort);
    }

    @Override
    public String getHiddenTemporaryPath() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isDAG() {
        return true;
    }

    private static HazelcastInstance buildCluster(int size, Config config) {
        ExecutorService executorService = Executors.newCachedThreadPool();
        HazelcastInstance instance = null;
        List<Future<HazelcastInstance>> futures = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            Future<HazelcastInstance> future = executorService.submit(() -> Hazelcast.newHazelcastInstance(config));
            futures.add(future);
        }
        for (Future<HazelcastInstance> future : futures) {
            instance = uncheckedGet(future);
        }
        executorService.shutdown();
        return instance;
    }

    private static <V> V uncheckedGet(Future<V> future) {
        try {
            return future.get();
        } catch (InterruptedException | ExecutionException e) {
            throw rethrow(e);
        }
    }
}
