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

import cascading.flow.BaseFlow;
import cascading.flow.FlowDef;
import cascading.flow.FlowException;
import cascading.flow.FlowProcess;
import cascading.flow.planner.PlatformInfo;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet2.JetEngineConfig;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import java.io.IOException;
import java.util.Map;
import org.slf4j.helpers.MessageFormatter;

public class JetFlow extends BaseFlow<JetEngineConfig> {

    private static final ILogger LOGGER = Logger.getLogger(JetFlow.class);
    private final HazelcastInstance instance;
    private JetEngineConfig config;

    public JetFlow(
            HazelcastInstance instance,
            PlatformInfo platformInfo,
            Map<Object, Object> properties,
            JetEngineConfig config,
            FlowDef flowDef) {
        super(platformInfo, properties, config, flowDef);
        this.instance = instance;
    }

    public HazelcastInstance getHazelcastInstance() {
        return instance;
    }

    @Override
    protected void initConfig(Map<Object, Object> properties, JetEngineConfig parentConfig) {
        config = parentConfig;
        config.getProperties().putAll(properties);
    }

    @Override
    protected void setConfigProperty(JetEngineConfig jobConfig, Object key, Object value) {
        jobConfig.getProperties().put(key, value);
    }

    @Override
    protected JetEngineConfig newConfig(JetEngineConfig defaultConfig) {
        return new JetEngineConfig();
    }

    @Override
    protected void internalStart() {
        try {
            deleteSinksIfReplace();
            deleteTrapsIfReplace();
        } catch (IOException exception) {
            throw new FlowException("unable to delete sinks", exception);
        }
    }

    @Override
    protected void internalClean(boolean stop) {

    }

    @Override
    protected int getMaxNumParallelSteps() {
        return 0;
    }

    @Override
    protected void internalShutdown() {
    }

    @Override
    public JetEngineConfig getConfig() {
        return config;
    }

    @Override
    public JetEngineConfig getConfigCopy() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<Object, Object> getConfigAsProperties() {
        return config.getProperties();
    }

    @Override
    public String getProperty(String key) {
        return config.getProperties().get(key).toString();
    }

    @Override
    public FlowProcess<JetEngineConfig> getFlowProcess() {
        return new JetFlowProcess(config, instance);
    }

    @Override
    public boolean stepsAreLocal() {
        return false;
    }

    @Override
    public void logInfo(String message, Object... arguments) {
        LOGGER.info(formatMessage(message, arguments));
    }

    @Override
    public void logDebug(String message, Object... arguments) {
        LOGGER.fine(formatMessage(message, arguments));
    }

    @Override
    public void logWarn(String message) {
        LOGGER.warning(message);
    }

    @Override
    public void logWarn(String message, Throwable throwable) {
        LOGGER.warning(message, throwable);
    }

    @Override
    public void logWarn(String message, Object... arguments) {
        LOGGER.warning(formatMessage(message, arguments));
    }

    @Override
    public void logError(String message, Object... arguments) {
        LOGGER.severe(formatMessage(message, arguments));
    }

    @Override
    public void logError(String message, Throwable throwable) {
        LOGGER.severe(message, throwable);
    }

    private String formatMessage(String message, Object[] arguments) {
        return MessageFormatter.arrayFormat(message, arguments).getMessage();
    }
}
