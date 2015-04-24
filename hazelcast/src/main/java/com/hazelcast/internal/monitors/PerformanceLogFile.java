/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.monitors;

import com.hazelcast.instance.BuildInfo;
import com.hazelcast.instance.BuildInfoProvider;
import com.hazelcast.instance.GroupProperties;
import com.hazelcast.instance.HazelcastInstanceImpl;
import com.hazelcast.instance.Node;
import com.hazelcast.internal.blackbox.Blackbox;
import com.hazelcast.internal.blackbox.Sensor;
import com.hazelcast.internal.management.dto.SlowOperationDTO;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.impl.operationservice.InternalOperationService;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

import static com.hazelcast.nio.IOUtil.closeResource;
import static java.lang.Math.round;
import static java.lang.String.format;
import static java.lang.System.currentTimeMillis;
import static java.lang.System.getProperties;
import static java.lang.System.getProperty;
import static java.util.Collections.sort;

final class PerformanceLogFile {
    private static final int ONE_MB = 1024 * 1024;

    volatile File logFile;

    private final Blackbox blackbox;
    private final HazelcastInstanceImpl hazelcastInstance;
    private final InternalOperationService operationService;
    private final ILogger logger;

    private final HeadRenderer headRenderer = new HeadRenderer();
    private final BodyRendered bodyRendered = new BodyRendered();
    private final String pathname;
    private long lastAccessed;
    private int index;
    private BufferedWriter writer;
    private boolean renderHead;
    private int maxRollingFileCount;
    private int maxRollingFileSizeBytes;

    PerformanceLogFile(PerformanceMonitor performanceMonitor) {
        this.logger = performanceMonitor.logger;
        this.hazelcastInstance = performanceMonitor.hazelcastInstance;
        Node node = hazelcastInstance.node;
        this.blackbox = node.nodeEngine.getBlackbox();
        this.operationService = node.nodeEngine.getOperationService();
        this.pathname = "performance-" + hazelcastInstance.getName() + "-" + currentTimeMillis() + "-%03d.log";
        GroupProperties props = node.getGroupProperties();
        this.maxRollingFileCount = props.PERFORMANCE_MONITOR_MAX_ROLLED_FILE_COUNT.getInteger();

        // we accept a float so it becomes easier to testing to create a small file.
        this.maxRollingFileSizeBytes = round(ONE_MB * props.PERFORMANCE_MONITOR_MAX_ROLLED_FILE_SIZE.getFloat());

        if (logger.isFinestEnabled()) {
            logger.finest("max rolling file size: " + maxRollingFileSizeBytes);
            logger.finest("max rolling file count: " + maxRollingFileCount);
        }
    }

    void render() {
        try {
            if (logFile == null) {
                logFile = new File(format(pathname, index));
                renderHead = true;
            }

            if (writer == null) {
                writer = new BufferedWriter(new FileWriter(logFile, true));
            }

            if (renderHead) {
                headRenderer.render(writer);
                renderHead = false;
            }

            bodyRendered.render(writer);
            writer.flush();
            lastAccessed = logFile.lastModified();

            if (logFile.length() > maxRollingFileSizeBytes) {
                closeResource(writer);
                writer = null;
                logFile = null;
                index++;
                deleteOld();
            }
        } catch (IOException e) {
            logIOError(e);
            logFile = null;
            closeResource(writer);
            writer = null;
        }
    }

    private void logIOError(IOException e) {
        if (logger.isFinestEnabled()) {
            logger.finest("PerformanceMonitor failed to output to file:"
                    + logFile.getAbsolutePath() + " cause:" + e.getMessage(), e);
        } else {
            logger.warning("PerformanceMonitor failed to output to file:"
                    + logFile.getAbsolutePath() + " cause:" + e.getMessage());
        }
    }

    private void deleteOld() {
        File file = new File(format(pathname, index - maxRollingFileCount));
        file.delete();
    }

    public boolean forceRender() {
        if (logFile == null) {
            return false;
        }

        return logFile.lastModified() != lastAccessed;
    }

    private final class BodyRendered {

        private final SimpleDateFormat sdf = new SimpleDateFormat("EEE, d MMM yyyy HH:mm:ss Z");
        private final List<String> parameters = new ArrayList<String>();
        private final List<Sensor> sensors = new ArrayList<Sensor>();
        private final StringBuilder sb = new StringBuilder();
        // Since the blackbox.modCount starts at 0, by making the lastModCount=-1, we are triggering the loading of the sensors
        // on the first run.
        private int lastModCount = -1;

        private void render(BufferedWriter writer) throws IOException {
            renderHeader(writer);
            renderBlackbox(writer);
            writer.append("\n\n");
            if (renderSlowOperation(writer)) {
                writer.append("\n\n");
            }
        }

        private void renderHeader(BufferedWriter writer) throws IOException {
            writer.append("================[ Blackbox ]============================================\n");
            Calendar calendar = Calendar.getInstance();
            writer.append(sdf.format(calendar.getTime())).append('\n');
            writer.append("------------------------------------------------------------------------\n");
        }

        private void renderBlackbox(BufferedWriter writer) throws IOException {
            updateViews();

            for (Sensor sensorView : sensors) {
                sensorView.render(sb);
                writer.append(sb).append('\n');
                sb.setLength(0);
            }
        }

        private void updateViews() {
            int modCount = blackbox.modCount();
            if (lastModCount == modCount) {
                return;
            }

            lastModCount = modCount;

            // lets get the new list of parameters and get them sorted.
            parameters.clear();
            parameters.addAll(blackbox.getParameters());
            sort(parameters);

            // and we update the sensors. This now contains a sorted list of sensorviews.
            sensors.clear();
            for (String parameter : parameters) {
                sensors.add(blackbox.getSensor(parameter));
            }
        }

        private boolean renderSlowOperation(BufferedWriter writer) throws IOException {
            List<SlowOperationDTO> slowOperations = operationService.getSlowOperationDTOs();
            if (slowOperations.isEmpty()) {
                return false;
            }

            writer.append("================[ Slow Operations ]=====================================\n");
            int k = 1;
            for (SlowOperationDTO slowOperation : slowOperations) {
                writer.append("#" + k)
                        .append("\n    " + slowOperation.operation)
                        .append("\n    Invocations: " + slowOperation.totalInvocations)
                        .append("\n    Stacktrace:\n");
                writer.append(slowOperation.stackTrace).append("\n\n");
                k++;
            }
            return true;
        }
    }

    // Responsible for rendering the head of the file containing the system/config properties.
    private final class HeadRenderer {

        private void render(BufferedWriter writer) throws IOException {
            renderBuildInfo(writer);
            writer.append("\n\n");
            renderSystemProperties(writer);
            writer.append("\n\n");
            renderConfigProperties(writer);
            writer.append("\n\n");
            writer.flush();
        }

        private void renderSystemProperties(BufferedWriter writer) throws IOException {
            writer.append("================[ System Properties ]===================================\n");
            writer.append(format("%-30s  %4s\n", "property", "value"));
            writer.append("------------------------------------------------------------------------\n");
            List keys = new LinkedList();
            keys.addAll(getProperties().keySet());
            sort(keys);

            for (Object key : keys) {
                String keyString = (String) key;
                if (ignore(keyString)) {
                    continue;
                }

                Object value = getProperty(keyString);

                String s = formatKeyValue(keyString, value);
                writer.append(s);
            }

            writer.append(formatKeyValue("jvm.args", getInputArgs()));
        }

        private String getInputArgs() {
            RuntimeMXBean runtimeMxBean = ManagementFactory.getRuntimeMXBean();
            List<String> arguments = runtimeMxBean.getInputArguments();

            StringBuffer sb = new StringBuffer();
            sb.append("jvm.args=");
            for (String argument : arguments) {
                sb.append(argument);
                sb.append(" ");
            }
            return sb.toString();
        }

        private void renderConfigProperties(BufferedWriter writer) throws IOException {
            writer.append("=================[ Hazelcast Config ]===================================\n");
            writer.append(format("%-60s  %4s\n", "property", "value"));
            writer.append("------------------------------------------------------------------------\n");
            List keys = new LinkedList();
            Properties properties = hazelcastInstance.getConfig().getProperties();
            keys.addAll(properties.keySet());
            sort(keys);

            for (Object key : keys) {
                String keyString = (String) key;
                String value = properties.getProperty(keyString);
                writer.append(format("%-60s  %4s\n", keyString, value));
            }
        }

        private void renderBuildInfo(BufferedWriter writer) throws IOException {
            writer.append("====================[ Build Info ]======================================\n");
            writer.append(format("%-30s  %4s\n", "property", "value"));
            writer.append("------------------------------------------------------------------------\n");

            BuildInfo buildInfo = BuildInfoProvider.getBuildInfo();
            writer.append(formatKeyValue("Version", buildInfo.getVersion()));
            writer.append(formatKeyValue("Build", buildInfo.getBuild()));
            writer.append(formatKeyValue("BuildNumber", buildInfo.getBuildNumber()));
            writer.append(formatKeyValue("Revision", buildInfo.getVersion()));
            writer.append(formatKeyValue("Enterprise", buildInfo.isEnterprise()));
        }

        private String formatKeyValue(String keyString, Object value) {
            return format("%-30s  %4s\n", keyString, value);
        }

        private boolean ignore(String systemProperty) {
            if (systemProperty.startsWith("java.awt")) {
                return true;
            }

            if (systemProperty.startsWith("java")
                    || systemProperty.startsWith("hazelcast")
                    || systemProperty.startsWith("sun")
                    || systemProperty.startsWith("os")) {
                return false;
            }

            return true;
        }
    }
}
