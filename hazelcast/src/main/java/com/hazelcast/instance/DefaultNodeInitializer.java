/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.instance;

import com.hazelcast.logging.ILogger;
import com.hazelcast.security.SecurityContext;

import java.io.InputStream;
import java.util.Properties;
import java.util.logging.Level;

public class DefaultNodeInitializer implements NodeInitializer {

    protected ILogger logger;
    protected ILogger systemLogger;
    protected Node node;
    protected String version;
    protected String build;
    private int buildNumber;

    public void beforeInitialize(Node node) {
        this.node = node;
        systemLogger = node.getLogger("com.hazelcast.system");
        logger = node.getLogger("com.hazelcast.initializer");
        parseSystemProps();
    }

    public void printNodeInfo(Node node) {
        systemLogger.info("Hazelcast Community Edition " + version + " ("
                + build + ") starting at " + node.getThisAddress());
        systemLogger.info("Copyright (C) 2008-2013 Hazelcast.com");
    }

    public void afterInitialize(Node node) {
    }

    protected void parseSystemProps() {
        version = System.getProperty("hazelcast.version", "unknown");
        build = System.getProperty("hazelcast.build", "unknown");
        if ("unknown".equals(version) || "unknown".equals(build)) {
            try {
                final InputStream inRuntimeProperties = NodeInitializer.class.getClassLoader().getResourceAsStream("hazelcast-runtime.properties");
                if (inRuntimeProperties != null) {
                    Properties runtimeProperties = new Properties();
                    runtimeProperties.load(inRuntimeProperties);
                    inRuntimeProperties.close();
                    version = runtimeProperties.getProperty("hazelcast.version");
                    build = runtimeProperties.getProperty("hazelcast.build");
                }
            } catch (Exception ignored) {
            }
        }
        try {
            buildNumber = Integer.getInteger("hazelcast.build", -1);
            if (buildNumber == -1) {
                buildNumber = Integer.parseInt(build);
            }
        } catch (Exception ignored) {
        }
    }

    public int getBuildNumber() {
        return buildNumber;
    }

    public String getVersion() {
        return version;
    }

    public String getBuild() {
        return build;
    }

    public SecurityContext getSecurityContext() {
        logger.warning("Security features are only available on Hazelcast Enterprise Edition!");
        return null;
    }

    public void destroy() {
        logger.info("Destroying node initializer.");
    }
}
