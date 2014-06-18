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
import com.hazelcast.storage.DataRef;
import com.hazelcast.storage.Storage;
import com.hazelcast.wan.WanReplicationService;
import com.hazelcast.wan.impl.WanReplicationServiceImpl;

public class DefaultNodeInitializer implements NodeInitializer {

    protected ILogger logger;
    protected ILogger systemLogger;
    protected Node node;
    protected String version;
    protected String build;

    @Override
    public void beforeInitialize(Node node) {
        this.node = node;
        systemLogger = node.getLogger("com.hazelcast.system");
        logger = node.getLogger("com.hazelcast.initializer");
        parseSystemProps();
    }

    @Override
    public void printNodeInfo(Node node) {
        systemLogger.info("Hazelcast " + version + " ("
                + build + ") starting at " + node.getThisAddress());
        systemLogger.info("Copyright (C) 2008-2014 Hazelcast.com");
    }

    @Override
    public void afterInitialize(Node node) {
    }

    protected void parseSystemProps() {
        version = node.getBuildInfo().getVersion();
        build = node.getBuildInfo().getBuild();
    }

    @Override
    public SecurityContext getSecurityContext() {
        logger.warning("Security features are only available on Hazelcast Enterprise Edition!");
        return null;
    }

    @Override
    public Storage<DataRef> getOffHeapStorage() {
        throw new UnsupportedOperationException("Offheap feature is only available on Hazelcast Enterprise Edition!");
    }

    @Override
    public WanReplicationService geWanReplicationService() {
        return new WanReplicationServiceImpl(node);
    }

    @Override
    public void destroy() {
        logger.info("Destroying node initializer.");
    }
}
