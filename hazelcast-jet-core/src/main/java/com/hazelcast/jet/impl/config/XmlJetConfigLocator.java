/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.impl.config;

import com.hazelcast.config.AbstractConfigLocator;

/**
 * A support class for the {@link XmlJetConfigBuilder} to locate the
 * xml configuration.
 */
public final class XmlJetConfigLocator extends AbstractConfigLocator {

    private static final String HAZELCAST_JET_CONFIG_PROPERTY = "hazelcast.jet.config";
    private static final String HAZELCAST_JET_XML = "hazelcast-jet.xml";
    private static final String HAZELCAST_JET_DEFAULT_XML = "hazelcast-jet-default.xml";

    public XmlJetConfigLocator() {
        super(false);
    }

    @Override
    public boolean locateFromSystemProperty() {
        return loadFromSystemProperty(HAZELCAST_JET_CONFIG_PROPERTY);
    }

    @Override
    protected boolean locateInWorkDir() {
        return loadFromWorkingDirectory(HAZELCAST_JET_XML);
    }

    @Override
    protected boolean locateOnClasspath() {
        return loadConfigurationFromClasspath(HAZELCAST_JET_XML);
    }

    @Override
    public boolean locateDefault() {
        loadDefaultConfigurationFromClasspath(HAZELCAST_JET_DEFAULT_XML);
        return true;
    }
}
