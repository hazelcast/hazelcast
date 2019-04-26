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

import com.hazelcast.config.AbstractXmlConfigBuilder;
import com.hazelcast.config.Config;
import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.instance.BuildInfo;
import com.hazelcast.instance.BuildInfoProvider;
import com.hazelcast.instance.JetBuildInfo;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.nio.IOUtil;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import javax.annotation.Nullable;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.InputStream;
import java.util.Properties;

import static com.hazelcast.jet.impl.util.ExceptionUtil.sneakyThrow;
import static com.hazelcast.util.Preconditions.checkTrue;
import static com.hazelcast.util.StringUtil.LINE_SEPARATOR;

/**
 * Loads the {@link JetConfig} using XML.
 */
public final class XmlJetConfigBuilder extends AbstractXmlConfigBuilder {

    private static final ILogger LOGGER = Logger.getLogger(XmlJetConfigBuilder.class);

    private final InputStream in;

    /**
     * Constructs a XmlJetConfigBuilder that tries to find a usable XML configuration file.
     * <p>
     * Loads the jet config using the following resolution mechanism:
     * <ol>
     * <li>first it checks if a system property 'hazelcast.jet.config' is set. If it exist and it begins with
     * 'classpath:', then a classpath resource is loaded. Else it will assume it is a file reference</li>
     * <li>it checks if a hazelcast-jet.xml is available in the working dir</li>
     * <li>it checks if a hazelcast-jet.xml is available on the classpath</li>
     * <li>it loads the hazelcast-jet-default.xml</li>
     * </ol>
     */
    public XmlJetConfigBuilder() {
        this((XmlJetConfigLocator) null);
    }

    public XmlJetConfigBuilder(InputStream inputStream) {
        checkTrue(inputStream != null, "inputStream can't be null");
        this.in = inputStream;
    }

    public XmlJetConfigBuilder(XmlJetConfigLocator locator) {
        if (locator == null) {
            locator = new XmlJetConfigLocator();
            locator.locateEverywhere();
        }
        this.in = locator.getIn();
    }

    /**
     * Sets properties to be used in variable resolution.
     *
     * If null properties supplied, System.properties will be used.
     *
     * @param properties the properties to be used to resolve ${variable}
     *                   occurrences in the XML file
     * @return the XmlJetConfigBuilder
     */
    public XmlJetConfigBuilder setProperties(@Nullable Properties properties) {
        if (properties == null) {
            properties = System.getProperties();
        }
        setPropertiesInternal(properties);
        return this;
    }

    @Deprecated
    public static JetConfig loadConfig(@Nullable InputStream stream, @Nullable Properties properties) {
        if (stream == null) {
            XmlJetConfigLocator locator = new XmlJetConfigLocator();
            locator.locateEverywhere();
            stream = locator.getIn();
        }
        JetConfig cfg = new XmlJetConfigBuilder(stream).setProperties(properties).build();
        cfg.setHazelcastConfig(getMemberConfig(properties));
        return cfg;
    }

    @Override
    protected Document parse(InputStream inputStream) throws Exception {
        DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
        dbf.setNamespaceAware(true);
        dbf.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
        DocumentBuilder builder = dbf.newDocumentBuilder();
        try {
            return builder.parse(inputStream);
        } catch (Exception e) {
            String msg = "Failed to parse config"
                    + LINE_SEPARATOR + "Exception: " + e.getMessage()
                    + LINE_SEPARATOR + "Hazelcast Jet startup interrupted.";
            LOGGER.severe(msg);
            throw new InvalidConfigurationException(e.getMessage(), e);
        } finally {
            IOUtil.closeResource(inputStream);
        }
    }

    @Override
    protected ConfigType getConfigType() {
        return ConfigType.JET;
    }

    @Override
    public String getNamespaceType() {
        return "jet-config";
    }

    @Override
    protected String getReleaseVersion() {
        BuildInfo buildInfo = BuildInfoProvider.getBuildInfo();
        JetBuildInfo jetBuildInfo = buildInfo.getJetBuildInfo();
        return jetBuildInfo.getVersion().substring(0, 3);
    }

    public JetConfig build() {
        return build(new JetConfig());
    }

    public JetConfig build(JetConfig config) {
        try {
            parseAndBuildConfig(config);
        } catch (Exception e) {
            throw sneakyThrow(e);
        } finally {
            IOUtil.closeResource(in);
        }
        config.setHazelcastConfig(getMemberConfig(getProperties()));
        return config;
    }

    private void parseAndBuildConfig(JetConfig config) throws Exception {
        Document doc = parse(in);
        Element root = doc.getDocumentElement();
        try {
            root.getTextContent();
        } catch (Throwable e) {
            domLevel3 = false;
        }
        process(root);
        schemaValidation(root.getOwnerDocument());
        new JetDomConfigProcessor(domLevel3, config).buildConfig(root);
    }

    private static Config getMemberConfig(Properties properties) {
        XmlJetMemberConfigLocator locator = new XmlJetMemberConfigLocator();
        locator.locateEverywhere();
        return new XmlConfigBuilder(locator.getIn()).setProperties(properties).build();
    }


}
