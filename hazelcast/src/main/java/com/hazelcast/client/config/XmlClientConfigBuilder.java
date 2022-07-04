/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.config;

import com.hazelcast.client.config.impl.ClientConfigSections;
import com.hazelcast.client.config.impl.ClientDomConfigProcessor;
import com.hazelcast.client.config.impl.XmlClientConfigLocator;
import com.hazelcast.config.AbstractXmlConfigBuilder;
import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.internal.config.ConfigLoader;
import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.internal.util.ExceptionUtil;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.spi.annotation.PrivateApi;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import javax.xml.parsers.DocumentBuilder;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Properties;

import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static com.hazelcast.internal.util.Preconditions.checkTrue;
import static com.hazelcast.internal.util.StringUtil.LINE_SEPARATOR;
import static com.hazelcast.internal.util.XmlUtil.getNsAwareDocumentBuilderFactory;

/**
 * Loads the {@link com.hazelcast.client.config.ClientConfig} using XML.
 */
public class XmlClientConfigBuilder extends AbstractXmlConfigBuilder {

    private static final ILogger LOGGER = Logger.getLogger(XmlClientConfigBuilder.class);

    private final InputStream in;

    public XmlClientConfigBuilder(String resource) throws IOException {
        URL url = ConfigLoader.locateConfig(resource);
        checkTrue(url != null, "Could not load " + resource);
        this.in = url.openStream();
    }

    public XmlClientConfigBuilder(File file) throws IOException {
        checkNotNull(file, "File is null!");
        this.in = new FileInputStream(file);
    }

    public XmlClientConfigBuilder(URL url) throws IOException {
        checkNotNull(url, "URL is null!");
        this.in = url.openStream();
    }

    public XmlClientConfigBuilder(InputStream in) {
        this.in = in;
    }

    /**
     * Loads the client config using the following resolution mechanism:
     * <ol>
     * <li>first it checks if a system property 'hazelcast.client.config' is set. If it exist and
     * it begins with 'classpath:', then a classpath resource is loaded. Else it will assume it is a file
     * reference. The configuration file or resource will be loaded only if the postfix of its name ends
     * with `.xml`.</li>
     * <li>it checks if a hazelcast-client.xml is available in the working dir</li>
     * <li>it checks if a hazelcast-client.xml is available on the classpath</li>
     * <li>it loads the hazelcast-client-default.xml</li>
     * </ol>
     */
    public XmlClientConfigBuilder() {
        this((XmlClientConfigLocator) null);
    }

    /**
     * Constructs a {@link XmlClientConfigBuilder} that loads the configuration
     * with the provided {@link XmlClientConfigLocator}.
     * <p>
     * If the provided {@link XmlClientConfigLocator} is {@code null}, a new
     * instance is created and the config is located in every possible
     * places. For these places, please see {@link XmlClientConfigLocator}.
     * <p>
     * If the provided {@link XmlClientConfigLocator} is not {@code null}, it
     * is expected that it already located the configuration XML to load
     * from. No further attempt to locate the configuration XML is made
     * if the configuration XML is not located already.
     *
     * @param locator the configured locator to use
     */
    @PrivateApi
    public XmlClientConfigBuilder(XmlClientConfigLocator locator) {
        if (locator == null) {
            locator = new XmlClientConfigLocator();
            locator.locateEverywhere();
        }
        this.in = locator.getIn();
    }

    @Override
    protected Document parse(InputStream inputStream) throws Exception {
        DocumentBuilder builder = getNsAwareDocumentBuilderFactory().newDocumentBuilder();
        try {
            return builder.parse(inputStream);
        } catch (Exception e) {
            String msg = "Failed to parse Config Stream"
                    + LINE_SEPARATOR + "Exception: " + e.getMessage()
                    + LINE_SEPARATOR + "HazelcastClient startup interrupted.";
            LOGGER.severe(msg);
            throw new InvalidConfigurationException(e.getMessage(), e);
        } finally {
            IOUtil.closeResource(inputStream);
        }
    }

    public XmlClientConfigBuilder setProperties(Properties properties) {
        setPropertiesInternal(properties);
        return this;
    }

    @Override
    protected ConfigType getConfigType() {
        return ConfigType.CLIENT;
    }

    public ClientConfig build() {
        return build(Thread.currentThread().getContextClassLoader());
    }

    public ClientConfig build(ClassLoader classLoader) {
        ClientConfig clientConfig = new ClientConfig();
        build(clientConfig, classLoader);
        return clientConfig;
    }

    void build(ClientConfig clientConfig, ClassLoader classLoader) {
        clientConfig.setClassLoader(classLoader);
        try {
            parseAndBuildConfig(clientConfig);
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        } finally {
            IOUtil.closeResource(in);
        }
    }

    private void parseAndBuildConfig(ClientConfig clientConfig) throws Exception {
        Document doc = parse(in);
        Element root = doc.getDocumentElement();
        checkRootElement(root);
        try {
            root.getTextContent();
        } catch (Throwable e) {
            domLevel3 = false;
        }
        process(root);
        if (shouldValidateTheSchema()) {
            schemaValidation(root.getOwnerDocument());
        }
        new ClientDomConfigProcessor(domLevel3, clientConfig).buildConfig(root);
    }

    private void checkRootElement(Element root) {
        String rootNodeName = root.getNodeName();
        if (!ClientConfigSections.HAZELCAST_CLIENT.getName().equals(rootNodeName)) {
            throw new InvalidConfigurationException("Invalid root element in xml configuration! "
                    + "Expected: <" + ClientConfigSections.HAZELCAST_CLIENT.getName()
                    + ">, Actual: <" + rootNodeName + ">.");
        }
    }
}
