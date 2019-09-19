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

package com.hazelcast.config;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.nio.IOUtil;
import com.hazelcast.internal.util.ExceptionUtil;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Properties;

import static com.hazelcast.instance.BuildInfoProvider.HAZELCAST_INTERNAL_OVERRIDE_VERSION;
import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static com.hazelcast.internal.util.Preconditions.checkTrue;
import static com.hazelcast.internal.util.StringUtil.LINE_SEPARATOR;

/**
 * A XML {@link ConfigBuilder} implementation.
 */
public class XmlConfigBuilder extends AbstractXmlConfigBuilder implements ConfigBuilder {

    private static final ILogger LOGGER = Logger.getLogger(XmlConfigBuilder.class);

    private final InputStream in;

    private File configurationFile;
    private URL configurationUrl;

    /**
     * Constructs a XmlConfigBuilder that reads from the provided XML file.
     *
     * @param xmlFileName the name of the XML file that the XmlConfigBuilder reads from
     * @throws FileNotFoundException if the file can't be found
     */
    public XmlConfigBuilder(String xmlFileName) throws FileNotFoundException {
        this(new FileInputStream(xmlFileName));
        this.configurationFile = new File(xmlFileName);
    }

    /**
     * Constructs a XmlConfigBuilder that reads from the given InputStream.
     *
     * @param inputStream the InputStream containing the XML configuration
     * @throws IllegalArgumentException if inputStream is {@code null}
     */
    public XmlConfigBuilder(InputStream inputStream) {
        checkTrue(inputStream != null, "inputStream can't be null");
        this.in = inputStream;
    }

    /**
     * Constructs a XMLConfigBuilder that reads from the given URL.
     *
     * @param url the given url that the XMLConfigBuilder reads from
     * @throws IOException if URL is invalid
     */
    public XmlConfigBuilder(URL url) throws IOException {
        checkNotNull(url, "URL is null!");
        this.in = url.openStream();
        this.configurationUrl = url;
    }

    /**
     * Constructs a XmlConfigBuilder that tries to find a usable XML configuration file.
     */
    public XmlConfigBuilder() {
        this((XmlConfigLocator) null);
    }

    /**
     * Constructs a {@link XmlConfigBuilder} that loads the configuration
     * with the provided {@link XmlConfigLocator}.
     * <p>
     * If the provided {@link XmlConfigLocator} is {@code null}, a new
     * instance is created and the config is located in every possible
     * places. For these places, please see {@link XmlConfigLocator}.
     * <p>
     * If the provided {@link XmlConfigLocator} is not {@code null}, it
     * is expected that it already located the configuration XML to load
     * from. No further attempt to locate the configuration XML is made
     * if the configuration XML is not located already.
     *
     * @param locator the configured locator to use
     */
    public XmlConfigBuilder(XmlConfigLocator locator) {
        if (locator == null) {
            locator = new XmlConfigLocator();
            locator.locateEverywhere();
        }
        this.in = locator.getIn();
        this.configurationFile = locator.getConfigurationFile();
        this.configurationUrl = locator.getConfigurationUrl();
    }

    /**
     * Sets the used properties. Can be null if no properties should be used.
     * <p>
     * Properties are used to resolve ${variable} occurrences in the XML file.
     *
     * @param properties the new properties
     * @return the XmlConfigBuilder
     */
    public XmlConfigBuilder setProperties(Properties properties) {
        super.setPropertiesInternal(properties);
        return this;
    }

    @Override
    protected ConfigType getConfigType() {
        return ConfigType.SERVER;
    }

    @Override
    public Config build() {
        return build(new Config());
    }

    Config build(Config config) {
        config.setConfigurationFile(configurationFile);
        config.setConfigurationUrl(configurationUrl);
        try {
            parseAndBuildConfig(config);
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
        return config;
    }

    private void parseAndBuildConfig(Config config) throws Exception {
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

        new MemberDomConfigProcessor(domLevel3, config).buildConfig(root);
    }

    private void checkRootElement(Element root) {
        String rootNodeName = root.getNodeName();
        if (!ConfigSections.HAZELCAST.isEqual(rootNodeName)) {
            throw new InvalidConfigurationException("Invalid root element in xml configuration!"
                    + " Expected: <" + ConfigSections.HAZELCAST.name + ">, Actual: <" + rootNodeName + ">.");
        }
    }

    private boolean shouldValidateTheSchema() {
        // in case of overridden Hazelcast version there may be no schema with that version
        // (this feature is used only in Simulator testing)
        return System.getProperty(HAZELCAST_INTERNAL_OVERRIDE_VERSION) == null;
    }

    @Override
    protected Document parse(InputStream is) throws Exception {
        DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
        dbf.setNamespaceAware(true);
        dbf.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
        DocumentBuilder builder = dbf.newDocumentBuilder();
        Document doc;
        try {
            doc = builder.parse(is);
        } catch (Exception e) {
            if (configurationFile != null) {
                String msg = "Failed to parse " + configurationFile
                        + LINE_SEPARATOR + "Exception: " + e.getMessage()
                        + LINE_SEPARATOR + "Hazelcast startup interrupted.";
                LOGGER.severe(msg);

            } else if (configurationUrl != null) {
                String msg = "Failed to parse " + configurationUrl
                        + LINE_SEPARATOR + "Exception: " + e.getMessage()
                        + LINE_SEPARATOR + "Hazelcast startup interrupted.";
                LOGGER.severe(msg);
            } else {
                String msg = "Failed to parse the inputstream"
                        + LINE_SEPARATOR + "Exception: " + e.getMessage()
                        + LINE_SEPARATOR + "Hazelcast startup interrupted.";
                LOGGER.severe(msg);
            }
            throw new InvalidConfigurationException(e.getMessage(), e);
        } finally {
            IOUtil.closeResource(is);
        }
        return doc;
    }
}
