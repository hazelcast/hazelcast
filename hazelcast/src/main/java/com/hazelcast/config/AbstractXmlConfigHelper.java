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

package com.hazelcast.config;

import com.hazelcast.config.AbstractXmlConfigBuilder.ConfigType;
import com.hazelcast.instance.BuildInfo;
import com.hazelcast.instance.BuildInfoProvider;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.internal.util.StringUtil;
import com.hazelcast.internal.util.XmlUtil;
import com.hazelcast.spi.annotation.PrivateApi;
import org.w3c.dom.Document;
import org.xml.sax.InputSource;

import javax.xml.transform.Result;
import javax.xml.transform.Source;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.sax.SAXSource;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import javax.xml.validation.Validator;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;

import static com.hazelcast.internal.nio.IOUtil.closeResource;
import static com.hazelcast.internal.util.StringUtil.LINE_SEPARATOR;

/**
 * Contains Hazelcast XML Configuration helper methods and variables.
 */
@SuppressWarnings("checkstyle:methodcount")
public abstract class AbstractXmlConfigHelper extends AbstractConfigBuilder {

    private static final ILogger LOGGER = Logger.getLogger(AbstractXmlConfigHelper.class);

    protected boolean domLevel3 = true;

    final String xmlns = "http://www.hazelcast.com/schema/" + getNamespaceType();

    private final String hazelcastSchemaLocation = getConfigType().name + "-config-" + getReleaseVersion() + ".xsd";

    public String getNamespaceType() {
        return getConfigType().name.equals("hazelcast") ? "config" : "client-config";
    }

    protected ConfigType getConfigType() {
        return ConfigType.SERVER;
    }

    protected void schemaValidation(Document doc) throws Exception {
        ArrayList<StreamSource> schemas = new ArrayList<>();
        InputStream inputStream = null;
        String schemaLocation = doc.getDocumentElement().getAttribute("xsi:schemaLocation");
        schemaLocation = schemaLocation.replaceAll("^ +| +$| (?= )", "");

        // get every two pair. every pair includes namespace and uri
        String[] xsdLocations = schemaLocation.split("(?<!\\G\\S+)\\s");

        for (String xsdLocation : xsdLocations) {
            if (xsdLocation.isEmpty()) {
                continue;
            }
            String namespace = xsdLocation.split('[' + LINE_SEPARATOR + " ]+")[0];
            String uri = xsdLocation.split('[' + LINE_SEPARATOR + " ]+")[1];

            // if this is hazelcast namespace but location is different log only warning
            if (namespace.equals(xmlns) && !uri.endsWith(hazelcastSchemaLocation)) {
                if (LOGGER.isWarningEnabled()) {
                    LOGGER.warning("Name of the hazelcast schema location[" + uri + "] is incorrect, using default");
                }
            }

            // if this is not hazelcast namespace then try to load from uri
            if (!namespace.equals(xmlns)) {
                inputStream = loadSchemaFile(uri);
                schemas.add(new StreamSource(inputStream));
            }
        }

        // include hazelcast schema
        schemas.add(new StreamSource(getClass().getClassLoader().getResourceAsStream(hazelcastSchemaLocation)));

        // document to InputStream conversion
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        Source xmlSource = new DOMSource(doc);
        Result outputTarget = new StreamResult(outputStream);
        TransformerFactory transformerFactory = XmlUtil.getTransformerFactory();
        transformerFactory.newTransformer().transform(xmlSource, outputTarget);
        InputStream is = new ByteArrayInputStream(outputStream.toByteArray());

        // schema validation
        SchemaFactory schemaFactory = XmlUtil.getSchemaFactory();
        Schema schema = schemaFactory.newSchema(schemas.toArray(new Source[0]));
        Validator validator = schema.newValidator();
        try {
            SAXSource source = new SAXSource(new InputSource(is));
            validator.validate(source);
        } catch (Exception e) {
            throw new InvalidConfigurationException(e.getMessage(), e);
        } finally {
            for (StreamSource source : schemas) {
                closeResource(source.getInputStream());
            }
            closeResource(inputStream);
        }
    }

    protected InputStream loadSchemaFile(String schemaLocation) {
        // is resource file
        InputStream inputStream = Thread.currentThread().getContextClassLoader().getResourceAsStream(schemaLocation);
        // is URL
        if (inputStream == null) {
            try {
                inputStream = new URL(schemaLocation).openStream();
            } catch (Exception e) {
                throw new InvalidConfigurationException("Your xsd schema couldn't be loaded");
            }
        }
        return inputStream;
    }

    @PrivateApi
    public String getReleaseVersion() {
        BuildInfo buildInfo = BuildInfoProvider.getBuildInfo();
        String[] versionTokens = StringUtil.tokenizeVersionString(buildInfo.getVersion());
        return versionTokens[0] + "." + versionTokens[1];
    }

    protected String xmlToJavaName(final String name) {
        String javaRefName = xmlRefToJavaName(name);
        if (javaRefName != null) {
            return javaRefName;
        }
        final StringBuilder builder = new StringBuilder();
        final char[] charArray = name.toCharArray();
        boolean dash = false;
        final StringBuilder token = new StringBuilder();
        for (char aCharArray : charArray) {
            if (aCharArray == '-') {
                appendToken(builder, token);
                dash = true;
                continue;
            }
            token.append(dash ? Character.toUpperCase(aCharArray) : aCharArray);
            dash = false;
        }
        appendToken(builder, token);
        return builder.toString();
    }

    private String xmlRefToJavaName(final String name) {
        if (name.equals("split-brain-protection-ref")) {
            return "splitBrainProtectionName";
        }
        if (name.equals("flow-control-period")) {
            return "flowControlPeriodMs";
        }
        return null;
    }

    protected void appendToken(final StringBuilder builder, final StringBuilder token) {
        String string = token.toString();
        if ("Jvm".equals(string)) {
            string = "JVM";
        }
        builder.append(string);
        token.setLength(0);
    }

}
