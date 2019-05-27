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

package com.hazelcast.jet.config;

import com.hazelcast.config.ConfigXmlGenerator.XmlGenerator;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.util.Preconditions;

import javax.xml.transform.OutputKeys;
import javax.xml.transform.Source;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;
import java.io.StringReader;
import java.io.StringWriter;

import static com.hazelcast.nio.IOUtil.closeResource;

/**
 * The JetConfigXmlGenerator is responsible for transforming a
 * {@link JetConfig} to a Hazelcast Jet XML string.
 */
public final class JetConfigXmlGenerator {

    private static final ILogger LOGGER = Logger.getLogger(JetConfigXmlGenerator.class);

    private JetConfigXmlGenerator() {
    }

    /**
     * Convenience for {@link #generate(JetConfig, int)} without any indentation
     */
    public static String generate(JetConfig jetConfig) {
        return generate(jetConfig, -1);
    }

    /**
     * Generates Hazelcast Jet XML string for given {@link JetConfig} using the
     * {@code indent} value.
     */
    public static String generate(JetConfig jetConfig, int indent) {
        Preconditions.isNotNull(jetConfig, "JetConfig");

        StringBuilder xml = new StringBuilder();
        xml.append("<?xml version=\"1.0\" encoding=\"UTF-8\" ?>");
        XmlGenerator gen = new XmlGenerator(xml);

        gen.open("hazelcast-jet", "xmlns", "http://www.hazelcast.com/schema/jet-config",
                "xmlns:xsi", "http://www.w3.org/2001/XMLSchema-instance",
                "xsi:schemaLocation", "http://www.hazelcast.com/schema/jet-config "
                        + "http://www.hazelcast.com/schema/jet-config/hazelcast-jet-config-3.1.xsd");

        gen.appendProperties(jetConfig.getProperties());

        instance(gen, jetConfig.getInstanceConfig());
        edgeDefaults(gen, jetConfig.getDefaultEdgeConfig());
        metrics(gen, jetConfig.getMetricsConfig());

        gen.close();

        return format(xml.toString(), indent);
    }

    private static String format(String input, int indent) {
        if (indent < 0) {
            return input;
        }
        if (indent == 0) {
            throw new IllegalArgumentException("Indent should be greater than 0");
        }
        StreamResult xmlOutput = null;
        try {
            Source xmlInput = new StreamSource(new StringReader(input));
            xmlOutput = new StreamResult(new StringWriter());
            TransformerFactory transformerFactory = TransformerFactory.newInstance();
            /*
             * Older versions of Xalan still use this method of setting indent values.
             * Attempt to make this work but don't completely fail if it's a problem.
             */
            try {
                transformerFactory.setAttribute("indent-number", indent);
            } catch (IllegalArgumentException e) {
                logFinest("Failed to set indent-number attribute; cause: " + e.getMessage());
            }
            Transformer transformer = transformerFactory.newTransformer();
            transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");
            transformer.setOutputProperty(OutputKeys.ENCODING, "UTF-8");
            transformer.setOutputProperty(OutputKeys.INDENT, "yes");
            /*
             * Newer versions of Xalan will look for a fully-qualified output property in order to specify amount of
             * indentation to use. Attempt to make this work as well but again don't completely fail if it's a problem.
             */
            try {
                transformer.setOutputProperty("{http://xml.apache.org/xslt}indent-amount", Integer.toString(indent));
            } catch (IllegalArgumentException e) {
                logFinest("Failed to set indent-amount property; cause: " + e.getMessage());
            }
            transformer.transform(xmlInput, xmlOutput);
            return xmlOutput.getWriter().toString();
        } catch (Exception e) {
            LOGGER.warning(e);
            return input;
        } finally {
            if (xmlOutput != null) {
                closeResource(xmlOutput.getWriter());
            }
        }
    }

    private static void logFinest(String message) {
        if (LOGGER.isFinestEnabled()) {
            LOGGER.finest(message);
        }
    }

    private static void metrics(XmlGenerator gen, MetricsConfig metrics) {
        gen.open("metrics", "enabled", metrics.isEnabled(), "jmxEnabled", metrics.isJmxEnabled())
           .node("retention-seconds", metrics.getRetentionSeconds())
           .node("collection-interval-seconds", metrics.getCollectionIntervalSeconds())
           .node("metrics-for-data-structures", metrics.isMetricsForDataStructuresEnabled())
           .close();
    }

    private static void edgeDefaults(XmlGenerator gen, EdgeConfig defaultEdge) {
        gen.open("edge-defaults")
           .node("queue-size", defaultEdge.getQueueSize())
           .node("packet-size-limit", defaultEdge.getPacketSizeLimit())
           .node("receive-window-multiplier", defaultEdge.getReceiveWindowMultiplier())
           .close();

    }

    private static void instance(XmlGenerator gen, InstanceConfig instance) {
        gen.open("instance")
           .node("cooperative-thread-count", instance.getCooperativeThreadCount())
           .node("flow-control-period", instance.getFlowControlPeriodMs())
           .node("backup-count", instance.getBackupCount())
           .node("scale-up-delay-millis", instance.getScaleUpDelayMillis())
           .node("lossless-restart-enabled", instance.isLosslessRestartEnabled())
           .close();
    }
}
