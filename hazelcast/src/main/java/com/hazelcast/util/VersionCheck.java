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

package com.hazelcast.util;

import com.hazelcast.instance.Node;
import com.hazelcast.logging.ILogger;
import org.w3c.dom.Document;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathFactory;
import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLConnection;

public final class VersionCheck {

    public static final int TIMEOUT = 1000;

    private VersionCheck() {
    }

    public static void check(final Node hazelcastNode, final String buildDate, final String version) {
        if (!hazelcastNode.getGroupProperties().VERSION_CHECK_ENABLED.getBoolean()) {
            return;
        }
        new Thread(new Runnable() {
            public void run() {
                doCheck(hazelcastNode, buildDate, version);
            }
        }).start();
    }

    private static void doCheck(Node hazelcastNode, String buildDate, String version) {
        ILogger logger = hazelcastNode.getLogger(VersionCheck.class.getName());
        String urlStr = "http://www.hazelcast.com/version.jsp?version=" + version;
        try {
            Document doc = fetchWebService(urlStr);
            if (doc != null) {
                org.w3c.dom.Node nodeFinal = (org.w3c.dom.Node) XPathFactory.newInstance().newXPath()
                        .evaluate("/hazelcast-version/final", doc, XPathConstants.NODE);
                String finalVersion = nodeFinal.getAttributes().getNamedItem("version").getTextContent();
                String finalDate = nodeFinal.getAttributes().getNamedItem("date").getTextContent();
                org.w3c.dom.Node nodeSnapshot = (org.w3c.dom.Node) XPathFactory.newInstance().newXPath()
                        .evaluate("/hazelcast-version/snapshot", doc, XPathConstants.NODE);
                String snapshotVersion = nodeSnapshot.getAttributes().getNamedItem("version").getTextContent();
                String snapshotDate = nodeSnapshot.getAttributes().getNamedItem("date").getTextContent();
                if (!version.contains("SNAPSHOT")) {
                    // final version...check final
                    int currentDate = Integer.parseInt(buildDate);
                    int finalOne = Integer.parseInt(finalDate);
                    if (currentDate < finalOne) {
                        StringBuilder sb = new StringBuilder("Newer version of Hazelcast is available.\n");
                        sb.append("======================================\n");
                        sb.append("\n");
                        sb.append("You are running " + version + "\t[" + buildDate + "]\n");
                        sb.append("Newer version " + finalVersion + "\t[" + finalDate + "]\n");
                        sb.append("\n");
                        sb.append("======================================\n");
                        logger.warning(sb.toString());
                    }
                } else {
                    // snapshot
                    int currentDate = Integer.parseInt(buildDate);
                    int availableOne = Integer.parseInt(snapshotDate);
                    if (currentDate < availableOne) {
                        StringBuilder sb = new StringBuilder("Newer version of Hazelcast snapshot is available.\n");
                        sb.append("======================================\n");
                        sb.append("\n");
                        sb.append("You are running " + version + "\t[" + buildDate + "]\n");
                        sb.append("Newer version " + snapshotVersion + "\t[" + snapshotDate + "]\n");
                        sb.append("\n");
                        sb.append("======================================\n");
                        logger.warning(sb.toString());
                    }
                }
            }
        } catch (Throwable e) {
        }
    }

    @edu.umd.cs.findbugs.annotations.SuppressWarnings("REC_CATCH_EXCEPTION")
    private static Document fetchWebService(String urlStr) {
        InputStream in = null;
        try {
            URL url = new URL(urlStr);
            URLConnection conn = url.openConnection();
            conn.setConnectTimeout(TIMEOUT * 2);
            conn.setReadTimeout(TIMEOUT * 2);
            in = new BufferedInputStream(conn.getInputStream());
            final DocumentBuilder builder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
            return builder.parse(in);
        } catch (Exception ignored) {
        } finally {
            if (in != null) {
                try {
                    in.close();
                } catch (IOException e) {
                }
            }
        }
        return null;
    }
}
