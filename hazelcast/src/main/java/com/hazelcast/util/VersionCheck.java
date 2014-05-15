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
import org.w3c.dom.Document;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLClassLoader;
import java.net.URLConnection;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.jar.Attributes;
import java.util.jar.Manifest;

public final class VersionCheck {

    private static final int TIMEOUT = 1000;
    private static final int A_INTERVAL = 5;
    private static final int B_INTERVAL = 10;
    private static final int C_INTERVAL = 20;
    private static final int D_INTERVAL = 40;
    private static final int E_INTERVAL = 60;
    private static final int F_INTERVAL = 100;
    private static final int G_INTERVAL = 150;
    private static final int H_INTERVAL = 300;
    private static final int J_INTERVAL = 600;

    private ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();

    public VersionCheck() {
    }

    public void check(final Node hazelcastNode, final String version, final boolean isEnterprise) {
        if (!hazelcastNode.getGroupProperties().VERSION_CHECK_ENABLED.getBoolean()) {
            return;
        }
        executor.scheduleAtFixedRate(new Runnable() {
            public void run() {
                doCheck(hazelcastNode, version, isEnterprise);
            }
        }, 0, 1, TimeUnit.DAYS);
    }

    public void shutdown() {
        executor.shutdown();
    }

    private String convertToLetter(int size) {
        String letter;
        if (size < A_INTERVAL) {
            letter = "A";
        } else if (size < B_INTERVAL) {
            letter = "B";
        } else if (size < C_INTERVAL) {
            letter = "C";
        } else if (size < D_INTERVAL) {
            letter = "D";
        } else if (size < E_INTERVAL) {
            letter = "E";
        } else if (size < F_INTERVAL) {
            letter = "F";
        } else if (size < G_INTERVAL) {
            letter = "G";
        } else if (size < H_INTERVAL) {
            letter = "H";
        } else if (size < J_INTERVAL) {
            letter = "J";
        } else {
            letter = "I";
        }
        return letter;

    }

    private void doCheck(Node hazelcastNode, String version, boolean isEnterprise) {
        URLClassLoader cl = (URLClassLoader) getClass().getClassLoader();
        String p = "NULL";
        try {
            URL url = cl.findResource("META-INF/MANIFEST.MF");
            Manifest manifest = new Manifest(url.openStream());
            final Attributes mainAttributes = manifest.getMainAttributes();
            p = mainAttributes.getValue("clientId");
        } catch (IOException ignored) {

        }

        String urlStr = "http://www.hazelcast.com/version.jsp?version=" + version
                + "&m=" + hazelcastNode.getLocalMember().getUuid()
                + "&e=" + isEnterprise
                + "&l=" + hazelcastNode.getConfig().getLicenseKey()
                + "&p=" + p
                + "&c=" + hazelcastNode.getConfig().getGroupConfig().getName().hashCode()
                + "&crsz=" + convertToLetter(hazelcastNode.getClusterService().getMembers().size())
                + "&cssz=" + convertToLetter(hazelcastNode.clientEngine.getClientEndpointCount());

        fetchWebService(urlStr);
    }

    private Document fetchWebService(String urlStr) {
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
                } catch (IOException ignored) {
                }
            }
        }
        return null;
    }
}
