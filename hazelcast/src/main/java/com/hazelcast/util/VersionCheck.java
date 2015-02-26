/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.nio.IOUtil;
import org.w3c.dom.Document;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLConnection;
import java.nio.charset.Charset;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * Checks version of hazelcast with central server.
 */
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

    private MessageDigest md;

    public VersionCheck() {
        try {
            md = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException ignored) {
            EmptyStatement.ignore(ignored);
        }
    }

    public void check(final Node hazelcastNode, final String version, final boolean isEnterprise) {
        if (!hazelcastNode.getGroupProperties().VERSION_CHECK_ENABLED.getBoolean()) {
            return;
        }
        hazelcastNode.nodeEngine.getExecutionService().scheduleAtFixedRate(new Runnable() {
            public void run() {
                doCheck(hazelcastNode, version, isEnterprise);
            }
        }, 0, 1, TimeUnit.DAYS);
    }

    public void shutdown() {
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
        String downloadId = "source";
        InputStream is = null;
        try {
            is = getClass().getClassLoader().getResourceAsStream("hazelcast-download.properties");
            if (is != null) {
                final Properties properties = new Properties();
                properties.load(is);
                downloadId = properties.getProperty("hazelcastDownloadId");
            }
        } catch (IOException ignored) {
            EmptyStatement.ignore(ignored);
        } finally {
            IOUtil.closeResource(is);
        }

        String urlStr = "http://versioncheck.hazelcast.com/version.jsp?version=" + version
                + "&m=" + hazelcastNode.getLocalMember().getUuid()
                + "&e=" + isEnterprise
                + "&l=" + toMD5String(hazelcastNode.getConfig().getLicenseKey())
                + "&p=" + downloadId
                + "&c=" + toMD5String(hazelcastNode.getConfig().getGroupConfig().getName())
                + "&crsz=" + convertToLetter(hazelcastNode.getClusterService().getMembers().size())
                + "&cssz=" + convertToLetter(hazelcastNode.clientEngine.getClientEndpointCount());

        fetchWebService(urlStr);
    }

    private String toMD5String(String str) {
        if (md == null || str == null) {
            return "NULL";
        }
        byte[] byteData = md.digest(str.getBytes(Charset.forName("UTF-8")));

        //CHECKSTYLE:OFF suppressed because of magic numbers.
        StringBuilder sb = new StringBuilder();
        for (byte aByteData : byteData) {
            sb.append(Integer.toString((aByteData & 0xff) + 0x100, 16).substring(1));
        }
        //CHECKSTYLE:ON
        return sb.toString();
    }

    private Document fetchWebService(String urlStr) {
        InputStream in = null;
        try {
            URL url = new URL(urlStr);
            URLConnection conn = url.openConnection();
            conn.setRequestProperty("User-Agent", "Mozilla/5.0");
            conn.setConnectTimeout(TIMEOUT * 2);
            conn.setReadTimeout(TIMEOUT * 2);
            in = new BufferedInputStream(conn.getInputStream());
            final DocumentBuilder builder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
            return builder.parse(in);
        } catch (SAXException ignored) {
            EmptyStatement.ignore(ignored);
        } catch (IOException ignored) {
            EmptyStatement.ignore(ignored);
        } catch (ParserConfigurationException ignored) {
            EmptyStatement.ignore(ignored);
        } finally {
            IOUtil.closeResource(in);
        }

        return null;
    }
}
