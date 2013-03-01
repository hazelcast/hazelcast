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

package com.hazelcast.web;

import com.hazelcast.util.StreamUtil;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.*;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

public class Installer {

    protected static final Logger logger = Logger.getLogger(Installer.class.getName());

    private static final boolean DEBUG = false;

    private String clusteredFilePrefix = "clustered-";

    private boolean addHazelcastLib = true;

    private boolean replaceOld = false;

    private boolean appsSharingSessions = false;

    private String version = "1.9.4";

    private String libDir = "../lib/";

    public static void main(final String[] args) {
        final Installer installer = new Installer();
        installer.install(args);
    }

    public Document createDocument(final InputStream in) {
        Document doc = null;
        try {
            final DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            final DocumentBuilder builder = factory.newDocumentBuilder();
            doc = builder.parse(in);
        } catch (final Exception e) {
            e.printStackTrace();
        }
        return doc;
    }

    public String getClusteredFilePrefix() {
        return clusteredFilePrefix;
    }

    public boolean isAddHazelcastLib() {
        return addHazelcastLib;
    }

    public boolean isAppsSharingSessions() {
        return appsSharingSessions;
    }

    public boolean isReplaceOld() {
        return replaceOld;
    }

    public final void modify(final File src, final File dest) {
        try {
            final ZipOutputStream out = new ZipOutputStream(new BufferedOutputStream(
                    new FileOutputStream(dest)));
            final ZipFile zipFile = new ZipFile(src);
            final Enumeration entries = zipFile.entries();
            String jarLocation = null;
            if (src.getName().endsWith("war"))
                jarLocation = "WEB-INF/lib/";
            while (entries.hasMoreElements()) {
                final ZipEntry entry = (ZipEntry) entries.nextElement();
                log("entry name " + entry.getName());
                out.putNextEntry(new ZipEntry(entry.getName()));
                if (entry.isDirectory()) {
                    continue;
                }
                final String name = entry.getName();
                if (jarLocation == null) {
                    if (name.endsWith(".jar")) {
                        final int slashIndex = name.lastIndexOf('/');
                        if (slashIndex == -1) {
                            jarLocation = "";
                        } else {
                            jarLocation = name.substring(0, slashIndex) + "/";
                        }
                    }
                }
                final InputStream in = zipFile.getInputStream(entry);
                if (name.endsWith(".war")) {
                    modifyWar(in, out);
                } else if (name.equals("WEB-INF/web.xml")) {
                    readModifyWrite(in, out);
                } else {
                    StreamUtil.copyStream(in, out);
                }
                in.close();
            }
            if (jarLocation == null)
                jarLocation = "";
            if (isAddHazelcastLib()) {
                log("Jar Location " + jarLocation);
                String hazelcastJarName = "hazelcast-" + version + ".jar";
                String hazelcastWMJarName = "hazelcast-wm-" + version + ".jar";
                addFileToZipStream((libDir + hazelcastJarName), (jarLocation + hazelcastJarName), out);
                addFileToZipStream((libDir + hazelcastWMJarName), (jarLocation + hazelcastWMJarName), out);
            }
            out.flush();
            out.close();
        } catch (final Exception e) {
            e.printStackTrace();
        }
    }

    void addFileToZipStream(String fileName, String zipFilePath, ZipOutputStream out) throws Exception {
        final ZipEntry hazelcastZip = new ZipEntry(zipFilePath);
        out.putNextEntry(hazelcastZip);
        final InputStream in = new FileInputStream(fileName);
        StreamUtil.copyStream(in, out);
        in.close();
    }

    public Document modifyWebXml(final Document doc) {
        final Element docElement = doc.getDocumentElement();
        final NodeList nodelist = docElement.getChildNodes();
        final List<String> lsListeners = new ArrayList<String>();
        Node firstFilter = null;
        Node displayElement = null;
        Node afterDisplayElement = null;
        final int sessionTimeoutDefault = -23490375;
        int sessionTimeout = sessionTimeoutDefault;
        for (int i = 0; i < nodelist.getLength(); i++) {
            final Node node = nodelist.item(i);
            final String name = node.getNodeName();
            if ("display-name".equals(name)) {
                displayElement = node;
            } else {
                if ("listener".equals(name)) {
                    String className = null;
                    final NodeList nl = node.getChildNodes();
                    for (int a = 0; a < nl.getLength(); a++) {
                        final Node n = nl.item(a);
                        if (n.getNodeName().equals("listener-class")) {
                            className = n.getTextContent();
                        }
                    }
                    lsListeners.add(className);
                    docElement.removeChild(node);
                } else if ("filter".equals(name)) {
                    if (firstFilter == null) {
                        firstFilter = node;
                    }
                } else if ("session-config".equals(name)) {
                    final NodeList nl = node.getChildNodes();
                    for (int a = 0; a < nl.getLength(); a++) {
                        final Node n = nl.item(a);
                        if (n.getNodeName().equals("session-timeout")) {
                            try {
                                sessionTimeout = Integer.parseInt(n.getTextContent().trim());
                            } catch (final Exception e) {
                                e.printStackTrace();
                            }
                        }
                    }
                }
                if (displayElement != null && afterDisplayElement == null) {
                    afterDisplayElement = node;
                }
            }
        }
        final Element filter = doc.createElement("filter");
        append(doc, filter, "filter-name", "hazel-filter");
        append(doc, filter, "filter-class", "com.hazelcast.web.WebFilter");
        Node initParam = append(doc, filter, "init-param", null);
        append(doc, initParam, "param-name", "apps-sharing-sessions");
        append(doc, initParam, "param-value", String.valueOf(appsSharingSessions));
        if (sessionTimeout != sessionTimeoutDefault) {
            initParam = append(doc, filter, "init-param", null);
            append(doc, initParam, "param-name", "session-timeout");
            append(doc, initParam, "param-value", "" + sessionTimeout);
        }
        Node first = firstFilter;
        if (first == null) {
            if (afterDisplayElement != null) {
                first = afterDisplayElement;
            }
        }
        if (first == null) {
            first = docElement.getFirstChild();
        }
        docElement.insertBefore(filter, first);
        final Element filterMapping = doc.createElement("filter-mapping");
        append(doc, filterMapping, "filter-name", "hazel-filter");
        append(doc, filterMapping, "url-pattern", "/*");
        append(doc, filterMapping, "dispatcher", "FORWARD");
        append(doc, filterMapping, "dispatcher", "INCLUDE");
        append(doc, filterMapping, "dispatcher", "REQUEST");
        final Element contextListener = doc.createElement("listener");
        append(doc, contextListener, "listener-class",
                "com.hazelcast.web.WebFilter$ContextListener");
        docElement.insertBefore(filterMapping, after(docElement, "filter"));
        docElement.insertBefore(contextListener, after(docElement, "filter-mapping"));
        return doc;
    }

    public void readModifyWrite(final InputStream in, final OutputStream out) {
        final Document finalDoc = modifyWebXml(createDocument(in));
        StreamUtil.streamXML(finalDoc, out);
        if (DEBUG)
            StreamUtil.streamXML(finalDoc, System.out);
    }

    public void setAddHazelcastLib(final boolean addHazelcastLib) {
        this.addHazelcastLib = addHazelcastLib;
    }

    public void setAppsSharingSessions(final boolean appsSharingSessions) {
        this.appsSharingSessions = appsSharingSessions;
    }

    public void setClusteredFilePrefix(final String clusteredFilePrefix) {
        this.clusteredFilePrefix = clusteredFilePrefix;
    }

    public void setReplaceOld(final boolean replaceOld) {
        this.replaceOld = replaceOld;
    }

    public final void unzip(final File file) {
        final String warFilename = file.getName();
        final String destDirName = warFilename.substring(0, warFilename.lastIndexOf('.'));
        Enumeration entries;
        ZipFile zipFile;
        final File destDir = new File(destDirName);
        destDir.mkdirs();
        try {
            zipFile = new ZipFile(file);
            entries = zipFile.entries();
            while (entries.hasMoreElements()) {
                final ZipEntry entry = (ZipEntry) entries.nextElement();
                if (entry.isDirectory()) {
                    (new File(destDir, entry.getName())).mkdir();
                    continue;
                }
                final File entryFile = new File(destDir, entry.getName());
                final InputStream in = zipFile.getInputStream(entry);
                final OutputStream out = new BufferedOutputStream(new FileOutputStream(entryFile));
                StreamUtil.copyStream(in, out);
                in.close();
                out.close();
            }
            zipFile.close();
        } catch (final IOException ioe) {
            ioe.printStackTrace();
        }
    }

    private Node after(final Node parent, final String nodeName) {
        final NodeList nodelist = parent.getChildNodes();
        int index = -1;
        for (int i = 0; i < nodelist.getLength(); i++) {
            final Node node = nodelist.item(i);
            final String name = node.getNodeName();
            if (nodeName.equals(name)) {
                index = i;
            }
        }
        if (index == -1)
            return null;
        if (nodelist.getLength() > (index + 1)) {
            return nodelist.item(index + 1);
        }
        return null;
    }

    private Node append(final Document doc, final Node parent, final String element,
                        final String value) {
        final Element child = doc.createElement(element);
        if (value != null)
            child.setTextContent(value);
        parent.appendChild(child);
        return child;
    }

    private void install(final String[] args) {
        if (args == null || args.length == 0) {
            print("No application is specified!");
            printHelp();
        }
        final Set<String> setApps = new HashSet<String>();
        if (args != null) {
            for (final String arg : args) {
                if (arg.startsWith("-")) {
                    if (arg.equals("-apps-sharing-sessions")) {
                        appsSharingSessions = true;
                        addHazelcastLib = false;
                    } else if (arg.startsWith("-version")) {
                        version = arg.substring(arg.indexOf("-version") + "-version".length());
                    } else if (arg.startsWith("-libDir")) {
                        libDir = arg.substring(arg.indexOf("-libDir") + "-libDir".length());
                    } else if (arg.startsWith("-excludeJar")) {
                        addHazelcastLib = false;
                    }
                } else {
                    setApps.add(arg);
                }
            }
        }
        for (final String appFilename : setApps) {
            processApp(appFilename);
        }
        logger.log(Level.INFO, "Done!");
    }

    private void log(final Object obj) {
        if (DEBUG)
            logger.log(Level.INFO, obj.toString());
    }

    private void modifyWar(final InputStream in, final OutputStream os) {
        final ByteArrayOutputStream bos = new ByteArrayOutputStream();
        final ZipOutputStream out = new ZipOutputStream(bos);
        final ZipInputStream zin = new ZipInputStream(in);
        try {
            while (true) {
                final ZipEntry entry = zin.getNextEntry();
                if (entry == null)
                    break;
                out.putNextEntry(new ZipEntry(entry.getName()));
                log("war file " + entry.getName());
                if (entry.isDirectory()) {
                    continue;
                }
                if (entry.getName().equals("WEB-INF/web.xml")) {
                    final ByteArrayOutputStream bosWebXml = new ByteArrayOutputStream();
                    StreamUtil.copyStream(zin, bosWebXml);
                    bosWebXml.flush();
                    final byte[] webxmlBytes = bosWebXml.toByteArray();
                    bosWebXml.close();
                    final ByteArrayInputStream binWebXml = new ByteArrayInputStream(webxmlBytes);
                    readModifyWrite(binWebXml, out);
                    binWebXml.close();
                } else {
                    StreamUtil.copyStream(zin, out);
                }
                out.flush();
            }
        } catch (final Exception e) {
            e.printStackTrace();
            if (DEBUG)
                System.exit(0);
        } finally {
            try {
                out.flush();
                out.close();
                bos.writeTo(os);
            } catch (final IOException e) {
                e.printStackTrace();
            }
        }
    }

    private void print(final Object obj) {
        logger.log(Level.INFO, obj.toString());
    }

    private void printHelp() {
        print("clusterWebapp.bat <war-file-path or ear-file-path>");
    }

    private void processApp(final String appFilename) {
        final File file = new File(appFilename);
        final String clusteredFileName = clusteredFilePrefix + file.getName();
        final File fileOriginal = new File(file.getParentFile(), clusteredFileName);
        modify(file, fileOriginal);
        if (isReplaceOld()) {
            final boolean success = file.renameTo(fileOriginal);
            if (success) {
                logger.log(Level.INFO, "old Application File was replaced!");
            }
        }
        logger.log(Level.INFO, "Done. New clustered application at "
                + fileOriginal.getAbsolutePath());
    }
}
