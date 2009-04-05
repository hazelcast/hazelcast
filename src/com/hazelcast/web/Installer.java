/* 
 * Copyright (c) 2007-2008, Hazel Ltd. All Rights Reserved.
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
 *
 */

package com.hazelcast.web;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import com.hazelcast.impl.Build;
import com.hazelcast.impl.Util;

public class Installer {

	protected static Logger logger = Logger.getLogger(Installer.class.getName());

	private static final boolean DEBUG = Build.DEBUG;

	private String clusteredFilePrefix = "clustered-";

	private boolean addHazellib = true;

	private boolean replaceOld = false;

	private boolean appsSharingSessions = false;

    private boolean wrapServletAndJSP = true;

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

	public boolean isAddHazellib() {
		return addHazellib;
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
				} else if (entry.getName().endsWith(".jsp")) {
					modifyJSP(in, out);
				} else {
					Util.copyStream(in, out);
				}
				in.close();
			}
			if (jarLocation == null)
				jarLocation = "";

			if (isAddHazellib()) {
				log("Jar Location " + jarLocation);
				final ZipEntry hazelcastZip = new ZipEntry(jarLocation + "hazelcast.jar");
				out.putNextEntry(hazelcastZip);
				final InputStream in = new FileInputStream("hazelcast.jar");
				Util.copyStream(in, out);
				in.close();

			}
			out.flush();
			out.close();

		} catch (final Exception e) {
			e.printStackTrace();
		}
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
				} else if ("servlet".equals(name)) {
					final NodeList nl = node.getChildNodes();
					for (int a = 0; a < nl.getLength(); a++) {
						final Node n = nl.item(a);
						if (n.getNodeName().equals("servlet-class")) {
							final String className = n.getTextContent();
							final String wrapperClass = "com.hazelcast.web.ServletWrapper";
							final String hazelParam = "*hazelcast-base-servlet-class";
							n.setTextContent(wrapperClass);
							Node initParam = null;
							initParam = append(doc, node, "init-param", null);
							append(doc, initParam, "param-name", hazelParam);
							append(doc, initParam, "param-value", className);
							node.appendChild(initParam);
						}
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
		append(doc, initParam, "param-name", "listener-count");
		append(doc, initParam, "param-value", String.valueOf(lsListeners.size()));

		initParam = append(doc, filter, "init-param", null);
		append(doc, initParam, "param-name", "apps-sharing-sessions");
		append(doc, initParam, "param-value", String.valueOf(appsSharingSessions));

		if (sessionTimeout != sessionTimeoutDefault) {
			initParam = append(doc, filter, "init-param", null);
			append(doc, initParam, "param-name", "session-timeout");
			append(doc, initParam, "param-value", "" + sessionTimeout);
		}
		int counter = 0;
		for (final String listener : lsListeners) {
			initParam = append(doc, filter, "init-param", null);
			append(doc, initParam, "param-name", "listener" + counter++);
			append(doc, initParam, "param-value", listener);
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

		final Element contextListener = doc.createElement("listener");
		append(doc, contextListener, "listener-class",
				"com.hazelcast.web.WebFilter$ContextListener");

		docElement.insertBefore(filterMapping, after(docElement, "filter"));

		docElement.insertBefore(contextListener, after(docElement, "filter-mapping"));

		return doc;
	}

	public void readModifyWrite(final InputStream in, final OutputStream out) {
		final Document finalDoc = modifyWebXml(createDocument(in));
		Util.streamXML(finalDoc, out);
		if (DEBUG)
			Util.streamXML(finalDoc, System.out);
	}

	public void setAddHazellib(final boolean addHazellib) {
		this.addHazellib = addHazellib;
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
				Util.copyStream(in, out);
				in.close();
				out.close();
			}

			zipFile.close();
		} catch (final IOException ioe) {
			ioe.printStackTrace();
			return;
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

	private String getTextContent(final Node node) {
		final Node child = node.getFirstChild();
		if (child != null) {
			final Node next = child.getNextSibling();
			if (next == null) {
				return hasTextContent(child) ? getTextContent(child) : "";
			}
			final StringBuffer buf = new StringBuffer();
			getTextContent(node, buf);
			return buf.toString();
		}
		return "";
	}

	private void getTextContent(final Node node, final StringBuffer buf) {
		Node child = node.getFirstChild();
		while (child != null) {
			if (hasTextContent(child)) {
				getTextContent(child, buf);
			}
			child = child.getNextSibling();
		}

	}

	private boolean hasTextContent(final Node child) {
		return child.getNodeType() != Node.COMMENT_NODE
				&& child.getNodeType() != Node.PROCESSING_INSTRUCTION_NODE
				&& (child.getNodeType() != Node.TEXT_NODE);
	}

	private void install(final String[] args) {
		if (args == null || args.length == 0) {
			print("No application is specified!");
			printHelp();
		}

		final Set<String> setApps = new HashSet<String>();
		for (final String arg : args) {
			if (arg.startsWith("-")) {
				if (arg.equals("-apps-sharing-sessions")) {
					appsSharingSessions = true;
					addHazellib = false;
				}
			} else {
				setApps.add(arg);
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

	private void modifyJSP(final InputStream in, final OutputStream out) {
		log("Modifying JSP");
		try {
			final BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(out));
			final BufferedReader br = new BufferedReader(new InputStreamReader(in));
			String strLine;
			if (wrapServletAndJSP) {
                bw.write("<%@ page extends=\"com.hazelcast.web.JspWrapper\" %>\n");
            }
			while ((strLine = br.readLine()) != null) {
				bw.write(strLine);
				bw.write('\n');
			}
			bw.flush();
		} catch (final Exception e) {
			e.printStackTrace();
		}
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
					Util.copyStream(zin, bosWebXml);
					bosWebXml.flush();

					final byte[] webxmlBytes = bosWebXml.toByteArray();
					bosWebXml.close();
					final ByteArrayInputStream binWebXml = new ByteArrayInputStream(webxmlBytes);
					readModifyWrite(binWebXml, out);
					binWebXml.close();
				} else if (entry.getName().endsWith(".jsp")) {
					modifyJSP(zin, out);
				} else {
					Util.copyStream(zin, out);
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

	private final void processApp(final String appFilename) {
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
