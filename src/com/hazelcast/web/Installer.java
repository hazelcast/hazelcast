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
import java.util.List;
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

	private static final boolean DEBUG = Build.get().BASE_DEBUG;
	private String clusteredFilePrefix = "clustered-";
	private boolean addHazellib = true;
	private boolean replaceOld = false;

	public static void main(String[] args) {
		Installer installer = new Installer();
		installer.install(args);
	}

	private void install(String[] args) {
		if (args == null || args.length == 0) {
			print("No application is specified!");
			printHelp();
		}

		File file = new File(args[0]);
		String clusteredFileName = clusteredFilePrefix + file.getName();
		File fileOriginal = new File(file.getParentFile(), clusteredFileName);

		// file.renameTo(fileOriginal);
		// copyFile(file, fileOriginal);
		modify(file, fileOriginal);

		if (isReplaceOld()) {
			boolean success = file.renameTo(fileOriginal);
			if (success) {
				System.out.println("old Application File was replaced!");
			}
		}
		System.out.println("Done. New clustered application at " + fileOriginal.getAbsolutePath());

	}

	public final void modify(File src, File dest) {

		try {
			ZipOutputStream out = new ZipOutputStream(new BufferedOutputStream(
					new FileOutputStream(dest)));
			ZipFile zipFile = new ZipFile(src);

			Enumeration entries = zipFile.entries();
			String jarLocation = null;
			if (src.getName().endsWith("war"))
				jarLocation = "WEB-INF/lib/";
			while (entries.hasMoreElements()) {
				ZipEntry entry = (ZipEntry) entries.nextElement();
				log("entry name " + entry.getName());
				out.putNextEntry(new ZipEntry(entry.getName()));

				if (entry.isDirectory()) {
					continue;
				}
				String name = entry.getName();

				if (jarLocation == null) {
					if (name.endsWith(".jar")) {
						int slashIndex = name.lastIndexOf('/');
						if (slashIndex == -1) {
							jarLocation = "";
						} else {
							jarLocation = name.substring(0, slashIndex) + "/";
						}
					}
				}

				InputStream in = zipFile.getInputStream(entry);
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
				ZipEntry hazelcastZip = new ZipEntry(jarLocation + "hazelcast.jar");
				out.putNextEntry(hazelcastZip);
				InputStream in = new FileInputStream("hazelcast.jar");
				Util.copyStream(in, out);
				in.close();

			}
			out.flush();
			out.close();

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private void modifyWar(InputStream in, OutputStream os) {
		ByteArrayOutputStream bos = new ByteArrayOutputStream();

		ZipOutputStream out = new ZipOutputStream(bos);
		ZipInputStream zin = new ZipInputStream(in);
		try {
			while (true) {
				ZipEntry entry = zin.getNextEntry();
				if (entry == null)
					break;
				out.putNextEntry(new ZipEntry(entry.getName()));
				log("war file " + entry.getName());
				if (entry.isDirectory()) {
					continue;
				}
				if (entry.getName().equals("WEB-INF/web.xml")) {
					ByteArrayOutputStream bosWebXml = new ByteArrayOutputStream();
					Util.copyStream(zin, bosWebXml);
					bosWebXml.flush();

					byte[] webxmlBytes = bosWebXml.toByteArray();
					bosWebXml.close();
					ByteArrayInputStream binWebXml = new ByteArrayInputStream(webxmlBytes);
					readModifyWrite(binWebXml, out);
					binWebXml.close();
				} else if (entry.getName().endsWith(".jsp")) {
					modifyJSP(zin, out);
				} else {
					Util.copyStream(zin, out);
				}
				out.flush();
			}

		} catch (Exception e) {
			e.printStackTrace();
			if (DEBUG)
				System.exit(0);
		} finally {
			try {
				out.flush();
				out.close();
				bos.writeTo(os);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	private void modifyJSP(InputStream in, OutputStream out) {
		log("Modifying JSP");
		try {
			BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(out));
			BufferedReader br = new BufferedReader(new InputStreamReader(in));
			String strLine;
			bw.write("<%@ page extends=\"com.hazelcast.web.JspWrapper\" %>\n");
			while ((strLine = br.readLine()) != null) {
				bw.write(strLine);
				bw.write('\n');
			}
			bw.flush();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void readModifyWrite(InputStream in, OutputStream out) {
		Document finalDoc = modifyWebXml(createDocument(in));
		Util.streamXML(finalDoc, out);
		if (DEBUG)
			Util.streamXML(finalDoc, System.out);
	}

	public Document createDocument(InputStream in) {
		Document doc = null;
		try {
			DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
			DocumentBuilder builder = factory.newDocumentBuilder();
			doc = builder.parse(in);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return doc;
	}

	public Document modifyWebXml(Document doc) {
		Element docElement = doc.getDocumentElement();
		NodeList nodelist = docElement.getChildNodes();
		List<String> lsListeners = new ArrayList<String>();
		Node firstFilter = null;
		Node displayElement = null;
		Node afterDisplayElement = null;
		int sessionTimeoutDefault = -23490375;
		int sessionTimeout = sessionTimeoutDefault;
		for (int i = 0; i < nodelist.getLength(); i++) {
			Node node = nodelist.item(i);
			String name = node.getNodeName();
			if ("display-name".equals(name)) {
				displayElement = node;
			} else {
				if ("listener".equals(name)) {
					String className = null;
					NodeList nl = node.getChildNodes();
					for (int a = 0; a < nl.getLength(); a++) {
						Node n = nl.item(a);
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
					NodeList nl = node.getChildNodes();
					for (int a = 0; a < nl.getLength(); a++) {
						Node n = nl.item(a);
						if (n.getNodeName().equals("servlet-class")) {
							String className = n.getTextContent();
							String wrapperClass = "com.hazelcast.web.ServletWrapper";
							String hazelParam = "*hazelcast-base-servlet-class";
							n.setTextContent(wrapperClass);
							Node initParam = null;
							initParam = append(doc, node, "init-param", null);
							append(doc, initParam, "param-name", hazelParam);
							append(doc, initParam, "param-value", className);
							node.appendChild(initParam);
						}
					}
				} else if ("session-config".equals(name)) {
					NodeList nl = node.getChildNodes();
					for (int a = 0; a < nl.getLength(); a++) {
						Node n = nl.item(a);
						if (n.getNodeName().equals("session-timeout")) {
							try {
								sessionTimeout = Integer.parseInt(n.getTextContent().trim());
							} catch (Exception e) {
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
		Element filter = doc.createElement("filter");
		append(doc, filter, "filter-name", "hazel-filter");
		append(doc, filter, "filter-class", "com.hazelcast.web.WebFilter");

		Node initParam = append(doc, filter, "init-param", null);
		append(doc, initParam, "param-name", "listener-count");
		append(doc, initParam, "param-value", String.valueOf(lsListeners.size()));
		if (sessionTimeout != sessionTimeoutDefault) {
			initParam = append(doc, filter, "init-param", null);
			append(doc, initParam, "param-name", "session-timeout");
			append(doc, initParam, "param-value", "" + sessionTimeout);
		}
		int counter = 0;
		for (String listener : lsListeners) {
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
		Element filterMapping = doc.createElement("filter-mapping");
		append(doc, filterMapping, "filter-name", "hazel-filter");
		append(doc, filterMapping, "url-pattern", "/*");

		Element contextListener = doc.createElement("listener");
		append(doc, contextListener, "listener-class",
				"com.hazelcast.web.WebFilter$ContextListener");

		docElement.insertBefore(filterMapping, after(docElement, "filter"));

		docElement.insertBefore(contextListener, after(docElement, "filter-mapping"));

		return doc;
	}

	private String getTextContent(Node node) {
		Node child = node.getFirstChild();
		if (child != null) {
			Node next = child.getNextSibling();
			if (next == null) {
				return hasTextContent(child) ? getTextContent(child) : "";
			}
			StringBuffer buf = new StringBuffer();
			getTextContent(node, buf);
			return buf.toString();
		}
		return "";
	}

	private void getTextContent(Node node, StringBuffer buf) {
		Node child = node.getFirstChild();
		while (child != null) {
			if (hasTextContent(child)) {
				getTextContent(child, buf);
			}
			child = child.getNextSibling();
		}

	}

	private boolean hasTextContent(Node child) {
		return child.getNodeType() != Node.COMMENT_NODE
				&& child.getNodeType() != Node.PROCESSING_INSTRUCTION_NODE
				&& (child.getNodeType() != Node.TEXT_NODE);
	}

	private Node after(Node parent, String nodeName) {
		NodeList nodelist = parent.getChildNodes();
		int index = -1;
		for (int i = 0; i < nodelist.getLength(); i++) {
			Node node = nodelist.item(i);
			String name = node.getNodeName();
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

	private Node append(Document doc, Node parent, String element, String value) {
		Element child = doc.createElement(element);
		if (value != null)
			child.setTextContent(value);
		parent.appendChild(child);
		return child;
	}

	public final void unzip(File file) {
		String warFilename = file.getName();
		String destDirName = warFilename.substring(0, warFilename.lastIndexOf('.'));
		Enumeration entries;
		ZipFile zipFile;
		File destDir = new File(destDirName);
		destDir.mkdirs();
		try {
			zipFile = new ZipFile(file);
			entries = zipFile.entries();
			while (entries.hasMoreElements()) {
				ZipEntry entry = (ZipEntry) entries.nextElement();
				if (entry.isDirectory()) {
					(new File(destDir, entry.getName())).mkdir();
					continue;
				}
				File entryFile = new File(destDir, entry.getName());
				InputStream in = zipFile.getInputStream(entry);
				OutputStream out = new BufferedOutputStream(new FileOutputStream(entryFile));
				Util.copyStream(in, out);
				in.close();
				out.close();
			}

			zipFile.close();
		} catch (IOException ioe) {
			ioe.printStackTrace();
			return;
		}
	}

	private void log(Object obj) {
		if (DEBUG)
			System.out.println(obj);
	}

	private void print(Object obj) {
		System.out.println(obj);
	}

	private void printHelp() {
		print("clusterWebapp.bat <war-file-path or ear-file-path>");
	}

	public String getClusteredFilePrefix() {
		return clusteredFilePrefix;
	}

	public void setClusteredFilePrefix(String clusteredFilePrefix) {
		this.clusteredFilePrefix = clusteredFilePrefix;
	}

	public boolean isAddHazellib() {
		return addHazellib;
	}

	public void setAddHazellib(boolean addHazellib) {
		this.addHazellib = addHazellib;
	}

	public boolean isReplaceOld() {
		return replaceOld;
	}

	public void setReplaceOld(boolean replaceOld) {
		this.replaceOld = replaceOld;
	}

}
