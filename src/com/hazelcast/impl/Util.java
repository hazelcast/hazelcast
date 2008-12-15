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
 
package com.hazelcast.impl;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;

import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.w3c.dom.Document;

public class Util {
	public static void logWarning(String msg) {
		log(msg, "WARNING");
	}

	public static void logFatal(String msg) {
		log(msg, "FATAL =");
	}

	public static void log(String msg, String type) {
		System.out.println("=============== Hazelcast WARNING ==================");
		System.out.println(msg);
		System.out.println("====================================================");
	}

	public static String inputStreamToString(InputStream in) throws IOException {
		BufferedReader br = new BufferedReader(new InputStreamReader(in));
		StringBuffer sb = new StringBuffer();
		String line;
		while ((line = br.readLine()) != null) {
			sb.append(line).append("\n");
		}
		return sb.toString();
	}

	public static void streamXML(Document doc, OutputStream out) {
		try {// Use a Transformer for output
			TransformerFactory tFactory = TransformerFactory.newInstance();
			Transformer transformer = tFactory.newTransformer();

			if (doc.getDoctype() != null) {
				String systemId = doc.getDoctype().getSystemId();
				String publicId = doc.getDoctype().getPublicId();
				transformer.setOutputProperty(OutputKeys.DOCTYPE_PUBLIC, publicId);
				transformer.setOutputProperty(OutputKeys.DOCTYPE_SYSTEM, systemId);
			}

			transformer.setOutputProperty(OutputKeys.INDENT, "yes");

			DOMSource source = new DOMSource(doc);
			StreamResult result = new StreamResult(out);
			transformer.transform(source, result);

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static final void copyStream(InputStream in, OutputStream out) throws IOException {
		byte[] buffer = new byte[1024];
		int len;
		int total = 0;
		while ((len = in.read(buffer)) >= 0) {
			out.write(buffer, 0, len);
			total += len;
		}
	}

	public static final void copyFile(File src, File dest) {
		try {
			FileInputStream in = new FileInputStream(src);
			FileOutputStream out = new FileOutputStream(dest);
			copyStream(in, out);
			in.close();
			out.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static final void writeText(String str, OutputStream out) {
		try {
			BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(out));
			bw.write(str);
			bw.flush();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
