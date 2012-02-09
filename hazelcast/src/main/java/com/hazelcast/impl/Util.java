/* 
 * Copyright (c) 2008-2010, Hazel Ltd. All Rights Reserved.
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

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import org.w3c.dom.Document;

import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.*;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

public class Util {

    final static ILogger logger = Logger.getLogger(Util.class.getName());

    public static int hashCode(final byte[] data) {
        if (data == null) return Integer.MIN_VALUE;
        // FNV (Fowler/Noll/Vo) Hash "1a"
        final int prime = 0x01000193;
        int hash = 0x811c9dc5;
        for (int i = data.length - 1; i >= 0; i--) {
            hash = (hash ^ data[i]) * prime;
        }
        return hash;
    }

    /**
     * -1 means infinite.
     * 0 means no-wait.
     *
     * @param duration
     * @param unit
     * @return
     */
    public static long toMillis(long duration, TimeUnit unit) {
        if (duration == 0 || unit == null) {
            return 0;
        } else if (duration < 0) {
            return -1;
        } else {
            return unit.toMillis(duration);
        }
    }

    public static void copyFile(final File src, final File dest) {
        try {
            final FileInputStream in = new FileInputStream(src);
            final FileOutputStream out = new FileOutputStream(dest);
            copyStream(in, out);
            in.close();
            out.close();
        } catch (final Exception e) {
            logger.log(Level.WARNING, e.getMessage(), e);
        }
    }

    public static void copyStream(final InputStream in, final OutputStream out)
            throws IOException {
        final byte[] buffer = new byte[1024];
        int len;
        while ((len = in.read(buffer)) >= 0) {
            out.write(buffer, 0, len);
        }
    }

    public static String inputStreamToString(final InputStream in) throws IOException {
        final BufferedReader br = new BufferedReader(new InputStreamReader(in));
        final StringBuffer sb = new StringBuffer();
        String line;
        while ((line = br.readLine()) != null) {
            sb.append(line).append("\n");
        }
        return sb.toString();
    }

    public static void streamXML(final Document doc, final OutputStream out) {
        try {// Use a Transformer for output
            final TransformerFactory tFactory = TransformerFactory.newInstance();
            final Transformer transformer = tFactory.newTransformer();
            if (doc.getDoctype() != null) {
                final String systemId = doc.getDoctype().getSystemId();
                final String publicId = doc.getDoctype().getPublicId();
                transformer.setOutputProperty(OutputKeys.DOCTYPE_PUBLIC, publicId);
                transformer.setOutputProperty(OutputKeys.DOCTYPE_SYSTEM, systemId);
            }
            transformer.setOutputProperty(OutputKeys.INDENT, "yes");
            final DOMSource source = new DOMSource(doc);
            final StreamResult result = new StreamResult(out);
            transformer.transform(source, result);
        } catch (final Exception e) {
            logger.log(Level.WARNING, e.getMessage(), e);
        }
    }

    public static void writeText(final String str, final OutputStream out) {
        try {
            final BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(out));
            bw.write(str);
            bw.flush();
        } catch (final Exception e) {
            logger.log(Level.WARNING, e.getMessage(), e);
        }
    }
}
