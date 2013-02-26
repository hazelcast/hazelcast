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

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import org.w3c.dom.Document;

import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.*;
import java.util.logging.Level;

public class StreamUtil {

    final static ILogger logger = Logger.getLogger(StreamUtil.class.getName());

    public static void copyStream(final InputStream in, final OutputStream out)
            throws IOException {
        final byte[] buffer = new byte[1024];
        int len;
        while ((len = in.read(buffer)) >= 0) {
            out.write(buffer, 0, len);
        }
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
}
