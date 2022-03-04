/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.config;

import com.hazelcast.config.ConfigRecognizer;
import com.hazelcast.config.ConfigStream;
import com.hazelcast.internal.util.XmlUtil;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;
import org.xml.sax.helpers.DefaultHandler;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import static com.hazelcast.internal.util.StringUtil.equalsIgnoreCase;
import static java.util.Objects.requireNonNull;

/**
 * Abstract {@link ConfigRecognizer} implementation that recognizes
 * Hazelcast XML configurations. The recognition is done by looking into
 * the provided configuration to check if the root node is the expected
 * one.
 * <p/>
 * This implementation uses a SAX parser. The parsing is aborted once the
 * root tag is processed.
 * <p/>
 * If the provided configuration is not a valid XML document, no exception
 * is thrown. Instead, the configuration is simply not recognized by this
 * implementation.
 * </p>
 * Note that this {@link ConfigRecognizer} doesn't validate the
 * configuration and doesn't look further into the provided configuration.
 */
public class AbstractXmlConfigRootTagRecognizer implements ConfigRecognizer {
    private final SAXParser saxParser;
    private final String expectedRootNode;
    private final ILogger logger = Logger.getLogger(AbstractXmlConfigRootTagRecognizer.class);

    public AbstractXmlConfigRootTagRecognizer(String expectedRootNode) throws Exception {
        this.expectedRootNode = expectedRootNode;
        SAXParserFactory factory = XmlUtil.getSAXParserFactory();
        saxParser = factory.newSAXParser();
    }

    @Override
    public boolean isRecognized(ConfigStream configStream) throws Exception {
        MemberHandler memberHandler = new MemberHandler(expectedRootNode);
        try {
            saxParser.parse(configStream, memberHandler);
        } catch (TerminateParseException ex) {
            // expected, always
        } catch (SAXParseException ex) {
            // thrown if the provided XML is not a valid XML
            handleParseException(ex);
            return false;
        } catch (Exception ex) {
            // thrown if any unexpected exception is encountered
            handleUnexpectedException(ex);
            throw ex;
        }
        return memberHandler.isMemberXml;
    }

    private void handleParseException(SAXParseException ex) {
        if (logger.isFineEnabled()) {
            logger.fine("An exception is encountered while processing the provided XML configuration", ex);
        }
    }

    private void handleUnexpectedException(Exception ex) {
        if (logger.isFineEnabled()) {
            logger.fine("An unexpected exception is encountered while processing the provided XML configuration", ex);
        }
    }

    private static final class MemberHandler extends DefaultHandler {
        private final String expectedRootNode;
        private boolean isMemberXml;

        private MemberHandler(String expectedRootNode) {
            this.expectedRootNode = requireNonNull(expectedRootNode);
        }

        @Override
        public void startElement(String uri, String localName, String qName, Attributes attributes) throws SAXException {
            if (equalsIgnoreCase(expectedRootNode, qName)) {
                isMemberXml = true;
            }
            throw new TerminateParseException();
        }
    }

    private static final class TerminateParseException extends SAXException {
    }
}
