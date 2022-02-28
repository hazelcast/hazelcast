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

package com.hazelcast.internal.util;

import static com.hazelcast.internal.nio.IOUtil.closeResource;

import java.io.StringReader;
import java.io.StringWriter;

import javax.annotation.Nullable;
import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParserFactory;
import javax.xml.stream.XMLInputFactory;
import javax.xml.transform.ErrorListener;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Source;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.SchemaFactory;

import org.xml.sax.SAXException;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

/**
 * Utility class for XML processing. It contains several methods to retrieve XML processing factories with XXE protection
 * enabled (based on recommendation in the
 * <a href="https://cheatsheetseries.owasp.org/cheatsheets/XML_External_Entity_Prevention_Cheat_Sheet.html">OWASP XXE prevention
 * cheat-sheet</a>).
 */
public final class XmlUtil {

    /**
     * System property name which allows ignoring failures during enabling the XML External Entity protection.
     * This property should only be used as a last
     * resort. Hazelcast uses the XXE protection by setting properties {@link XMLConstants#ACCESS_EXTERNAL_DTD} and
     * {@link XMLConstants#ACCESS_EXTERNAL_SCHEMA}. These properties are supported in modern XML processors (JAXP 1.5+, Java
     * 8+). Old JAXP implementations on the classpath (e.g. Xerces, Xalan) may miss the support and they throw exception
     * during enabling the XXE protection. Setting this system property to true suppresses/ignores such Exceptions.
     */
    public static final String SYSTEM_PROPERTY_IGNORE_XXE_PROTECTION_FAILURES = "hazelcast.ignoreXxeProtectionFailures";

    private static final String FEATURES_DISALLOW_DOCTYPE = "http://apache.org/xml/features/disallow-doctype-decl";
    private static final ILogger LOGGER = Logger.getLogger(XmlUtil.class);

    private XmlUtil() {
    }

    /**
     * Returns namespace aware instance of {@link DocumentBuilderFactory} with XXE protection enabled.
     *
     * @throws ParserConfigurationException enabling XXE protection fail
     */
    public static DocumentBuilderFactory getNsAwareDocumentBuilderFactory() throws ParserConfigurationException {
        DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
        dbf.setNamespaceAware(true);
        setFeature(dbf, FEATURES_DISALLOW_DOCTYPE);
        return dbf;
    }

    /**
     * Returns {@link TransformerFactory} with XXE protection enabled.
     */
    public static TransformerFactory getTransformerFactory() {
        TransformerFactory transformerFactory = TransformerFactory.newInstance();
        setAttribute(transformerFactory, XMLConstants.ACCESS_EXTERNAL_DTD);
        setAttribute(transformerFactory, XMLConstants.ACCESS_EXTERNAL_STYLESHEET);
        return transformerFactory;
    }

    /**
     * Returns {@link SchemaFactory} with XXE protection enabled.
     */
    public static SchemaFactory getSchemaFactory() throws SAXException {
        SchemaFactory schemaFactory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
        setProperty(schemaFactory, XMLConstants.ACCESS_EXTERNAL_SCHEMA);
        setProperty(schemaFactory, XMLConstants.ACCESS_EXTERNAL_DTD);
        return schemaFactory;
    }

    /**
     * Returns {@link SAXParserFactory} with XXE protection enabled.
     */
    public static SAXParserFactory getSAXParserFactory() throws ParserConfigurationException, SAXException {
        SAXParserFactory factory = SAXParserFactory.newInstance();
        setFeature(factory, FEATURES_DISALLOW_DOCTYPE);
        return factory;
    }

    /**
     * Returns {@link XMLInputFactory} with XXE protection enabled.
     */
    public static XMLInputFactory getXMLInputFactory() {
        return getProtectedFactory(XMLInputFactory.newInstance());
    }

    /**
     * Returns {@link XMLInputFactory} with XXE protection enabled.
     *
     * @param xmlInputFactory {@link XMLInputFactory} to protect
     */
    public static XMLInputFactory getProtectedFactory(XMLInputFactory xmlInputFactory) {
        setProperty(xmlInputFactory, XMLInputFactory.SUPPORT_DTD, false);
        return xmlInputFactory;
    }

    /**
     * Formats given XML String with the given indentation used. If the {@code input} XML string is {@code null}, or
     * {@code indent} parameter is negative, or XML transformation fails, then the original value is returned unchanged. The
     * {@link IllegalArgumentException} is thrown when {@code indent==0}.
     *
     * @param input the XML String
     * @param indent indentation (number of spaces used for one indentation level)
     * @return formatted XML String or the original String if the formatting fails.
     * @throws IllegalArgumentException when indentation is equal to zero
     */
    @SuppressWarnings("checkstyle:NPathComplexity")
    public static String format(@Nullable String input, int indent) throws IllegalArgumentException {
        if (input == null || indent < 0) {
            return input;
        }
        if (indent == 0) {
            throw new IllegalArgumentException("Indentation must not be 0.");
        }
        StreamResult xmlOutput = null;
        try {
            Source xmlInput = new StreamSource(new StringReader(input));
            xmlOutput = new StreamResult(new StringWriter());
            TransformerFactory transformerFactory = getTransformerFactory();
            /*
             * Older versions of Xalan still use this method of setting indent values.
             * Attempt to make this work but don't completely fail if it's a problem.
             */
            try {
                transformerFactory.setAttribute("indent-number", indent);
            } catch (IllegalArgumentException e) {
                if (LOGGER.isFinestEnabled()) {
                    LOGGER.finest("Failed to set indent-number attribute; cause: " + e.getMessage());
                }
            }
            Transformer transformer = transformerFactory.newTransformer();
            // workaround IBM Java behavior - the silent ignorance of issues during the transformation.
            transformer.setErrorListener(ThrowingErrorListener.INSTANCE);
            transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");
            transformer.setOutputProperty(OutputKeys.ENCODING, "UTF-8");
            transformer.setOutputProperty(OutputKeys.INDENT, "yes");
            /*
             * Newer versions of Xalan will look for a fully-qualified output property in order to specify amount of
             * indentation to use. Attempt to make this work as well but again don't completely fail if it's a problem.
             */
            try {
                transformer.setOutputProperty("{http://xml.apache.org/xslt}indent-amount", Integer.toString(indent));
            } catch (IllegalArgumentException e) {
                if (LOGGER.isFinestEnabled()) {
                    LOGGER.finest("Failed to set indent-amount property; cause: " + e.getMessage());
                }
            }
            transformer.transform(xmlInput, xmlOutput);
            return xmlOutput.getWriter().toString();
        } catch (Exception e) {
            LOGGER.warning(e);
            return input;
        } finally {
            if (xmlOutput != null) {
                closeResource(xmlOutput.getWriter());
            }
        }
    }

    /**
     * Returns ErrorListener implementation which just throws the original error.
     */
    public ErrorListener getErrorListener() {
        return ThrowingErrorListener.INSTANCE;
    }

    static void setAttribute(TransformerFactory transformerFactory, String attributeName) {
        try {
            transformerFactory.setAttribute(attributeName, "");
        } catch (IllegalArgumentException iae) {
            printWarningAndRethrowEventually(iae, TransformerFactory.class, "attribute " + attributeName);
        }
    }

    static void setFeature(DocumentBuilderFactory dbf, String featureName) throws ParserConfigurationException {
        try {
            dbf.setFeature(featureName, true);
        } catch (ParserConfigurationException e) {
            printWarningAndRethrowEventually(e, DocumentBuilderFactory.class, "feature " + featureName);
        }
    }

    static void setFeature(SAXParserFactory saxParserFactory, String featureName)
            throws ParserConfigurationException, SAXException {
        try {
            saxParserFactory.setFeature(featureName, true);
        } catch (SAXException e) {
            printWarningAndRethrowEventually(e, SAXParserFactory.class, "feature " + featureName);
        } catch (ParserConfigurationException e) {
            printWarningAndRethrowEventually(e, SAXParserFactory.class, "feature " + featureName);
        }
    }

    static void setProperty(SchemaFactory schemaFactory, String propertyName) throws SAXException {
        try {
            schemaFactory.setProperty(propertyName, "");
        } catch (SAXException e) {
            printWarningAndRethrowEventually(e, SchemaFactory.class, "property " + propertyName);
        }
    }

    static void setProperty(XMLInputFactory xmlInputFactory, String propertyName, Object value) {
        try {
            xmlInputFactory.setProperty(propertyName, value);
        } catch (IllegalArgumentException e) {
            printWarningAndRethrowEventually(e, XMLInputFactory.class, "property " + propertyName);
        }
    }

    private static <T extends Exception> void printWarningAndRethrowEventually(T cause, Class<?> clazz, String objective)
            throws T {
        String className = clazz.getSimpleName();
        if (Boolean.getBoolean(SYSTEM_PROPERTY_IGNORE_XXE_PROTECTION_FAILURES)) {
            LOGGER.warning("Enabling XXE protection failed. The " + objective + " is not supported by the " + className
                    + ". The " + SYSTEM_PROPERTY_IGNORE_XXE_PROTECTION_FAILURES
                    + " system property is used so the XML processing continues in the UNSECURE mode"
                    + " with XXE protection disabled!!!");
        } else {
            LOGGER.severe(
                    "Enabling XXE protection failed. The " + objective + " is not supported by the " + className
                            + ". This usually mean an outdated XML processor"
                            + " is present on the classpath (e.g. Xerces, Xalan). If you are not able to resolve the issue by"
                            + " fixing the classpath, the " + SYSTEM_PROPERTY_IGNORE_XXE_PROTECTION_FAILURES
                            + " system property can be used to disable XML External Entity protections."
                            + " We don't recommend disabling the XXE as such the XML processor configuration is unsecure!",
                    cause);
            throw cause;
        }
    }

    /**
     * ErrorListener implementation which just throws the error. It workarounds IBM Java default behaviour when
     * {@link Transformer#transform(Source, javax.xml.transform.Result)} finishes without problems even if an exception is
     * thrown within the call. If an error happens, we want to know about it and handle it properly.
     */
    static final class ThrowingErrorListener implements ErrorListener {
        public static final ThrowingErrorListener INSTANCE = new ThrowingErrorListener();

        private ThrowingErrorListener() {
        }

        @Override
        public void warning(TransformerException exception) throws TransformerException {
            throw exception;
        }

        @Override
        public void fatalError(TransformerException exception) throws TransformerException {
            throw exception;
        }

        @Override
        public void error(TransformerException exception) throws TransformerException {
            throw exception;
        }
    }

}
