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

package com.hazelcast.config.replacer;

import com.hazelcast.core.HazelcastException;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.OverridePropertyRule;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.NetworkInterface;
import java.util.Enumeration;
import java.util.Properties;

import static com.hazelcast.config.replacer.EncryptionReplacer.encrypt;
import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.endsWith;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assume.assumeNotNull;

/**
 * Unit tests for {@link EncryptionReplacer}.
 */
@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class})
public class EncryptionReplacerTest extends AbstractPbeReplacerTest {

    private static final ILogger LOGGER = Logger.getLogger(EncryptionReplacerTest.class);
    private static final String interfaceName;

    private static final String XML_LEGACY_CONFIG = "    <config-replacers>\n"
            + "        <replacer class-name='" + EncryptionReplacer.class.getName() + "'>\n"
            + "            <properties>\n"
            + "                <property name='keyLengthBits'>64</property>\n"
            + "                <property name='saltLengthBytes'>8</property>\n"
            + "                <property name='cipherAlgorithm'>DES</property>\n"
            + "                <property name='secretKeyFactoryAlgorithm'>PBKDF2WithHmacSHA1</property>\n"
            + "                <property name='secretKeyAlgorithm'>DES</property>\n"
            + "            </properties>\n"
            + "        </replacer>\n"
            + "    </config-replacers>\n";

    private static final String XML_DEFAULT_CONFIG = "    <config-replacers>\n"
            + "        <replacer class-name='" + EncryptionReplacer.class.getName() + "'/>\n"
            + "    </config-replacers>\n";

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    @Rule
    public OverridePropertyRule userNameProperty = OverridePropertyRule.set("user.name", "test");

    @Rule
    public OverridePropertyRule hazelcastConfigProperty = OverridePropertyRule.clear("hazelcast.config");

    @Test
    public void testGetPrefix() {
        assertEquals("ENC", new EncryptionReplacer().getPrefix());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNoPasswordInputProvided() throws Exception {
        EncryptionReplacer replacer = new EncryptionReplacer();
        Properties properties = new Properties();
        properties.setProperty(EncryptionReplacer.PROPERTY_PASSWORD_USER_PROPERTIES, "false");
        replacer.init(properties);
    }

    @Test(expected = HazelcastException.class)
    public void testMissingFileWithPassword() throws Exception {
        assumeDefaultAlgorithmsSupported();
        EncryptionReplacer replacer = new EncryptionReplacer();
        Properties properties = new Properties();
        properties.setProperty(EncryptionReplacer.PROPERTY_PASSWORD_USER_PROPERTIES, "false");
        properties.setProperty(EncryptionReplacer.PROPERTY_PASSWORD_FILE, "/path/to/nonExistingFile");
        replacer.init(properties);
        replacer.encrypt("test", 1);
    }

    @Test
    public void testNetworkInterfaceUsed() throws Exception {
        assumeNotNull(interfaceName);
        assumeDefaultAlgorithmsSupported();

        Properties properties = new Properties();
        properties.setProperty(EncryptionReplacer.PROPERTY_PASSWORD_NETWORK_INTERFACE, interfaceName);
        properties.setProperty(EncryptionReplacer.PROPERTY_PASSWORD_USER_PROPERTIES, "false");
        assertReplacerWorks(createAndInitReplacer(properties));
    }

    @Test
    public void testUserChanged() throws Exception {
        assumeDefaultAlgorithmsSupported();
        AbstractPbeReplacer replacer = createAndInitReplacer(new Properties());
        userNameProperty.setOrClearProperty("somebodyElse");
        // String generated by replacer.encrypt("aTestString", 77)
        assertNull(replacer.getReplacement("Ddg0fskLeOc=:77:7jE2UZCh4a5HsHIQhUcxdg=="));
    }

    @Test
    public void testGenerateEncrypted() throws Exception {
        assumeDefaultAlgorithmsSupported();
        String xml = "<hazelcast xmlns=\"http://www.hazelcast.com/schema/config\">\n" + XML_DEFAULT_CONFIG + "</hazelcast>";
        File configFile = createFileWithString(xml);
        hazelcastConfigProperty.setOrClearProperty(configFile.getAbsolutePath());
        String encrypted = encrypt("test");
        assertThat(encrypted, allOf(startsWith("$ENC{"), endsWith("}")));
    }

    @Test
    public void testGenerateEncryptedLegacy() throws Exception {
        assumeAlgorithmsSupported("PBKDF2WithHmacSHA1", "DES");
        String xml = "<hazelcast xmlns=\"http://www.hazelcast.com/schema/config\">\n" + XML_LEGACY_CONFIG + "</hazelcast>";
        File configFile = createFileWithString(xml);
        hazelcastConfigProperty.setOrClearProperty(configFile.getAbsolutePath());
        String encrypted = encrypt("test");
        assertThat(encrypted, allOf(startsWith("$ENC{"), endsWith("}")));
    }

    @Test
    public void testClientGenerateEncrypted() throws Exception {
        assumeDefaultAlgorithmsSupported();
        String xml = "<hazelcast-client xmlns=\"http://www.hazelcast.com/schema/client-config\">\n" + XML_DEFAULT_CONFIG
                + "</hazelcast-client>";
        File configFile = createFileWithString(xml);
        hazelcastConfigProperty.setOrClearProperty(configFile.getAbsolutePath());
        String encrypted = encrypt("test");
        assertThat(encrypted, allOf(startsWith("$ENC{"), endsWith("}")));
    }

    @Test
    public void testClientGenerateEncryptedLegacy() throws Exception {
        assumeAlgorithmsSupported("PBKDF2WithHmacSHA1", "DES");
        String xml = "<hazelcast-client xmlns=\"http://www.hazelcast.com/schema/client-config\">\n" + XML_LEGACY_CONFIG
                + "</hazelcast-client>";
        File configFile = createFileWithString(xml);
        hazelcastConfigProperty.setOrClearProperty(configFile.getAbsolutePath());
        String encrypted = encrypt("test");
        assertThat(encrypted, allOf(startsWith("$ENC{"), endsWith("}")));
    }

    @Override
    protected AbstractPbeReplacer createAndInitReplacer(String password, Properties properties) throws Exception {
        File file = createFileWithString(password);
        properties.setProperty(EncryptionReplacer.PROPERTY_PASSWORD_USER_PROPERTIES, "false");
        properties.setProperty(EncryptionReplacer.PROPERTY_PASSWORD_FILE, file.getAbsolutePath());
        return createAndInitReplacer(properties);
    }

    private AbstractPbeReplacer createAndInitReplacer(Properties properties) {
        AbstractPbeReplacer replacer = new EncryptionReplacer();
        replacer.init(properties);
        return replacer;
    }

    private File createFileWithString(String string) throws IOException {
        File file = tempFolder.newFile();
        if (string != null && string.length() > 0) {
            PrintWriter out = new PrintWriter(file);
            try {
                out.print(string);
            } finally {
                IOUtil.closeResource(out);
            }
        }
        return file;
    }

    static {
        String name = null;
        try {
            Enumeration<NetworkInterface> networkInterfaces = NetworkInterface.getNetworkInterfaces();
            while (networkInterfaces.hasMoreElements()) {
                NetworkInterface networkInterface = networkInterfaces.nextElement();
                byte[] hardwareAddress = networkInterface.getHardwareAddress();
                if (hardwareAddress != null) {
                    name = networkInterface.getName();
                    break;
                }
            }
        } catch (Exception e) {
            LOGGER.severe(e);
        }
        interfaceName = name;
    }
}
