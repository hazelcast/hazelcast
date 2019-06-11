package com.hazelcast.internal.util;

import com.hazelcast.config.SymmetricEncryptionConfig;
import com.hazelcast.internal.util.CipherHelper.SymmetricCipherBuilder;
import com.hazelcast.nio.Connection;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.util.RootCauseMatcher;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.security.NoSuchAlgorithmException;

import static com.hazelcast.internal.util.CipherHelper.initBouncySecurityProvider;
import static org.mockito.Mockito.mock;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class CipherHelperTest extends HazelcastTestSupport {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private SymmetricEncryptionConfig invalidConfiguration;
    private Connection mockedConnection;

    @Before
    public void setUp() {
        invalidConfiguration = new SymmetricEncryptionConfig()
                .setEnabled(true)
                .setAlgorithm("invalidAlgorithm");

        mockedConnection = mock(Connection.class);
    }

    @Test
    public void testConstructor() {
        assertUtilityConstructor(CipherHelper.class);
    }

    @Test
    public void testInitBouncySecurityProvider() {
        try {
            System.setProperty("hazelcast.security.bouncy.enabled", "true");

            initBouncySecurityProvider();
        } finally {
            System.clearProperty("hazelcast.security.bouncy.enabled");
        }
    }

    @Test
    public void testCreateCipher_withInvalidConfiguration() {
        SymmetricCipherBuilder builder = new SymmetricCipherBuilder(invalidConfiguration, null);

        expectedException.expect(new RootCauseMatcher(NoSuchAlgorithmException.class));
        builder.create(true);
    }

}
