package com.hazelcast.jca;

import org.jboss.jca.embedded.Embedded;
import org.jboss.jca.embedded.EmbeddedFactory;

import java.io.File;
import java.net.URL;

import javax.naming.InitialContext;
import javax.naming.NamingException;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import static org.junit.Assert.*;

public class AbsDeploymentTest {

	/** Embedded */
	private static Embedded embeddedJCAContainer;

	/** JNDI prefix */
	private static final String JNDI_PREFIX = "java:/";
	private static final String JNDI_HAZELCAST_CF = "HazelcastCF";

	private InitialContext context;

	private URL rarArchive;

	protected ConnectionFactoryImpl connectionFactory;

	@After 
	public void tearDown() throws Throwable {
		embeddedJCAContainer.undeploy(rarArchive);

		if (context != null) {
			try {
				context.close();
			} catch (NamingException ne) {
				// Ignore
			}
		}
	}
	
	@Before
	public void setUp() throws Throwable {
		String archiveVersion = getClass().getPackage().getImplementationVersion();
		File f = new File("./target/hazelcast-ra-"+archiveVersion+".rar");
		rarArchive = f.toURI().toURL();

		embeddedJCAContainer.deploy(rarArchive);

		context = new InitialContext();
		
		Object o = context.lookup(JNDI_PREFIX + JNDI_HAZELCAST_CF);

		assertNotNull(o);
		assertTrue(o instanceof ConnectionFactoryImpl);

		connectionFactory = (ConnectionFactoryImpl) o;
	}

	@BeforeClass
	public static void beforeClass() throws Throwable {
		// Create an embedded JCA instance
		embeddedJCAContainer = EmbeddedFactory.create();

		// Startup
		embeddedJCAContainer.startup();
	}

	@AfterClass
	public static void afterClass() throws Throwable {
		// Shutdown
//throws IOException:		embeddedJCAContainer.shutdown(); 
	}
}
