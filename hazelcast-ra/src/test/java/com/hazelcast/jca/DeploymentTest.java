package com.hazelcast.jca;

import org.jboss.jca.embedded.Embedded;
import org.jboss.jca.embedded.EmbeddedFactory;

import java.io.File;
import java.net.URL;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.*;

public class DeploymentTest {

	/** Embedded */
	private static Embedded embedded;

	/** JNDI prefix */
	private static final String JNDI_PREFIX = "java:/";

	/**
	 * 
	 * Simple test to verify deployment of myresourceadapter.rar
	 * 
	 * @throws Throwable
	 *             throwable exception
	 */

	@Test
	public void testDeployment() throws Throwable {

		File f = new File("./target/hazelcast-ra-2.2.1.rar");
		URL archive = f.toURI().toURL();

		Context context = null;

		try {

			embedded.deploy(archive);

			context = new InitialContext();
			Object o = context.lookup(JNDI_PREFIX + "HazelcastCF");

			assertNotNull(o);

		} finally {

			embedded.undeploy(archive);

			if (context != null) {
				try {
					context.close();
				} catch (NamingException ne) {
					// Ignore
				}
			}
		}
	}

	@BeforeClass
	public static void beforeClass() throws Throwable {
		// Create an embedded JCA instance
		embedded = EmbeddedFactory.create();

		// Startup
		embedded.startup();
	}

	@AfterClass
	public static void afterClass() throws Throwable {
		// Shutdown
		embedded.shutdown();
	}
}
