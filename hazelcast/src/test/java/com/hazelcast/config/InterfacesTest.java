package com.hazelcast.config;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

public class InterfacesTest {

	final String interfaceA = "127.0.0.1";
	final String interfaceB = "127.0.0.2";
	final String interfaceC = "127.0.0.3";

	@Test
	public void testIsEnabled() {
		Interfaces interfaces = new Interfaces();
		assertFalse(interfaces.isEnabled());
	}

	@Test
	public void testSetEnabled() {
		Interfaces interfaces = new Interfaces();
		interfaces.setEnabled(true);
		assertTrue(interfaces.isEnabled());
	}

	@Test
	public void testAddInterface() {
		Interfaces interfaces = new Interfaces();
		interfaces.addInterface(interfaceA);
		assertTrue(interfaces.getInterfaces().contains(interfaceA));
	}

	@Test
	public void testClear() {
		Interfaces interfaces = new Interfaces();
		
		interfaces.addInterface(interfaceA);
		interfaces.addInterface(interfaceB);
		interfaces.addInterface(interfaceC);

		assertTrue(interfaces.getInterfaces().size() == 3);
		interfaces.clear();
		assertTrue(interfaces.getInterfaces().size() == 0);
	}

	@Test
	public void testGetInterfaceList() {
		Interfaces interfaces = new Interfaces();
		assertNotNull(interfaces.getInterfaces());
	}

	@Test
	public void testSetInterfaceList() {
		
		List<String> interfaceList = new ArrayList<String>();
		interfaceList.add(interfaceA);
		interfaceList.add(interfaceB);
		interfaceList.add(interfaceC);

		Interfaces interfaces = new Interfaces();
		interfaces.setInterfaces(interfaceList);

		assertTrue(interfaces.getInterfaces().contains(interfaceA));
		assertTrue(interfaces.getInterfaces().contains(interfaceB));
		assertTrue(interfaces.getInterfaces().contains(interfaceC));
	}

	@Test
	public void shouldNotContainDuplicateInterfaces() {
		Interfaces interfaces = new Interfaces();
		
		interfaces.addInterface(interfaceA);
		assertTrue(interfaces.getInterfaces().size() == 1);
		
		interfaces.addInterface(interfaceA);
		assertTrue(interfaces.getInterfaces().size() == 1);
	}
	
	@Test(expected = UnsupportedOperationException.class)
	public void shouldNotBeModifiable() {
		Interfaces interfaces = new Interfaces();
		interfaces.addInterface(interfaceA);
		interfaces.getInterfaces().clear();
	}
	
}
