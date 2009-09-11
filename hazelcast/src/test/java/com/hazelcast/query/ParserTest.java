package com.hazelcast.query;

import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import static junit.framework.Assert.*;

public class ParserTest {
	Parser parser = new Parser();
	@Test
	public void parseEmpty(){
		List l = parser.toPrefix("");
		assertEquals(Arrays.asList(), l);
	}
	@Test
	public void parseAEqB(){
		String s = "a = b";
		List list = parser.toPrefix(s);
		assertEquals(Arrays.asList("a","b","="), list);
	}
	@Test 
	public void parseAeqBandXgrtY(){
		assertTrue(parser.hasHigherPrecedence("=", "AND"));
		assertFalse(parser.hasHigherPrecedence("=", ">"));

		List list = parser.toPrefix("a = b AND x > y");
		assertEquals(Arrays.asList("a","b","=","x","y",">","AND"), list);
	}
	@Test
	public void parseAeqBandOpenBsmlCorDgtEclose(){
		String s = "A = B AND ( B < C OR D > E )";
		List list = parser.toPrefix(s);
		assertEquals(Arrays.asList("A","B","=","B","C","<","D","E",">","OR","AND"),list);
	}
	@Test
	public void testComplexStatement(){
		String s = "age > 5 AND ( ( ( active = true ) AND ( age = 23 ) ) OR age > 40 ) AND ( salary > 10 ) OR age = 10";
		List list = parser.toPrefix(s);
		System.out.println(s);
		System.out.println(list);
		assertEquals(Arrays.asList("age","5",">","active","true","=","age","23","=","AND","age","40",">","OR","AND","salary","10",">","AND","age","10","=","OR"), list);
	}
	@Test
	public void testTwoInnerParanthesis(){
		String s = "a and b AND ( ( ( a > c AND b > d ) OR ( x = y ) ) ) OR t > u";
		List list = parser.toPrefix(s);
		System.out.println(s);
		System.out.println(list);
		assertEquals(Arrays.asList("a","b","and","a","c",">","b","d",">","AND","x","y","=","OR","AND","t","u",">","OR"), list);
	}
	
	@Test
	public void split1(){
		List tokens = parser.split("a and b");
		assertEquals(Arrays.asList("a","and","b"), tokens);
	}
	@Test
	public void split2(){
		List tokens = parser.split("(a and b)");
		assertEquals(Arrays.asList("(","a","and","b",")"), tokens);
	}
	@Test
	public void split3(){
		List tokens = parser.split("((a and b))");
		assertEquals(Arrays.asList("(","(","a","and","b",")",")"), tokens);
	}
	@Test
	public void split4(){
		List tokens = parser.split("a and b AND(((a>c AND b> d) OR (x = y )) ) OR t>u");
		assertEquals(Arrays.asList("a","and","b","AND","(","(","(","a",">","c","AND","b",">","d",")","OR","(","x","=","y",")",")",")","OR","t",">","u"), tokens);
	}
	
	@Test
	public void split5(){
		List tokens = parser.split("a and b AND(((a>=c AND b> d) OR (x <> y )) ) OR t>u");
		assertEquals(Arrays.asList("a","and","b","AND","(","(","(","a",">=","c","AND","b",">","d",")","OR","(","x","<>","y",")",")",")","OR","t",">","u"), tokens);
	}
	
	@Test
	public void testComplexStatementWithGreaterAndEqueals(){
		String s = "age >= 5 AND ((( active = true ) AND (age = 23 )) OR age > 40) AND( salary>10 ) OR age=10";
		List list = parser.toPrefix(s);
		System.out.println(s);
		System.out.println(list);
		assertEquals(Arrays.asList("age","5",">=","active","true","=","age","23","=","AND","age","40",">","OR","AND","salary","10",">","AND","age","10","=","OR"), list);
	}
}
