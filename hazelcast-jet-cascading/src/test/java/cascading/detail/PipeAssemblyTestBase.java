/*
 * Copyright (c) 2007-2016 Concurrent, Inc. All Rights Reserved.
 *
 * Project and contact information: http://www.cascading.org/
 *
 * This file is part of the Cascading project.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cascading.detail;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.lang.reflect.InvocationTargetException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Scanner;

import cascading.PlatformTestCase;
import cascading.flow.Flow;
import cascading.pipe.Pipe;
import cascading.platform.PlatformSuite;
import cascading.platform.TestPlatform;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntryIterator;
import cascading.util.Util;
import data.InputData;
import junit.framework.TestSuite;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

//@RunWith(SuiteMethod.class)
@PlatformSuite
public abstract class PipeAssemblyTestBase extends PlatformTestCase
  {
  private static final Logger LOG = LoggerFactory.getLogger( PipeAssemblyTestBase.class );

  static Fields[] OP_ARGS_FIELDS = new Fields[]{new Fields( -1 ), new Fields( 0 ), Fields.ALL};
  static Fields[] OP_DECL_FIELDS = new Fields[]{new Fields( "field" ), Fields.UNKNOWN, Fields.VALUES, Fields.ARGS};
  static Fields[] OP_SELECT_FIELDS = new Fields[]{new Fields( -1 ), new Fields( "field" ), Fields.RESULTS, Fields.ALL,
                                                  Fields.REPLACE, Fields.SWAP};
  static final String OP_VALUE = "value";

  static Fields[] LHS_ARGS_FIELDS = new Fields[]{new Fields( -1 ), Fields.ALL};
  static Fields[] LHS_DECL_FIELDS = new Fields[]{new Fields( "field" ), Fields.UNKNOWN, Fields.ARGS};
  static Fields[] LHS_SELECT_FIELDS = new Fields[]{Fields.RESULTS, Fields.ALL};
  static final String LHS_VALUE = "value";

  static Fields[] RHS_ARGS_FIELDS = new Fields[]{new Fields( -1 ), new Fields( 0 ), Fields.ALL};
  static Fields[] RHS_DECL_FIELDS = new Fields[]{new Fields( "field2" ), Fields.UNKNOWN, Fields.VALUES, Fields.ARGS};
  static Fields[] RHS_SELECT_FIELDS = new Fields[]{new Fields( -1 ), new Fields( "field2" ), new Fields( "field" ),
                                                   Fields.RESULTS, Fields.ALL};
  static final String RHS_VALUE = "value2";

  private String key;

  public static void makeSuites( TestPlatform testPlatform, Properties properties, Map<String, Pipe> pipes, TestSuite suite, Class type ) throws IllegalAccessException, InvocationTargetException, InstantiationException, IOException, ClassNotFoundException
    {
    for( String name : pipes.keySet() )
      {
      if( isUNDEFINED( properties, name ) )
        {
        LOG.debug( "skipping: {}", name );
        continue;
        }

//        TestPlatform testPlatform = makeInstance( getPlatformClass( type.getClassLoader() ) );

      // test platform dependencies not installed, so skip
//        if( testPlatform == null )
//          continue;

      String platformName = testPlatform.getName();

      String displayName = String.format( "%s[%s]", name, platformName );

      PlatformTestCase platformTest = (PlatformTestCase) type.getConstructors()[ 0 ].newInstance( properties, displayName, name, pipes.get( name ) );

      platformTest.installPlatform( testPlatform );

      suite.addTest( platformTest );
      }
    }

  public static Properties loadProperties( String type ) throws IOException
    {
    String path = PipeAssemblyTestBase.class.getPackage().getName().replace( ".", "/" ) + "/" + type;
    InputStream input = PipeAssemblyTestBase.class.getClassLoader().getResourceAsStream( path );

    Properties properties = new Properties();

    properties.load( input );

    return properties;
    }

  static String runOnly( Properties properties )
    {
    return properties.getProperty( "run.only" );
    }

  private static boolean isUNDEFINED( Properties properties, String name )
    {
    return !properties.containsKey( name + ".ERROR" ) && !properties.containsKey( name + ".tuple" );
    }

  public static Map<String, Pipe> buildOpPipes( String prefix, Pipe pipe, AssemblyFactory assemblyFactory, Fields[] args_fields, Fields[] decl_fields, Fields[] select_fields, String functionValue, String runOnly )
    {
    Map<String, Pipe> pipes = new LinkedHashMap<String, Pipe>();

    for( int arg = 0; arg < args_fields.length; arg++ )
      {
      Fields argFields = args_fields[ arg ];

      for( int decl = 0; decl < decl_fields.length; decl++ )
        {
        Fields declFields = decl_fields[ decl ];

        for( int select = 0; select < select_fields.length; select++ )
          {
          Fields selectFields = select_fields[ select ];

          String name;
          if( prefix != null )
            name = prefix + "." + Util.join( Fields.fields( argFields, declFields, selectFields ), "_" );
          else
            name = Util.join( Fields.fields( argFields, declFields, selectFields ), "_" );

          if( runOnly != null && !runOnly.equalsIgnoreCase( name ) )
            continue;

          pipes.put( name, assemblyFactory.createAssembly( pipe, argFields, declFields, functionValue, selectFields ) );
          }
        }
      }

    return pipes;
    }

  Properties properties;
  private Pipe pipe;
  Fields argFields;
  Fields declFields;
  Fields selectFields;
  Tuple resultTuple;
  int resultLength;

  public PipeAssemblyTestBase( Properties properties, String displayName, String key, Pipe pipe )
    {
    setName( displayName );
    this.key = key;
    this.properties = properties;
    this.pipe = pipe;
    this.resultTuple = getResultTuple();
    this.resultLength = getResultLength();
    }

  Tuple getResultTuple()
    {
    return parse( (String) properties.get( key + ".tuple" ) );
    }

  static Tuple parse( String string )
    {
    if( string == null || string.length() == 0 )
      return null;

    string = string.replaceAll( "^ *\\[*", "" );
    string = string.replaceAll( "\\]* *$", "" );

    Scanner scanner = new Scanner( new StringReader( string ) );
    scanner.useDelimiter( "(' *, *')|(^ *')|(' *$)" );

    Tuple result = new Tuple();

    while( scanner.hasNext() )
      {
      if( scanner.hasNextInt() )
        result.add( scanner.nextInt() );
      else if( scanner.hasNextDouble() )
        result.add( scanner.nextDouble() );
      else
        result.add( scanner.next() );
      }

    scanner.close();

    return result;
    }

  boolean isWriteDOT()
    {
    return properties.containsKey( key + ".writedot" );
    }

  int getResultLength()
    {
    return Integer.parseInt( properties.getProperty( key + ".length", properties.getProperty( "default.length" ) ) );
    }

  boolean isError()
    {
    return properties.containsKey( key + ".ERROR" );
    }

  @org.junit.Test
  public void runTest() throws Exception
    {
    copyFromLocal( InputData.inputFileNums20 );
    Tap source = getPlatform().getTextFile( InputData.inputFileNums20 );
    Tap sink = getPlatform().getTextFile( getOutputPath( key ), SinkMode.REPLACE );

    Flow flow = null;

    try
      {
      flow = getPlatform().getFlowConnector().connect( source, sink, pipe );

      if( isWriteDOT() )
        flow.writeDOT( getName() + ".dot" ); // use display name

      flow.complete();

      if( isError() )
        fail( "did not throw asserted error" );
      }
    catch( Exception exception )
      {
      if( isError() )
        return;
      else
        throw exception;
      }

    if( resultLength != -1 )
      validateLength( flow, resultLength );

    if( resultTuple != null )
      {
        TupleEntryIterator iterator = flow.openSink();
        boolean found = false;
        while (iterator.hasNext())
          {
          Object result = iterator.next().getObject( 1 );
          if (result.equals(resultTuple.toString()))
            {
            found = true;
            break;
            }
          }
        if (!found)
          fail(" expected tuple " + resultTuple + " not found in result.");
    } else
      fail( "no result assertion made for:" + getName() + " with result: " + resultTuple );
    }
  }
