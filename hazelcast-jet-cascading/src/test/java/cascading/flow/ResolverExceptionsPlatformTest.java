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

package cascading.flow;

import cascading.PlatformTestCase;
import cascading.flow.planner.PlannerException;
import cascading.operation.Identity;
import cascading.operation.aggregator.Count;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.FieldsResolverException;
import org.junit.Test;

/**
 * This test helps maintain consistent error messages across resolver failures.
 * <p/>
 * add new resolver usecases to the test suite.
 */
public class ResolverExceptionsPlatformTest extends PlatformTestCase
  {
  public ResolverExceptionsPlatformTest()
    {
    }

  private void verify( Tap source, Tap sink, Pipe pipe )
    {
    try
      {
      getPlatform().getFlowConnector().connect( source, sink, pipe );
      fail( "no exception thrown" );
      }
    catch( Exception exception )
      {
      assertTrue( exception instanceof PlannerException );
      assertTrue( exception.getCause().getCause() instanceof FieldsResolverException );
      }
    }

  @Test
  public void testSchemeResolver() throws Exception
    {
    Fields sourceFields = new Fields( "first", "second" );
    Tap source = getPlatform().getTabDelimitedFile( sourceFields, "input/path", SinkMode.KEEP );

    Fields sinkFields = new Fields( "third", "fourth" );
    Tap sink = getPlatform().getTabDelimitedFile( sinkFields, "output/path", SinkMode.REPLACE );

    Pipe pipe = new Pipe( "test" );

    verify( source, sink, pipe );
    }

  @Test
  public void testEachArgResolver() throws Exception
    {
    Fields sourceFields = new Fields( "first", "second" );
    Tap source = getPlatform().getTabDelimitedFile( sourceFields, "input/path", SinkMode.KEEP );

    Fields sinkFields = new Fields( "third", "fourth" );
    Tap sink = getPlatform().getTabDelimitedFile( sinkFields, "output/path", SinkMode.REPLACE );

    Pipe pipe = new Pipe( "test" );
    pipe = new Each( pipe, new Fields( "third" ), new Identity() );

    verify( source, sink, pipe );
    }

  @Test
  public void testEachOutResolver() throws Exception
    {
    Fields sourceFields = new Fields( "first", "second" );
    Tap source = getPlatform().getTabDelimitedFile( sourceFields, "input/path", SinkMode.KEEP );

    Fields sinkFields = new Fields( "third", "fourth" );
    Tap sink = getPlatform().getTabDelimitedFile( sinkFields, "output/path", SinkMode.REPLACE );

    Pipe pipe = new Pipe( "test" );
    pipe = new Each( pipe, new Fields( "first" ), new Identity( new Fields( "none" ) ), new Fields( "third" ) );

    verify( source, sink, pipe );
    }

  @Test
  public void testGroupByResolver() throws Exception
    {
    Fields sourceFields = new Fields( "first", "second" );
    Tap source = getPlatform().getTabDelimitedFile( sourceFields, "input/path", SinkMode.KEEP );

    Fields sinkFields = new Fields( "third", "fourth" );
    Tap sink = getPlatform().getTabDelimitedFile( sinkFields, "output/path", SinkMode.REPLACE );

    Pipe pipe = new Pipe( "test" );
    pipe = new GroupBy( pipe, new Fields( "third" ) );

    verify( source, sink, pipe );
    }

  @Test
  public void testGroupBySortResolver() throws Exception
    {
    Fields sourceFields = new Fields( "first", "second" );
    Tap source = getPlatform().getTabDelimitedFile( sourceFields, "input/path", SinkMode.KEEP );

    Fields sinkFields = new Fields( "third", "fourth" );
    Tap sink = getPlatform().getTabDelimitedFile( sinkFields, "output/path", SinkMode.REPLACE );

    Pipe pipe = new Pipe( "test" );
    pipe = new GroupBy( pipe, new Fields( "first" ), new Fields( "third" ) );

    verify( source, sink, pipe );
    }

  @Test
  public void testEveryArgResolver() throws Exception
    {
    Fields sourceFields = new Fields( "first", "second" );
    Tap source = getPlatform().getTabDelimitedFile( sourceFields, "input/path", SinkMode.KEEP );

    Fields sinkFields = new Fields( "third", "fourth" );
    Tap sink = getPlatform().getTabDelimitedFile( sinkFields, "output/path", SinkMode.REPLACE );

    Pipe pipe = new Pipe( "test" );
    pipe = new GroupBy( pipe, new Fields( "first" ) );

    pipe = new Every( pipe, new Fields( "third" ), new Count() );

    verify( source, sink, pipe );
    }

  @Test
  public void testEveryOutResolver() throws Exception
    {
    Fields sourceFields = new Fields( "first", "second" );
    Tap source = getPlatform().getTabDelimitedFile( sourceFields, "input/path", SinkMode.KEEP );

    Fields sinkFields = new Fields( "third", "fourth" );
    Tap sink = getPlatform().getTabDelimitedFile( sinkFields, "output/path", SinkMode.REPLACE );

    Pipe pipe = new Pipe( "test" );
    pipe = new GroupBy( pipe, new Fields( "first" ) );

    pipe = new Every( pipe, new Fields( "second" ), new Count(), new Fields( "third" ) );

    verify( source, sink, pipe );
    }

  }
