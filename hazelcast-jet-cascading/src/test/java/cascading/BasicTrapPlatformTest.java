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

package cascading;

import java.util.HashMap;
import java.util.Map;

import cascading.operation.aggregator.Count;
import cascading.operation.regex.RegexParser;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import org.junit.Test;

/**
 *
 */
public class BasicTrapPlatformTest extends PlatformTestCase
  {
  public BasicTrapPlatformTest()
    {
    }

  @Test
  public void testTrapNamesFail() throws Exception
    {
    Tap source = getPlatform().getTextFile( "foosource" );

    Pipe pipe = new Pipe( "test" );

    pipe = new Each( pipe, new Fields( "line" ), new RegexParser( new Fields( "ip" ), "^[^ ]*" ), new Fields( "ip" ) );

    // always fail
    pipe = new Each( pipe, new Fields( "ip" ), new TestFunction( new Fields( "test" ), null ), Fields.ALL );

    pipe = new GroupBy( pipe, new Fields( "ip" ) );
    pipe = new Every( pipe, new Count(), new Fields( "ip", "count" ) );

    Tap sink = getPlatform().getTextFile( "footap", SinkMode.REPLACE );
    Tap trap = getPlatform().getTextFile( "footrap", SinkMode.REPLACE );

    Map<String, Tap> sources = new HashMap<String, Tap>();
    Map<String, Tap> sinks = new HashMap<String, Tap>();
    Map<String, Tap> traps = new HashMap<String, Tap>();

    sources.put( "test", source );
    sinks.put( "test", sink );
    traps.put( "nada", trap );

    try
      {
      getPlatform().getFlowConnector().connect( "trap test", sources, sinks, traps, pipe );
      fail( "did not fail on missing pipe name" );
      }
    catch( Exception exception )
      {
      // tests passed
      }
    }

  @Test
  public void testTrapNamesPass() throws Exception
    {

    Tap source = getPlatform().getTextFile( "foosource" );

    Pipe pipe = new Pipe( "map" );

    pipe = new Each( pipe, new Fields( "line" ), new RegexParser( new Fields( "ip" ), "^[^ ]*" ), new Fields( "ip" ) );

    // always fail
    pipe = new Each( pipe, new Fields( "ip" ), new TestFunction( new Fields( "test" ), null ), Fields.ALL );

    pipe = new GroupBy( "reduce", pipe, new Fields( "ip" ) );
    pipe = new Every( pipe, new Count(), new Fields( "ip", "count" ) );

    Tap sink = getPlatform().getTextFile( "foosink" );
    Tap trap = getPlatform().getTextFile( "footrap" );

    Map<String, Tap> sources = new HashMap<String, Tap>();
    Map<String, Tap> sinks = new HashMap<String, Tap>();
    Map<String, Tap> traps = new HashMap<String, Tap>();

    sources.put( "map", source );
    sinks.put( "reduce", sink );
    traps.put( "map", trap );

    getPlatform().getFlowConnector().connect( "trap test", sources, sinks, traps, pipe );
    }

  @Test
  public void testTrapNamesPass2() throws Exception
    {
    Tap source = getPlatform().getTextFile( "foosource" );

    Pipe pipe = new Pipe( "map" );

    pipe = new Each( pipe, new Fields( "line" ), new RegexParser( new Fields( "ip" ), "^[^ ]*" ), new Fields( "ip" ) );

    pipe = new Pipe( "middle", pipe );
    pipe = new Each( pipe, new Fields( "ip" ), new TestFunction( new Fields( "test" ), null ), Fields.ALL );

    pipe = new GroupBy( "reduce", pipe, new Fields( "ip" ) );
    pipe = new Every( pipe, new Count(), new Fields( "ip", "count" ) );

    Tap sink = getPlatform().getTextFile( "foosink" );
    Tap trap = getPlatform().getTextFile( "footrap" );

    Map<String, Tap> sources = new HashMap<String, Tap>();
    Map<String, Tap> sinks = new HashMap<String, Tap>();
    Map<String, Tap> traps = new HashMap<String, Tap>();

    sources.put( "map", source );
    sinks.put( "reduce", sink );
    traps.put( "middle", trap );

    getPlatform().getFlowConnector().connect( "trap test", sources, sinks, traps, pipe );
    }

  @Test
  public void testTrapNamesPass3() throws Exception
    {
    Tap source = getPlatform().getTextFile( "foosource" );

    Pipe pipe = new Pipe( "test" );

    pipe = new Each( pipe, new Fields( "line" ), new RegexParser( new Fields( "ip" ), "^[^ ]*" ), new Fields( "ip" ) );

    pipe = new Each( pipe, new Fields( "ip" ), new TestFunction( new Fields( "test" ), null ), Fields.ALL );

    pipe = new GroupBy( pipe, new Fields( "ip" ) );
    pipe = new Pipe( "first", pipe );
    pipe = new Every( pipe, new Count(), new Fields( "ip", "count" ) );
    pipe = new Pipe( "second", pipe );
    pipe = new Every( pipe, new Count( new Fields( "count2" ) ), new Fields( "ip", "count", "count2" ) );

    Tap sink = getPlatform().getTextFile( "foosink" );
    Tap trap = getPlatform().getTextFile( "footrap" );

    Map<String, Tap> sources = new HashMap<String, Tap>();
    Map<String, Tap> sinks = new HashMap<String, Tap>();
    Map<String, Tap> traps = new HashMap<String, Tap>();

    sources.put( "test", source );
    sinks.put( "second", sink );
    traps.put( "first", trap );

    getPlatform().getFlowConnector().connect( "trap test", sources, sinks, traps, pipe );
    }
  }