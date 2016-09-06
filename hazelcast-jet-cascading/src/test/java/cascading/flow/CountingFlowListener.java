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

import cascading.stats.CascadingStats;

/**
 *
 */
public class CountingFlowListener implements FlowListener
  {
  public int started = 0;
  public int stopping = 0;
  public int completed = 0;
  public int successful = 0;
  public int failed = 0;
  public int stopped = 0;
  public int skipped = 0;
  public int thrown = 0;

  @Override
  public void onStarting( Flow flow )
    {
    started++;
    }

  @Override
  public void onStopping( Flow flow )
    {
    stopping++;
    }

  @Override
  public void onCompleted( Flow flow )
    {
    completed++;

    CascadingStats.Status status = flow.getFlowStats().getStatus();

    switch( status )
      {
      case PENDING:
        break;
      case SKIPPED:
        skipped++;
        break;
      case STARTED:
        break;
      case SUBMITTED:
        break;
      case RUNNING:
        break;
      case SUCCESSFUL:
        successful++;
        break;
      case STOPPED:
        stopped++;
        break;
      case FAILED:
        failed++;
        break;
      }
    }

  @Override
  public boolean onThrowable( Flow flow, Throwable throwable )
    {
    thrown++;
    return false;
    }
  }
