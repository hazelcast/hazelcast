---
title: 012 - Improved Resilience of Fault-Tolerant Streaming Jobs
description: Make fault-tolerant streaming jobs capable of surviving fixable errors.
---

*Since*: 4.3

## Goal

Since streaming jobs with processing guarantees can't be re-run, like
their non-fault-tolerant counterparts (or batch jobs), having them fail,
can result in significant loss (in the form of irrecoverable
computational state). We want them to be as resilient as possible and
only ever fail due to the most severe of causes.

Right now any failure, like exceptions in user code, null values in
input and the likes, will stop the execution of a job. Moreover, not
only does execution stop, but snapshots of the job are also deleted, so
even if the root problem can be fixed, there is no way to recover or
resume the failed job.

We want to improve on this by only suspending jobs with such failures
and preserving their snapshots.

## Breaking-change

It might be argued that suspending a job on failure, instead of letting
it fail completely is a breaking change, as far as behaviour is
concerned.

One might also argue that it's broken behaviour which just took a long
time to fix.

Anyways, to be safe, we might preserve the current behaviour as default
and make the suggested changes optional. Maybe as a new element in
`JobConfig`, called `suspend_on_failure`, disabled unless otherwise
specified.

## Notifying the client

When we suspend a job due to a failure, we need to notify the client that
submitted it and give enough information to facilitate the repair of the
underlying root cause.

We will add a `String getSuspensionCause()` method, which will return
`Requested by user` if suspended due to a user request, or it will
return the exception with stack trace as a string if the job was
suspended due to an error.

If the job is not suspended, the method will throw an exception.

## When to use

Do we always want to suspend jobs when there is a failure? It doesn't
make sense to do it for failures that can't be remedied, but which are
those? Hard to tell, hard to exhaust all the possibilities.

Since the suspend feature will be optional and will need an explicit
setting, it's probably ok to do it for any failure (not just some
whitelisted ones). There is nothing to lose anyways. If the failure
can't be remedied, then the user can just cancel the job. They will be
no worse off than if it had failed automatically.

## Processing guarantees

It only makes sense to enable this feature for jobs with processing
guarantees. Only such jobs have mutable state. For jobs without
processing guarantees, the pipeline definition and the job config are
the only parts we can identify as state, and those are immutable. Batch
jobs also fall into the category of immutable state jobs.

However, nothing is to be gained from restricting the cases when this
behaviour can be set, so we will not do so for now.

## Enterprise synergy

Once implemented, this feature will integrate well with existing
enterprise functionality. When Jet suspends a job due to a failure,
enterprise users will be able to _export the snapshot_, fix the problem
(alter the DAG or the input data) and resubmit the job (via the _job
upgrade_ feature).

## Failure snapshot

Ideally, when an error happens which will be handled by suspending the
job, we would prefer to make all processors take a snapshot right then
so that we can later resume execution from the most current state. But
this, unfortunately doesn't seem possible.

Snapshots can be taken only when all processors are functioning properly
(due to the nature of how distributed snapshots happen).

But, even some slightly obsolete snapshot should be better than losing
the whole computation, so I guess this is a weakness we can live with.

## In-processor error handling

This functionality should be a solution of last resort, meaning that all
errors that can be handled without user intervention should be handled
automatically. For example sources and sinks losing connection to
external systems should attempt to reconnect internally, back off after
a certain number of tries, in general have their internal strategy for
dealing with problems as much as possible.
