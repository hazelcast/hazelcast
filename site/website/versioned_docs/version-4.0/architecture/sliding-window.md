---
title: Sliding Window Aggregation
description: How Jet optimizes the computation of a sliding window
id: version-4.0-sliding-window
original_id: sliding-window
---

Computing a dynamic quantity like "gretchen83's current speed and
direction" requires us to aggregate data over some time period. This is
what makes the sliding window so important: it tracks the value of such
a quantity in real time.

Hazelcast Jet pays special attention to reducing the computational cost
of the sliding window. Let's present the challenges.

## Windowing in General

If we start from the general concept of windowing, this is the basic
picture: there is a stream of timestamped data items (_events_) and a
set of time intervals called _windows_. They can overlap, so any given
event may belong to more than one window. We want to apply an aggregate
function to the data of each window. Let's use counting as a simple
example:

![Time Windowing](assets/arch-sliding-window-1.svg)

To compute the output for a window, you must run every item in it
through the aggregation algorithm, and you must do this computation for
each window independently. Since you may receive the events out of the
order of their occurrence, you must retain all the events until the
window has "closed" (event time has advanced beyond the window end) and
then pass them to the aggregation function in the correct time order.

## Decompose into Accumulation and Finishing

This is where Jet takes the first optimization step: it decomposes the
aggregate function into an order-insensitive (commutative) part that
runs on every event and has state &mdash; the _accumulate_ function
&mdash; and a function that transforms the accumulated state after
seeing all the events &mdash; the _finish_ function:

![Accumulate and Finish](assets/arch-sliding-window-2.svg)

It turns out that many useful aggregate functions can be decomposed like
this and maintain only fixed-size state during the accumulation. Examples
are average, standard deviation, slope (linear regression coefficient),
min, max etc.

On the other hand, this doesn't constrain us in any way: we can still
implement order-sensitive aggregation by keeping the events in a list
and sorting it by timestamp in the end.

## The Sliding Window

Now let's focus on the sliding window. This is how our set of windows
looks like:

![Sliding Window Positions](assets/arch-sliding-window-3.svg)

All windows have the same size and they are arranged along the time axis
at a constant pitch, called the _sliding step_. Typically, to get smooth
sliding, we choose the sliding step to be 1% of the window size. Each
event thus ends up in a hundred windows. That's a lot of computation.

## Break into Frames

To avoid this repeated work, Jet breaks down the windows into _frames_
whose size is exactly the sliding step and combines the frames into full
windows:

![Sliding Window Frames](assets/arch-sliding-window-4.svg)

Now we need another primitive: the _combine_ function. It takes two
state objects and combines them into a single object. The ability to
combine two states means that our accumulating function must be
associative: however we group the events into frames, after combining
the result must be the same.

It turns out that we're lucky again: almost all accumulating functions
that are commutative are also associative so we still keep the benefit
of fixed-size state in each frame.

Let's look at a more realistic picture, showing many small frames being
combined into much larger windows:

![Combining Frames](assets/arch-sliding-window-5.svg)

Even though the cost of combining frames is fixed regardless of how much
data is coming in, we still have to perform 100 combining operations for
each window (assuming a 1% sliding step). Jet adds yet another function
that allows us to reduce this to just two operations for each window:
_deduct_:

![Deduct and Combine](assets/arch-sliding-window-6.svg)

The _deduct_ function reverses the effect of a _combine_ and brings the
state back to where it would without a particular frame. As the window
slides on, we can simply deduct the trailing frame and combine the
leading one.

_Deduct_ is supported by less aggregate functions than _accumulate_ and
_combine_. For example, you can't use it for _min_ or _max_ aggregation.
It is an optional component in Jet's aggregation process.

## Local Accumulation, Global Combining

So far we haven't considered the distributed nature of computation in
Hazelcast Jet. If you have a distributed data source like Kafka, every
Jet node consumes a slice of the complete stream. Jet performs the
accumulation step locally where the data came in and then sends the
frames to a single member where they are combined along two dimensions:

![Global Combining](assets/arch-sliding-window-7.svg)

To summarize, this would be the "naïve windowing" picture:

1. Send all the data to a single node
2. Put every event into all the windows it belongs to
3. Keep all the events stored until the window is complete
4. Apply the aggregate function for each window separately

And this is what Hazelcast Jet does for the sliding window:

1. Process events locally, on the node where you received them
2. Don't store the events, apply aggregation right away
3. Aggregate just once, in the single frame where the event
   belongs
4. Send the aggregated partial results to a single node, for final
   combining.
