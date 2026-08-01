# 1. The mental model

PromiseDB answers one question:

> Can these resources be committed during these time intervals without exceeding future capacity or breaking existing commitments?

Imagine an industrial workshop. Performing one test requires:

- one machine;
- one operator;
- one inspection slot.

It is not useful to reserve only the machine if no operator is available. The three requirements must be accepted together or rejected together.

PromiseDB models this with a small set of concepts:

```text
Resource pool  something with finite capacity over time
Claim          one resource requirement over one interval
Bundle         claims that succeed or fail together
Promise        an accepted bundle and its lifecycle
Engine         the authority that accepts or rejects changes
```

The engine does not understand what a machine or operator means. It only understands identifiers, integer quantities, and UTC timestamps.

## A small example

Suppose the workshop has:

```text
machines:   capacity 2
operators:  capacity 3
inspection: capacity 1
```

A request needs, from 10:00 to 11:00:

```text
1 machine
1 operator
1 inspection slot
```

PromiseDB checks all three requirements against existing promises. It either accepts the complete bundle or applies nothing.

## What PromiseDB does not do

PromiseDB does not decide:

- which test the workshop should run;
- which employee should operate a machine;
- whether one plan is more profitable than another;
- how a business request becomes resource requirements.

The application or control plane performs that translation. PromiseDB authoritatively protects finite future capacity.

Next: [Resources and capacity](01-resources-and-capacity.md).
