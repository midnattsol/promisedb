# PromiseDB

PromiseDB is a transactional commitment processor for finite future capacity.

Applications model capacity as resource pools and request atomic bundles of time-bounded claims. PromiseDB accepts a bundle only when every claim can be satisfied without exceeding the capacity of any pool during any part of its interval.

A successful hold reserves all claims in the bundle as one promise. That promise can then be committed, released, replaced, or allowed to expire.

## Core model

- A **resource pool** provides a finite quantity measured in an application-defined unit.
- A **claim** requests a quantity from one pool during a half-open interval `[start, end)`.
- A **bundle** is a non-empty set of claims accepted or rejected atomically.
- A **promise** owns an accepted bundle and moves through a versioned lifecycle.

Held and committed promises consume capacity only during their claim intervals. Released and expired promises do not consume capacity.

PromiseDB does not decide which resources an application needs. Applications define their own pools and compose the claims that must be committed together.
