# 3. Claims, bundles, and promises

These three concepts describe a request at different levels.

## Claim

A `Claim` says:

> Consume this quantity from this resource pool during this interval.

Example:

```text
pool:     workshop-a/machines
interval: [10:00, 11:00)
quantity: 1
```

A claim is a requirement. By itself it is not an accepted reservation.

## Bundle

A `Bundle` groups claims that must be accepted atomically.

Our workshop test needs:

```text
Claim 1: 1 machine       [10:00, 11:00)
Claim 2: 1 operator      [10:00, 11:00)
Claim 3: 1 inspection    [10:00, 11:00)
```

The bundle is accepted only if every claim fits. If inspection has no capacity, PromiseDB does not reserve the machine or operator.

Claims in one bundle may use different intervals. This allows an application to provide an already materialized workflow without asking PromiseDB to plan it.

## Choice

A `Choice` is a non-empty ordered list of alternative bundles. `HoldOneOf` tries them in order and holds the first complete bundle that fits. For example, an application may prefer workshop A but accept workshop B:

```text
alternative 0: machine + operator in workshop A
alternative 1: machine + operator in workshop B
```

PromiseDB does not combine alternatives or reserve part of a rejected one. If no alternative fits, the outcome reports each alternative index with that bundle's complete availability conflicts.

## Relative bundle and first-slot search

A `RelativeClaim` describes a claim around a candidate anchor rather than at fixed timestamps. For a candidate start of `10:00`, offsets `-15 minutes` and `+45 minutes` materialize to `[09:45, 10:45)`. Negative offsets are allowed; the start offset must still be earlier than the end offset. A `RelativeBundle` groups one or more such claims.

`find_first_slot` tries candidate anchors from an earliest bound through a latest bound at a positive fixed step and returns the first complete materialized bundle that fits. It is advisory: another mutation may consume that capacity afterward.

`hold_first_slot` performs search and reservation as one serialized transition. There is no intermediate state where PromiseDB reports a slot and then races another hold for it. If no candidate fits, the outcome reports how many candidates were attempted rather than retaining potentially large conflict details for every window.

## Promise

A `Promise` is created after a bundle is successfully held.

It records:

- a stable `PromiseId`;
- the accepted bundle;
- lifecycle state;
- local version;
- creation and update sequences.

The distinction is:

```text
Claim           requirement for one pool
Bundle          atomic collection of requirements
RelativeBundle  workflow positioned at candidate anchors
Choice          ordered alternative bundles
Promise         accepted bundle with identity and lifecycle
```

The application decides how a business request becomes a bundle. PromiseDB decides whether that bundle can be accepted.

Next: [Hold, commit, release, and expiration](03-hold-commit-release.md).
