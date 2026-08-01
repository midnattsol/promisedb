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
Claim    requirement for one pool
Bundle   atomic collection of requirements
Promise  accepted bundle with identity and lifecycle
```

The application decides how a business request becomes a bundle. PromiseDB decides whether that bundle can be accepted.

Next: [Hold, commit, release, and expiration](03-hold-commit-release.md).
