# 5. Slack and deficits

Slack answers:

> How much capacity remains available at this time?

```text
slack = capacity - active usage
```

Example:

```text
capacity: 10
usage:     7
slack:     3
```

A new claim needing quantity 3 fits exactly. A claim needing 4 does not.

## Slack changes over time

Suppose capacity is 10 and one promise consumes 4 from 10:00 to 11:00:

```text
before 10:00        slack 10
[10:00, 11:00)      slack 6
from 11:00          slack 10
```

`SlackTimeline` stores points where the value changes:

```text
10:00 → 6
11:00 → 10
```

Each point applies until the next point. The timeline is a reconstructible index: capacity curves and active promises remain the authoritative information.

## Deficit

A forced capacity reduction may make usage greater than physical capacity:

```text
capacity: 5
usage:    8
slack:   -3
```

The positive deficit is:

```text
deficit = 3
```

PromiseDB accepts this physical reality in forced mode and reports:

- the affected interval;
- deficit quantity;
- overlapping promise IDs.

It does not choose which promise to cancel.

New holds cannot worsen a deficit. An atomic replace may be accepted when it improves existing negative slack, even if it does not completely resolve it.

Next: [Commands, events, and idempotency](05-commands-events-idempotency.md).
