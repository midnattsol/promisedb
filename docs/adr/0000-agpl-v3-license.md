# ADR-0006: GNU AGPL v3 licensing

- Status: Accepted
- Date: 2026-07-29

## Context

PromiseDB is intended to be open source while requiring operators of modified network-accessible versions to make corresponding source available under the license terms.

## Decision

License the repository under GNU Affero General Public License version 3. The canonical license text is stored in `LICENSE`; Cargo metadata declares the SPDX identifier configured by the package.

## Consequences

- Users may run, study, modify, and redistribute PromiseDB under AGPL terms.
- Modified network services are subject to AGPL's remote-network source obligations.
- Some organizations may avoid AGPL software.
- Future proprietary dual licensing would require appropriate rights over external contributions.

## Alternatives considered

- Apache-2.0: rejected because it permits closed modified hosted services.
- MIT: rejected for the same reason and because it has no comparable explicit patent terms.
- Source-available licenses: rejected because they are not open-source licenses.
