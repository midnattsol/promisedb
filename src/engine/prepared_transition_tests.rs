use super::*;
use crate::command::{CommandOperation, CommandResult};
use crate::domain::{CapacitySegment, RelativeClaim};
use crate::storage::transition_codec::{
    decode_transition, encode_transition, encode_transition_into,
};

fn pool(byte: u8) -> ResourcePoolId {
    ResourcePoolId::from_bytes([byte; 16])
}
fn promise(byte: u8) -> PromiseId {
    PromiseId::from_bytes([byte; 16])
}
fn curve(capacity: u64) -> CapacityCurve {
    CapacityCurve::from_sorted(vec![CapacitySegment::new(
        Interval::new(0, 1_000).unwrap(),
        capacity,
    )])
    .unwrap()
}
fn bundle(pool_id: ResourcePoolId, start: i64, end: i64, quantity: u64) -> Bundle {
    Bundle::new(vec![
        Claim::new(pool_id, Interval::new(start, end).unwrap(), quantity).unwrap(),
    ])
    .unwrap()
}
fn command(key: &str, operation: CommandOperation) -> Command {
    Command::new(
        ClientId::new("prepared-tests"),
        IdempotencyKey::new(key),
        operation,
    )
}
fn create(key: &str, pool_id: ResourcePoolId, capacity: u64) -> Command {
    command(
        key,
        CommandOperation::CreateResourcePool {
            resource_pool_id: pool_id,
            display_name: "pool".into(),
            unit: Unit::new("units".into(), 1).unwrap(),
            capacity_curve: curve(capacity),
        },
    )
}
fn prepare_and_publish(engine: &mut Engine, command: Command, now: Timestamp) -> DurableTransition {
    let prepared = engine.prepare_batch(vec![(command, now)]).unwrap();
    let durable = prepared.durable_items()[0].transition().clone();
    engine.can_publish(&prepared).unwrap();
    engine.publish_batch(prepared);
    durable
}

#[test]
fn batch_uses_one_state_clone_and_executes_sequentially() {
    ENGINE_STATE_CLONES.with(|count| count.set(0));
    let mut engine = Engine::new();
    let hold = command(
        "hold-batch",
        CommandOperation::Hold {
            promise_id: promise(1),
            bundle: bundle(pool(1), 10, 20, 2),
            expires_at: 100,
        },
    );
    let prepared = engine
        .prepare_batch(vec![
            (create("create-batch", pool(1), 10), 0),
            (hold.clone(), 1),
            (hold, 999),
            (
                command("hold-batch", CommandOperation::ProcessExpirations),
                999,
            ),
        ])
        .unwrap();

    assert_eq!(ENGINE_STATE_CLONES.with(std::cell::Cell::get), 1);
    assert_eq!(prepared.responses.len(), 4);
    assert_eq!(prepared.durable_items().len(), 2);
    assert_eq!(prepared.durable_items()[0].timestamp(), 0);
    assert_eq!(prepared.durable_items()[1].timestamp(), 1);
    assert_eq!(
        prepared.durable_items()[1]
            .transition()
            .resource_pools()
            .len(),
        1
    );
    assert_eq!(prepared.durable_items()[1].transition().promises().len(), 1);
    assert_eq!(prepared.responses[3], Err(DomainError::IdempotencyConflict));
    engine.can_publish(&prepared).unwrap();
    let responses = engine.publish_batch(prepared);
    assert_eq!(responses.len(), 4);
    assert!(engine.resource_pool(pool(1)).is_some());
    assert!(engine.promise(promise(1)).is_some());
    assert_eq!(engine.publication_revision, PublicationRevision(2));
}

#[test]
fn preparation_isolated_publish_exact_and_stale_rejected() {
    let mut engine = Engine::new();
    let first = engine
        .prepare_batch(vec![(create("create-a", pool(1), 10), 1)])
        .unwrap();
    assert_eq!(engine.state, EngineState::empty());
    assert_eq!(engine.publication_revision, PublicationRevision(0));

    let stale = engine
        .prepare_batch(vec![(create("create-b", pool(2), 10), 1)])
        .unwrap();
    let expected = first.candidate.as_ref().unwrap().clone();
    engine.can_publish(&first).unwrap();
    engine.publish_batch(first);
    assert_eq!(engine.state, expected);
    assert_eq!(engine.publication_revision, PublicationRevision(1));
    assert_eq!(
        engine.can_publish(&stale),
        Err(PreparationError::StaleRevision {
            expected: PublicationRevision(0),
            actual: PublicationRevision(1),
        })
    );
}

#[test]
fn revision_overflow_is_preflighted_without_mutation() {
    let mut engine = Engine::new();
    let original = create("original", pool(1), 10);
    assert!(engine.apply(original.clone(), 0).is_ok());
    assert_eq!(engine.publication_revision, PublicationRevision(1));
    engine.publication_revision = PublicationRevision(u128::MAX);
    let state = engine.state.clone();

    assert!(engine.apply(original, 999).is_ok());
    assert_eq!(
        engine.apply(
            command("original", CommandOperation::ProcessExpirations),
            999,
        ),
        Err(DomainError::IdempotencyConflict)
    );
    let prepared_retry = engine
        .prepare_batch(vec![(create("original", pool(1), 10), 999)])
        .unwrap();
    assert!(prepared_retry.durable_items().is_empty());
    assert!(prepared_retry.responses[0].is_ok());
    let prepared_conflict = engine
        .prepare_batch(vec![(
            command("original", CommandOperation::ProcessExpirations),
            999,
        )])
        .unwrap();
    assert!(prepared_conflict.durable_items().is_empty());
    assert_eq!(
        prepared_conflict.responses[0],
        Err(DomainError::IdempotencyConflict)
    );
    assert_eq!(engine.publication_revision, PublicationRevision(u128::MAX));
    assert_eq!(engine.state, state);

    assert_eq!(
        engine.apply(create("direct-overflow", pool(2), 10), 1),
        Err(DomainError::PublicationRevisionOverflow)
    );
    assert!(matches!(
        engine.prepare_batch(vec![(create("prepare-overflow", pool(2), 10), 1)]),
        Err(PreparationError::RevisionOverflow)
    ));
    assert_eq!(engine.publication_revision, PublicationRevision(u128::MAX));
    assert_eq!(engine.state, state);

    let mut source = Engine::new();
    let transition = prepare_and_publish(&mut source, create("install", pool(2), 10), 0);
    assert_eq!(
        engine.install_transition(transition),
        Err(InstallError::PublicationRevision)
    );
    assert_eq!(engine.publication_revision, PublicationRevision(u128::MAX));
    assert_eq!(engine.state, state);
}

#[test]
fn cached_retries_and_conflicts_have_no_transition_and_errors_are_cached() {
    let mut engine = Engine::new();
    let missing = command(
        "missing",
        CommandOperation::Hold {
            promise_id: promise(1),
            bundle: bundle(pool(9), 10, 20, 1),
            expires_at: 100,
        },
    );
    let prepared = engine.prepare_batch(vec![(missing.clone(), 1)]).unwrap();
    assert_eq!(
        prepared.responses[0],
        Err(DomainError::ResourcePoolNotFound)
    );
    assert_eq!(prepared.durable_items().len(), 1);
    engine.can_publish(&prepared).unwrap();
    engine.publish_batch(prepared);

    let retry = engine.prepare_batch(vec![(missing, 999)]).unwrap();
    assert_eq!(retry.responses[0], Err(DomainError::ResourcePoolNotFound));
    assert!(retry.durable_items().is_empty());
    let conflict = engine
        .prepare_batch(vec![(
            command("missing", CommandOperation::ProcessExpirations),
            999,
        )])
        .unwrap();
    assert_eq!(conflict.responses[0], Err(DomainError::IdempotencyConflict));
    assert!(conflict.durable_items().is_empty());
}

#[test]
fn unavailable_and_expiration_then_rejection_are_durable() {
    let mut engine = Engine::new();
    prepare_and_publish(&mut engine, create("create", pool(1), 1), 0);
    let unavailable = command(
        "unavailable",
        CommandOperation::Hold {
            promise_id: promise(1),
            bundle: bundle(pool(1), 10, 20, 2),
            expires_at: 100,
        },
    );
    let prepared = engine
        .prepare_batch(vec![(unavailable.clone(), 1)])
        .unwrap();
    assert!(matches!(
        prepared.responses[0],
        Ok(CommandResult::HoldCompleted(
            HoldOutcome::Unavailable { .. }
        ))
    ));
    assert_eq!(prepared.durable_items().len(), 1);
    engine.can_publish(&prepared).unwrap();
    engine.publish_batch(prepared);
    assert!(
        engine
            .prepare_batch(vec![(unavailable, 2)])
            .unwrap()
            .durable_items()
            .is_empty()
    );

    prepare_and_publish(
        &mut engine,
        command(
            "held",
            CommandOperation::Hold {
                promise_id: promise(2),
                bundle: bundle(pool(1), 30, 40, 1),
                expires_at: 10,
            },
        ),
        1,
    );
    let rejected = engine
        .prepare_batch(vec![(create("duplicate", pool(1), 1), 10)])
        .unwrap();
    assert_eq!(
        rejected.responses[0],
        Err(DomainError::ResourcePoolAlreadyExists)
    );
    let transition = rejected.durable_items()[0].transition();
    assert!(
        transition
            .events()
            .iter()
            .any(|event| event.kind() == EventKind::HoldExpired)
    );
    assert!(
        transition
            .promises()
            .iter()
            .any(|value| value.id() == promise(2) && value.state() == PromiseState::Expired)
    );
    engine.can_publish(&rejected).unwrap();
    engine.publish_batch(rejected);
    assert_eq!(
        engine.promise(promise(2)).unwrap().state(),
        PromiseState::Expired
    );
}

#[allow(clippy::vec_init_then_push)] // Each preparation depends on the prior publication.
fn complex_transitions() -> (Engine, Vec<DurableTransition>) {
    let mut engine = Engine::new();
    let mut transitions = Vec::new();
    transitions.push(prepare_and_publish(
        &mut engine,
        create("create", pool(1), 10),
        0,
    ));
    transitions.push(prepare_and_publish(
        &mut engine,
        command(
            "choice",
            CommandOperation::HoldOneOf {
                promise_id: promise(1),
                choice: Choice::new(vec![
                    bundle(pool(1), 10, 20, 20),
                    bundle(pool(1), 10, 20, 2),
                ])
                .unwrap(),
                expires_at: 100,
            },
        ),
        1,
    ));
    transitions.push(prepare_and_publish(
        &mut engine,
        command(
            "slot",
            CommandOperation::HoldFirstSlot {
                promise_id: promise(2),
                relative_bundle: RelativeBundle::new(vec![
                    RelativeClaim::new(pool(1), 0, 5, 2).unwrap(),
                ])
                .unwrap(),
                earliest_start: 30,
                latest_start: 50,
                step: 10,
                expires_at: 100,
            },
        ),
        1,
    ));
    transitions.push(prepare_and_publish(
        &mut engine,
        command(
            "replace",
            CommandOperation::Replace {
                promise_id: promise(1),
                expected_version: Version::new(1).unwrap(),
                new_bundle: bundle(pool(1), 40, 50, 3),
                new_state: ReplacementState::Committed,
            },
        ),
        2,
    ));
    transitions.push(prepare_and_publish(
        &mut engine,
        command(
            "force",
            CommandOperation::ReviseCapacity {
                resource_pool_id: pool(1),
                capacity_curve: curve(1),
                mode: CapacityRevisionMode::Force,
            },
        ),
        3,
    ));
    (engine, transitions)
}

#[test]
fn transition_codec_is_deterministic_exact_and_rejects_corruption() {
    let (_, transitions) = complex_transitions();
    for transition in transitions {
        let first = encode_transition(&transition).unwrap();
        let second = encode_transition(&transition).unwrap();
        assert_eq!(first, second);
        let mut appended = vec![0xaa, 0xbb];
        encode_transition_into(&transition, &mut appended).unwrap();
        assert_eq!(&appended[..2], &[0xaa, 0xbb]);
        assert_eq!(&appended[2..], first);
        let decoded = decode_transition(&first).unwrap();
        assert_eq!(decoded, transition);
        assert_eq!(encode_transition(&decoded).unwrap(), first);

        let mut invalid_response_tag = first.clone();
        let command_length = u32::from_le_bytes(first[1..5].try_into().unwrap()) as usize;
        let mut response_offset = 5 + command_length;
        for _ in 0..2 {
            let length = u32::from_le_bytes(
                first[response_offset..response_offset + 4]
                    .try_into()
                    .unwrap(),
            ) as usize;
            response_offset += 4 + length;
        }
        response_offset += 32;
        invalid_response_tag[response_offset] = 255;
        assert_eq!(
            decode_transition(&invalid_response_tag),
            Err(crate::storage::StorageError::InvalidTag {
                kind: "command response",
                tag: 255,
            })
        );

        let mut trailing = first.clone();
        trailing.push(0);
        assert!(decode_transition(&trailing).is_err());
        assert!(decode_transition(&first[..first.len() - 1]).is_err());
    }
    assert_eq!(
        decode_transition(&[2]),
        Err(crate::storage::StorageError::UnsupportedTransitionVersion(
            2
        ))
    );
}

#[test]
fn decoded_effect_install_matches_authoritative_state_and_rebuilt_indexes() {
    let (source, transitions) = complex_transitions();
    let mut recovered = Engine::new();
    for transition in transitions {
        let decoded = decode_transition(&encode_transition(&transition).unwrap()).unwrap();
        recovered.install_transition(decoded).unwrap();
        assert!(recovered.state.slack_timelines.is_empty());
    }
    recovered.rebuild_slack_timelines().unwrap();
    assert_eq!(recovered.state, source.state);
    assert_eq!(recovered.publication_revision, source.publication_revision);
}

#[test]
fn install_rejects_hash_corruption_and_duplicate_first_seen_identity() {
    let (_, mut transitions) = complex_transitions();
    let transition = transitions.remove(0);
    let mut corrupt = transition.clone();
    corrupt.command_hash = CommandHash::from_bytes([0xff; 32]);
    assert_eq!(
        Engine::new().install_transition(corrupt),
        Err(InstallError::CommandHash)
    );

    let mut recovered = Engine::new();
    recovered.install_transition(transition.clone()).unwrap();
    assert_eq!(
        recovered.install_transition(transition),
        Err(InstallError::DuplicateIdempotencyIdentity)
    );
}

#[test]
fn recovery_installs_effects_even_when_command_admission_would_now_reject() {
    let mut high_capacity = Engine::new();
    prepare_and_publish(&mut high_capacity, create("high", pool(1), 10), 0);
    let held = prepare_and_publish(
        &mut high_capacity,
        command(
            "hold",
            CommandOperation::Hold {
                promise_id: promise(1),
                bundle: bundle(pool(1), 10, 20, 5),
                expires_at: 100,
            },
        ),
        1,
    );

    let mut recovered = Engine::new();
    let low = prepare_and_publish(&mut Engine::new(), create("low", pool(1), 1), 0);
    recovered.install_transition(low).unwrap();
    recovered.rebuild_slack_timelines().unwrap();
    let replay_result = recovered.apply(held.command().clone(), 1);
    assert!(matches!(
        replay_result,
        Ok(CommandResult::HoldCompleted(
            HoldOutcome::Unavailable { .. }
        ))
    ));

    let mut recovered = Engine::new();
    let low = prepare_and_publish(&mut Engine::new(), create("low", pool(1), 1), 0);
    recovered.install_transition(low).unwrap();
    recovered
        .install_transition(decode_transition(&encode_transition(&held).unwrap()).unwrap())
        .unwrap();
    assert_eq!(
        recovered.promise(promise(1)).unwrap().state(),
        PromiseState::Held { expires_at: 100 }
    );
}
