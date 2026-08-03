#![cfg(feature = "typed")]

use inspace::{
    CodecError, CollectionDef, DB, OpenPolicy, StringCodec, TransactionError, TypedCodec, U64Codec,
    WriteOptions,
};
use std::{
    collections::BTreeMap,
    time::{Duration, SystemTime},
};

use rand::{Rng, SeedableRng, rngs::StdRng};

mod common;

const USERS: CollectionDef<u64, String, TypedCodec<U64Codec, StringCodec>> =
    CollectionDef::new("users", TypedCodec::new(U64Codec, StringCodec)).schema("example.users", 1);

const COUNTERS: CollectionDef<u64, u64, TypedCodec<U64Codec, U64Codec>> =
    CollectionDef::new("counters", TypedCodec::new(U64Codec, U64Codec));

const AUDIT: CollectionDef<u64, String, TypedCodec<U64Codec, StringCodec>> = CollectionDef::nested(
    &["tenant", "application"],
    "audit",
    TypedCodec::new(U64Codec, StringCodec),
)
.schema("example.audit", 1);

#[test]
fn reusable_collection_supports_typed_crud_and_bounded_scans()
-> Result<(), Box<dyn std::error::Error>> {
    let file = common::RandomFile::new();
    let db = DB::open(&file)?;

    db.write(|tx| {
        let users = tx.collection_mut(USERS)?;
        assert_eq!(users.insert(&2, &"Grace".into())?, None);
        assert_eq!(users.insert(&1, &"Ada".into())?, None);
        assert_eq!(users.insert(&3, &"Linus".into())?, None);
        assert_eq!(users.insert(&2, &"Hopper".into())?, Some("Grace".into()));
        Ok::<_, TransactionError<CodecError>>(())
    })?;

    db.read(|tx| {
        let users = tx.collection(USERS)?;
        assert_eq!(users.get(&1)?, Some("Ada".into()));
        assert!(users.contains_key(&2)?);
        assert_eq!(users.len(), 3);
        assert_eq!(users.multi_get([3, 9])?, vec![Some("Linus".into()), None]);
        assert_eq!(
            users
                .iter()
                .take_records(2)
                .collect::<Result<Vec<_>, _>>()?,
            vec![(1, "Ada".into()), (2, "Hopper".into())]
        );
        assert_eq!(
            users.range(&2, &4)?.collect::<Result<Vec<_>, _>>()?,
            vec![(2, "Hopper".into()), (3, "Linus".into())]
        );
        assert_eq!(
            users.iter_rev().collect::<Result<Vec<_>, _>>()?,
            vec![(3, "Linus".into()), (2, "Hopper".into()), (1, "Ada".into())]
        );
        let first_page = users.page_after(None, 2)?;
        assert_eq!(first_page.items.len(), 2);
        let second_page = users.page_after(first_page.next.as_ref(), 2)?;
        assert_eq!(second_page.items, vec![(3, "Linus".into())]);
        assert!(second_page.next.is_none());
        assert_eq!(users.keys().collect::<Result<Vec<_>, _>>()?, vec![1, 2, 3]);
        assert_eq!(
            users.values().collect::<Result<Vec<_>, _>>()?,
            vec!["Ada", "Hopper", "Linus"]
        );
        assert_eq!(
            users
                .range_bounds((std::ops::Bound::Excluded(&1), std::ops::Bound::Included(&2)))?
                .collect::<Result<Vec<_>, _>>()?,
            vec![(2, "Hopper".into())]
        );
        assert_eq!(
            users.range_rev(&1, &3)?.collect::<Result<Vec<_>, _>>()?,
            vec![(2, "Hopper".into()), (1, "Ada".into())]
        );
        Ok::<_, TransactionError<CodecError>>(())
    })?;

    db.write(|tx| {
        let users = tx.collection_mut(USERS)?;
        assert_eq!(users.remove(&2)?, Some("Hopper".into()));
        assert_eq!(users.remove(&2)?, None);
        assert_eq!(users.clear()?, 2);
        assert!(users.as_read().is_empty());
        Ok::<_, TransactionError<CodecError>>(())
    })?;
    Ok(())
}

#[test]
fn collection_schema_and_open_policy_are_enforced() -> Result<(), Box<dyn std::error::Error>> {
    let file = common::RandomFile::new();
    let db = DB::open(&file)?;
    db.write(|tx| {
        tx.collection_mut(USERS)?;
        Ok::<_, TransactionError<CodecError>>(())
    })?;

    let changed = USERS.schema("example.users", 2);
    let mismatch = db.read(|tx| {
        tx.collection(changed)?;
        Ok::<_, TransactionError<CodecError>>(())
    });
    assert!(matches!(
        mismatch,
        Err(TransactionError::Application(CodecError::SchemaMismatch {
            collection: "users",
            expected_version: 2,
            ..
        }))
    ));

    let create_only = USERS.open_policy(OpenPolicy::Create);
    let exists = db.write(|tx| {
        tx.collection_mut(create_only)?;
        Ok::<_, TransactionError<CodecError>>(())
    });
    assert!(matches!(
        exists,
        Err(TransactionError::Storage(inspace::Error::BucketExists))
    ));
    Ok(())
}

#[test]
fn entries_conflicts_numeric_updates_and_ttl_are_typed() -> Result<(), Box<dyn std::error::Error>> {
    let file = common::RandomFile::new();
    let db = DB::open(&file)?;

    db.write(|tx| {
        let users = tx.collection_mut(USERS)?;
        assert_eq!(users.entry(1)?.or_insert("Ada".into())?, "Ada");
        assert_eq!(
            users
                .entry(1)?
                .and_modify(|name| name.push_str(" Lovelace"))?
                .or_insert("ignored".into())?,
            "Ada Lovelace"
        );
        let conflict = users.compare_exchange(&1, Some(&"wrong".into()), &"new".into())?;
        assert!(!conflict.applied);
        assert_eq!(conflict.current, Some("Ada Lovelace".into()));

        let counters = tx.collection_mut(COUNTERS)?;
        assert_eq!(counters.fetch_add(&7, 3)?, 0);
        assert_eq!(counters.fetch_add(&7, 4)?, 3);

        users.insert_with_options(
            &9,
            &"short lived".into(),
            WriteOptions::expires_at(SystemTime::now() - Duration::from_secs(1)),
        )?;
        assert_eq!(users.get(&9)?, None);
        Ok::<_, TransactionError<CodecError>>(())
    })?;

    db.write(|tx| {
        let users = tx.collection_mut(USERS)?;
        users.insert_many([(2, "B".into()), (3, "C".into()), (4, "D".into())])?;
        assert_eq!(users.delete_range(&2, &4)?, 2);
        assert_eq!(users.pop_last()?, Some((4, "D".into())));
        assert_eq!(users.pop_first()?, Some((1, "Ada Lovelace".into())));
        Ok::<_, TransactionError<CodecError>>(())
    })?;
    Ok(())
}

#[test]
fn heterogeneous_batches_apply_inside_one_transaction() -> Result<(), Box<dyn std::error::Error>> {
    let file = common::RandomFile::new();
    let db = DB::open(&file)?;
    db.write(|tx| {
        let mut batch = tx.batch();
        batch
            .insert(USERS, &1, &"Ada".into())?
            .insert(COUNTERS, &1, &41)?
            .insert(COUNTERS, &2, &7)?
            .remove(COUNTERS, &2)?;
        assert_eq!(batch.apply()?, 4);
        Ok::<_, TransactionError<CodecError>>(())
    })?;
    db.read(|tx| {
        assert_eq!(tx.collection(USERS)?.get(&1)?, Some("Ada".into()));
        assert_eq!(tx.collection(COUNTERS)?.get(&1)?, Some(41));
        assert_eq!(tx.collection(COUNTERS)?.get(&2)?, None);
        Ok::<_, TransactionError<CodecError>>(())
    })?;
    Ok(())
}

#[test]
fn typed_collection_matches_btree_map_for_random_mutations()
-> Result<(), Box<dyn std::error::Error>> {
    let file = common::RandomFile::new();
    let db = DB::open(&file)?;
    let mut model = BTreeMap::<u64, u64>::new();
    let mut random = StdRng::seed_from_u64(0x1a5_ace);

    db.write(|tx| {
        let collection = tx.collection_mut(COUNTERS)?;
        for _ in 0..500 {
            let key = random.gen_range(0..64);
            if random.gen_bool(0.65) {
                let value = random.r#gen();
                assert_eq!(collection.insert(&key, &value)?, model.insert(key, value));
            } else {
                assert_eq!(collection.remove(&key)?, model.remove(&key));
            }
        }
        let actual = collection.iter().collect::<Result<Vec<_>, _>>()?;
        assert_eq!(actual, model.clone().into_iter().collect::<Vec<_>>());
        assert_eq!(
            collection.iter_rev().collect::<Result<Vec<_>, _>>()?,
            model.into_iter().rev().collect::<Vec<_>>()
        );
        Ok::<_, TransactionError<CodecError>>(())
    })?;
    Ok(())
}

#[test]
fn collection_definitions_support_stable_nested_paths() -> Result<(), Box<dyn std::error::Error>> {
    let file = common::RandomFile::new();
    let db = DB::open(&file)?;
    db.write(|tx| {
        tx.collection_mut(AUDIT)?.insert(&1, &"created".into())?;
        Ok::<_, TransactionError<CodecError>>(())
    })?;
    db.read(|tx| {
        assert_eq!(tx.collection(AUDIT)?.get(&1)?, Some("created".into()));
        Ok::<_, TransactionError<CodecError>>(())
    })?;
    Ok(())
}

#[test]
fn ordered_bulk_load_validates_monotonic_input() -> Result<(), Box<dyn std::error::Error>> {
    let file = common::RandomFile::new();
    let db = DB::open(&file)?;
    db.write(|tx| {
        let counters = tx.collection_mut(COUNTERS)?;
        let ordered = counters.insert_ordered([(1, 10), (2, 20), (3, 30)])?;
        assert_eq!(ordered.inserted, 3);
        assert!(ordered.ordered);
        let fallback = counters.insert_ordered([(5, 50), (4, 40)])?;
        assert_eq!(fallback.inserted, 2);
        assert!(!fallback.ordered);
        assert_eq!(counters.get(&4)?, Some(40));
        Ok::<_, TransactionError<CodecError>>(())
    })?;
    Ok(())
}
