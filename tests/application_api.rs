#![cfg(feature = "typed")]

use inspace::{
    CodecError, CollectionDef, DB, OpenPolicy, StringCodec, TransactionError, TypedCodec, U64Codec,
};

mod common;

const USERS: CollectionDef<u64, String, TypedCodec<U64Codec, StringCodec>> =
    CollectionDef::new("users", TypedCodec::new(U64Codec, StringCodec)).schema("example.users", 1);

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
