#![cfg(feature = "typed")]

use tagdata::{
    BytesCodec, CodecError, DB, I64Codec, KeyCodec, StringCodec, StringPairCodec, TypedBucket,
    TypedCodec, U64Codec,
};

mod common;

#[test]
fn typed_and_raw_access_coexist() -> Result<(), Box<dyn std::error::Error>> {
    let file = common::RandomFile::new();
    let db = DB::open(&file)?;
    let tx = db.write_tx()?;
    let raw = tx.create_bucket("numbers")?;
    let typed = TypedBucket::<u64, String, _>::new(raw, TypedCodec::new(U64Codec, StringCodec));

    assert_eq!(typed.put(&2, &"two".into())?, None);
    assert_eq!(typed.put(&1, &"one".into())?, None);
    assert_eq!(typed.get(&2)?, Some("two".into()));
    assert_eq!(
        typed.raw().get_kv(1_u64.to_be_bytes())?.unwrap().value(),
        b"one"
    );
    assert_eq!(typed.entries()?, vec![(1, "one".into()), (2, "two".into())]);
    assert_eq!(typed.range_inclusive(&1, &1)?, vec![(1, "one".into())]);
    assert_eq!(typed.delete(&2)?, "two");
    tx.commit()?;
    Ok(())
}

#[test]
fn provided_key_codecs_preserve_expected_order() -> Result<(), CodecError> {
    assert_order(&U64Codec, &[0_u64, 1, 255, 256, u64::MAX])?;
    assert_order(&I64Codec, &[i64::MIN, -2, -1, 0, 1, i64::MAX])?;
    assert_order(
        &StringCodec,
        &["".into(), "a".into(), "aa".into(), "b".into()],
    )?;
    assert_order(
        &StringPairCodec,
        &[
            ("a".into(), "".into()),
            ("a".into(), "a".into()),
            ("a\0".into(), "a".into()),
            ("b".into(), "".into()),
        ],
    )?;
    assert_order(&BytesCodec, &[vec![], vec![0], vec![0, 1], vec![1]])
}

fn assert_order<K: Clone + std::fmt::Debug + Eq, C: KeyCodec<K>>(
    codec: &C,
    values: &[K],
) -> Result<(), CodecError> {
    assert!(C::ORDER_PRESERVING);
    let encoded = values
        .iter()
        .map(|value| codec.encode_key(value))
        .collect::<Result<Vec<_>, _>>()?;
    assert!(encoded.windows(2).all(|pair| pair[0] < pair[1]));
    let decoded = encoded
        .iter()
        .map(|bytes| codec.decode_key(bytes))
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(decoded, values);
    Ok(())
}

#[cfg(feature = "serde-codec")]
#[test]
fn messagepack_values_are_opt_in() -> Result<(), Box<dyn std::error::Error>> {
    use serde::{Deserialize, Serialize};
    use tagdata::MessagePackCodec;

    #[derive(Debug, Eq, PartialEq, Serialize, Deserialize)]
    struct User {
        name: String,
        level: u32,
    }

    let file = common::RandomFile::new();
    let db = DB::open(&file)?;
    let tx = db.write_tx()?;
    let raw = tx.create_bucket("users")?;
    let typed = TypedBucket::<u64, User, _>::new(raw, TypedCodec::new(U64Codec, MessagePackCodec));
    typed.put(
        &7,
        &User {
            name: "Ada".into(),
            level: 3,
        },
    )?;
    assert_eq!(
        typed.get(&7)?,
        Some(User {
            name: "Ada".into(),
            level: 3,
        })
    );
    Ok(())
}
