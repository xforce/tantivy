// # DateTime field example
//
// This example shows how the DateTime field can be used

use tantivy::collector::TopDocs;
use tantivy::query::QueryParser;
use tantivy::schema::{DateTimeOptions, Schema, Value, INDEXED, STORED, STRING};
use tantivy::{DateTimePrecision, Index};

fn main() -> tantivy::Result<()> {
    // # Defining the schema
    let mut schema_builder = Schema::builder();
    let opts = DateTimeOptions::from(INDEXED)
        .set_stored()
        .set_precision(tantivy::DateTimePrecision::Seconds);
    let occurred_at = schema_builder.add_datetime_field("occurred_at", opts);
    let event_type = schema_builder.add_text_field("event_type", STRING | STORED);
    let schema = schema_builder.build();

    // # Indexing documents
    let index = Index::create_in_ram(schema.clone());

    let mut index_writer = index.writer(50_000_000)?;
    let doc = schema.parse_document(
        r#"{
        "occurred_at": "2022-06-22T12:53:50.53Z",
        "event_type": "click"
    }"#,
    )?;
    index_writer.add_document(doc)?;
    let doc = schema.parse_document(
        r#"{
        "occurred_at": "2022-06-22T13:00:00.20Z",
        "event_type": "double-click"
    }"#,
    )?;
    index_writer.add_document(doc)?;
    index_writer.commit()?;

    let reader = index.reader()?;
    let searcher = reader.searcher();

    // # Default fields: event_type
    let query_parser = QueryParser::for_index(&index, vec![event_type]);
    {
        let query = query_parser.parse_query("event_type:click")?;
        let count_docs = searcher.search(&*query, &TopDocs::with_limit(5))?;
        assert_eq!(count_docs.len(), 1);
    }
    {
        let query = query_parser.parse_query("occurred_at:\"2022-06-22T13:00:00.20Z\"")?;
        let count_docs = searcher.search(&*query, &TopDocs::with_limit(4))?;
        assert_eq!(count_docs.len(), 1);
        for (_score, doc_address) in count_docs {
            let retrieved_doc = searcher.doc(doc_address)?;
            assert!(matches!(retrieved_doc.get_first(occurred_at),
                Some(Value::DateTime(dt)) if dt.get_precision() == DateTimePrecision::Seconds));
            assert_eq!(
                schema.to_json(&retrieved_doc),
                r#"{"event_type":["double-click"],"occurred_at":["2022-06-22T13:00:00Z"]}"#
            );
        }
    }

    Ok(())
}