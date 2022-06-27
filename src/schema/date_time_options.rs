use std::collections::{HashSet, VecDeque};
use std::ops::BitOr;
use std::sync::{Arc, Mutex};

use serde::{Deserialize, Serialize};
use time::format_description::well_known::{Iso8601, Rfc2822, Rfc3339};
use time::OffsetDateTime;

use super::Cardinality;
use crate::schema::flags::{FastFlag, IndexedFlag, SchemaFlagList, StoredFlag};
use crate::DateTimePrecision;

/// Defines how DateTime field should be handled by tantivy.
#[derive(Clone, Serialize, Deserialize, Derivative)]
#[derivative(Debug, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct DateTimeOptions {
    indexed: bool,
    // This boolean has no effect if the field is not marked as indexed true.
    fieldnorms: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    fast: Option<Cardinality>,
    stored: bool,

    // Accepted input format for parsing input as DateTime.
    #[serde(default = "default_input_formats")]
    input_formats: HashSet<DateTimeFormat>,

    // Internal storage precision, used to avoid storing
    // very large numbers when not needed. This optimizes compression.
    #[serde(default)]
    precision: DateTimePrecision,

    // Holds a handle to the inputs parsers function pointer.
    // This is lazy loaded and sorted based on most recently successful parser.
    //
    // We hold it in a mutex as we need interior mutability to keep efficiently
    // try the parsers. right now we put the last successful parser in front of a queue
    // so we can try it first when parsing next document. We could also sort based
    // on most successful parser function.
    // However, given the need for mutex in this hot path, we could avoid this dance?
    #[serde(skip)]
    #[derivative(Debug = "ignore")]
    #[derivative(PartialEq = "ignore")]
    parsers: Arc<Mutex<Option<DateTimeParsersHolder>>>,
}

impl Default for DateTimeOptions {
    fn default() -> Self {
        Self {
            indexed: true,
            fieldnorms: true,
            fast: None,
            stored: true,
            input_formats: default_input_formats(),
            precision: DateTimePrecision::default(),
            parsers: Arc::new(Mutex::new(None)),
        }
    }
}

impl DateTimeOptions {
    /// Returns true iff the value is stored.
    pub fn is_stored(&self) -> bool {
        self.stored
    }

    /// Returns true iff the value is indexed and therefore searchable.
    pub fn is_indexed(&self) -> bool {
        self.indexed
    }

    /// Returns true iff the field has fieldnorm.
    pub fn fieldnorms(&self) -> bool {
        self.fieldnorms && self.indexed
    }

    /// Returns true iff the value is a fast field and multivalue.
    pub fn is_multivalue_fast(&self) -> bool {
        if let Some(cardinality) = self.fast {
            cardinality == Cardinality::MultiValues
        } else {
            false
        }
    }

    /// Returns true iff the value is a fast field.
    pub fn is_fast(&self) -> bool {
        self.fast.is_some()
    }

    /// Set the field as stored.
    ///
    /// Only the fields that are set as *stored* are
    /// persisted into the Tantivy's store.
    #[must_use]
    pub fn set_stored(mut self) -> DateTimeOptions {
        self.stored = true;
        self
    }

    /// Set the field as indexed.
    ///
    /// Setting an integer as indexed will generate
    /// a posting list for each value taken by the integer.
    ///
    /// This is required for the field to be searchable.
    #[must_use]
    pub fn set_indexed(mut self) -> DateTimeOptions {
        self.indexed = true;
        self
    }

    /// Set the field with fieldnorm.
    ///
    /// Setting an integer as fieldnorm will generate
    /// the fieldnorm data for it.
    #[must_use]
    pub fn set_fieldnorm(mut self) -> DateTimeOptions {
        self.fieldnorms = true;
        self
    }

    /// Set the field as a single-valued fast field.
    ///
    /// Fast fields are designed for random access.
    /// Access time are similar to a random lookup in an array.
    /// If more than one value is associated to a fast field, only the last one is
    /// kept.
    #[must_use]
    pub fn set_fast(mut self, cardinality: Cardinality) -> DateTimeOptions {
        self.fast = Some(cardinality);
        self
    }

    /// Returns the cardinality of the fastfield.
    ///
    /// If the field has not been declared as a fastfield, then
    /// the method returns None.
    pub fn get_fastfield_cardinality(&self) -> Option<Cardinality> {
        self.fast
    }

    /// Sets the DateTime field acceptable input format.
    pub fn set_input_formats(mut self, formats: HashSet<DateTimeFormat>) -> DateTimeOptions {
        self.input_formats = formats;
        self
    }

    /// Returns the DateTime field acceptable input format.
    pub fn get_input_formats(&self) -> &HashSet<DateTimeFormat> {
        &self.input_formats
    }

    /// Sets the precision for this DateTime field.
    ///
    /// Internal storage precision: Used to avoid storing
    /// very large numbers when not needed. This optimizes compression.
    pub fn set_precision(mut self, precision: DateTimePrecision) -> DateTimeOptions {
        self.precision = precision;
        self
    }

    /// Returns the storage precision for this DateTime field.
    ///
    /// Internal storage precision: Used to avoid storing
    /// very large numbers when not needed. This optimizes compression.
    pub fn get_precision(&self) -> DateTimePrecision {
        self.precision
    }

    /// Returns a handle to the inputs parsers function pointer.
    /// The parsers handle is initialized if not already.
    fn get_parsers(&self) -> Arc<Mutex<Option<DateTimeParsersHolder>>> {
        let mut parser_guard = self.parsers.lock().unwrap();
        if parser_guard.is_none() {
            *parser_guard = Some(DateTimeParsersHolder::from(self.clone()));
        }
        self.parsers.clone()
    }

    /// Parse DateTime from string input using specified input formats.
    pub fn parse_string(&self, value: String) -> Result<OffsetDateTime, String> {
        let parsers = self.get_parsers();
        let mut parsers_guard = parsers.lock().unwrap();
        parsers_guard.as_mut().unwrap().parse_string(value)
    }

    /// Parses DateTime from number input using specified input formats
    pub fn parse_number(&self, value: i64) -> Result<OffsetDateTime, String> {
        let parsers = self.get_parsers();
        let mut parsers_guard = parsers.lock().unwrap();
        parsers_guard.as_mut().unwrap().parse_number(value)
    }
}

impl From<()> for DateTimeOptions {
    fn from(_: ()) -> DateTimeOptions {
        DateTimeOptions::default()
    }
}

impl From<FastFlag> for DateTimeOptions {
    fn from(_: FastFlag) -> Self {
        DateTimeOptions {
            indexed: false,
            fieldnorms: false,
            stored: false,
            fast: Some(Cardinality::SingleValue),
            ..Default::default()
        }
    }
}

impl From<StoredFlag> for DateTimeOptions {
    fn from(_: StoredFlag) -> Self {
        DateTimeOptions {
            indexed: false,
            fieldnorms: false,
            stored: true,
            fast: None,
            ..Default::default()
        }
    }
}

impl From<IndexedFlag> for DateTimeOptions {
    fn from(_: IndexedFlag) -> Self {
        DateTimeOptions {
            indexed: true,
            fieldnorms: true,
            stored: false,
            fast: None,
            ..Default::default()
        }
    }
}

impl<T: Into<DateTimeOptions>> BitOr<T> for DateTimeOptions {
    type Output = DateTimeOptions;

    fn bitor(self, other: T) -> DateTimeOptions {
        let other = other.into();
        DateTimeOptions {
            indexed: self.indexed | other.indexed,
            fieldnorms: self.fieldnorms | other.fieldnorms,
            stored: self.stored | other.stored,
            fast: self.fast.or(other.fast),
            // TODO: Is the default ok for the other field?
            ..Default::default()
        }
    }
}

impl<Head, Tail> From<SchemaFlagList<Head, Tail>> for DateTimeOptions
where
    Head: Clone,
    Tail: Clone,
    Self: BitOr<Output = Self> + From<Head> + From<Tail>,
{
    fn from(head_tail: SchemaFlagList<Head, Tail>) -> Self {
        Self::from(head_tail.head) | Self::from(head_tail.tail)
    }
}

// Parser function types
type StringDateTimeParser = Arc<dyn Fn(&str) -> Result<OffsetDateTime, String> + Sync + Send>;
type NumberDateTimeParser = Arc<dyn Fn(i64) -> Result<OffsetDateTime, String> + Sync + Send>;

/// A struct for holding datetime parsing functions.
#[derive(Clone)]
pub struct DateTimeParsersHolder {
    /// Functions for parsing string input as datetime.
    string_parsers: VecDeque<StringDateTimeParser>,
    /// Function for parsing number input as timestamp.
    number_parser: NumberDateTimeParser,
}

impl Default for DateTimeParsersHolder {
    fn default() -> Self {
        Self {
            string_parsers: VecDeque::new(),
            number_parser: make_timestamp_parser(DateTimePrecision::Milliseconds),
        }
    }
}

impl DateTimeParsersHolder {
    /// Parses string input.
    pub fn parse_string(&mut self, value: String) -> Result<OffsetDateTime, String> {
        for (index, parser) in self.string_parsers.iter().enumerate() {
            if let Ok(date_time) = parser(&value) {
                // Move successful parser in front of queue.
                self.string_parsers.swap(0, index);
                return Ok(date_time);
            }
        }
        Err("Could not parse a valid datetime using all specified parsers.".to_string())
    }

    /// Parses number input.
    pub fn parse_number(&self, value: i64) -> Result<OffsetDateTime, String> {
        (self.number_parser)(value)
    }
}

impl From<DateTimeOptions> for DateTimeParsersHolder {
    fn from(opts: DateTimeOptions) -> Self {
        let mut string_parsers: VecDeque<StringDateTimeParser> = VecDeque::new();
        let mut number_parser: NumberDateTimeParser =
            make_timestamp_parser(DateTimePrecision::Milliseconds);
        for input_format in opts.input_formats {
            match input_format {
                DateTimeFormat::RCF3339 => string_parsers.push_back(Arc::new(rfc3339_parser)),
                DateTimeFormat::RFC2822 => string_parsers.push_back(Arc::new(rfc2822_parser)),
                DateTimeFormat::ISO8601 => string_parsers.push_back(Arc::new(iso8601_parser)),
                DateTimeFormat::Strftime(str_format) => {
                    string_parsers.push_back(make_strftime_parser(str_format))
                }
                DateTimeFormat::Timestamp(precision) => {
                    number_parser = make_timestamp_parser(precision)
                }
            }
        }
        DateTimeParsersHolder {
            string_parsers,
            number_parser,
        }
    }
}

/// Parses datetime strings using RFC3339 formatting.
fn rfc3339_parser(value: &str) -> Result<OffsetDateTime, String> {
    OffsetDateTime::parse(value, &Rfc3339).map_err(|error| error.to_string())
}

/// Parses DateTime strings using RFC2822 formatting.
fn rfc2822_parser(value: &str) -> Result<OffsetDateTime, String> {
    OffsetDateTime::parse(value, &Rfc2822).map_err(|error| error.to_string())
}

/// Parses DateTime strings using default ISO8601 formatting.
/// Examples: 2010-11-21T09:55:06.000000000+02:00, 2010-11-12 9:55:06 +2:00
fn iso8601_parser(value: &str) -> Result<OffsetDateTime, String> {
    OffsetDateTime::parse(value, &Iso8601::DEFAULT).map_err(|error| error.to_string())
}

/// Configures and returns a function for parsing DateTime strings
/// using strftime formatting.
fn make_strftime_parser(format: String) -> StringDateTimeParser {
    Arc::new(move |value: &str| {
        // expect timezone
        let date_time = if format.contains("%z") {
            chrono::DateTime::parse_from_str(value, &format)
                .map_err(|error| error.to_string())
                .map(|date_time| date_time.naive_utc())?
        } else {
            chrono::NaiveDateTime::parse_from_str(value, &format)
                .map_err(|error| error.to_string())?
        };

        OffsetDateTime::from_unix_timestamp_nanos(date_time.timestamp_nanos() as i128)
            .map_err(|error| error.to_string())
    })
}

/// Configures and returns a function for interpreting numbers
/// as timestamp with a precision.
fn make_timestamp_parser(precision: DateTimePrecision) -> NumberDateTimeParser {
    Arc::new(move |value: i64| {
        let date_time = match precision {
            DateTimePrecision::Seconds => OffsetDateTime::from_unix_timestamp(value),
            DateTimePrecision::Milliseconds => {
                OffsetDateTime::from_unix_timestamp_nanos((value as i128) * 1_000_000)
            }
            DateTimePrecision::Microseconds => {
                OffsetDateTime::from_unix_timestamp_nanos((value as i128) * 1000)
            }
            DateTimePrecision::Nanoseconds => {
                OffsetDateTime::from_unix_timestamp_nanos(value as i128)
            }
        };
        date_time.map_err(|error| error.to_string())
    })
}

/// An enum specifying all supported DateTime parsing format.
#[derive(Clone, Debug, Eq, Derivative, Serialize, Deserialize)]
#[derivative(Hash, PartialEq)]
pub enum DateTimeFormat {
    /// The rcf3339 input format.
    RCF3339,
    /// The rcf2822 input format.
    RFC2822,
    /// The iso8601 input format.
    ISO8601,
    /// The srtftime input format.
    Strftime(String),
    /// The timestamp input format with precision.
    Timestamp(
        #[derivative(PartialEq = "ignore")]
        #[derivative(Hash = "ignore")]
        DateTimePrecision,
    ),
}

impl Default for DateTimeFormat {
    fn default() -> Self {
        DateTimeFormat::ISO8601
    }
}

fn default_input_formats() -> HashSet<DateTimeFormat> {
    let mut input_formats = HashSet::new();
    input_formats.insert(DateTimeFormat::ISO8601);
    input_formats.insert(DateTimeFormat::Timestamp(DateTimePrecision::default()));
    input_formats
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::sync::{Arc, Mutex};

    use time::macros::{date, time};

    use super::*;

    #[test]
    fn test_strftime_format_cannot_be_duplicated() {
        let mut formats = HashSet::new();
        formats.insert(DateTimeFormat::Strftime(
            "%a %b %d %H:%M:%S %z %Y".to_string(),
        ));
        formats.insert(DateTimeFormat::Strftime("%Y %m %d".to_string()));
        formats.insert(DateTimeFormat::Strftime(
            "%a %b %d %H:%M:%S %z %Y".to_string(),
        ));
        formats.insert(DateTimeFormat::Timestamp(DateTimePrecision::Microseconds));
        assert_eq!(formats.len(), 3);
    }

    #[test]
    fn test_only_one_timestamp_format_can_be_added() {
        let mut formats = HashSet::new();
        formats.insert(DateTimeFormat::Timestamp(DateTimePrecision::Seconds));
        formats.insert(DateTimeFormat::Timestamp(DateTimePrecision::Microseconds));
        formats.insert(DateTimeFormat::Timestamp(DateTimePrecision::Milliseconds));
        formats.insert(DateTimeFormat::Timestamp(DateTimePrecision::Nanoseconds));
        assert_eq!(formats.len(), 1)
    }

    #[test]
    fn test_date_time_options_default_consistent_with_default() {
        let date_time_options: DateTimeOptions = serde_json::from_str(
            r#"{
                "indexed": true,
                "fieldnorms": true,
                "stored": true
            }"#,
        )
        .unwrap();
        assert_eq!(date_time_options, DateTimeOptions::default());
    }

    #[test]
    fn test_parse_date_time_field_mapping_single_value() {
        let date_time_options = serde_json::from_str::<DateTimeOptions>(
            r#"{
                "input_formats": [
                    "RCF3339", "RFC2822", {"Timestamp": "Milliseconds"}, {"Strftime": "%Y %m %d %H:%M:%S %z"}
                ],
                "precision": "Milliseconds",
                "indexed": true,
                "fieldnorms": false,
                "stored": false
            }"#,
        )
        .unwrap();

        let mut input_formats = HashSet::new();
        input_formats.insert(DateTimeFormat::RCF3339);
        input_formats.insert(DateTimeFormat::RFC2822);
        input_formats.insert(DateTimeFormat::Timestamp(DateTimePrecision::Milliseconds));
        input_formats.insert(DateTimeFormat::Strftime("%Y %m %d %H:%M:%S %z".to_string()));

        let expected_dt_opts = DateTimeOptions {
            input_formats,
            precision: DateTimePrecision::Milliseconds,
            indexed: true,
            fieldnorms: false,
            fast: None,
            stored: false,
            parsers: Arc::new(Mutex::new(None)),
        };

        assert_eq!(date_time_options, expected_dt_opts);
    }

    #[test]
    fn test_serialize_date_time_field() {
        let date_time_options = serde_json::from_str::<DateTimeOptions>(
            r#"
            {
                "indexed": true,
                "fieldnorms": false,
                "stored": false,
                "input_formats": [{"Timestamp": "Milliseconds"}]
            }"#,
        )
        .unwrap();

        let date_time_options_json = serde_json::to_value(&date_time_options).unwrap();
        assert_eq!(
            date_time_options_json,
            serde_json::json!({
                "input_formats": [{"Timestamp": "Milliseconds"}],
                "precision": "Milliseconds",
                "indexed": true,
                "fieldnorms": false,
                "stored": false
            })
        );
    }

    #[test]
    fn test_deserialize_date_time_options_with_wrong_options() {
        assert!(serde_json::from_str::<DateTimeOptions>(
            r#"{
                "indexed": true,
                "fieldnorms": false,
                "stored": false,
                "name": "foo",
            }"#
        )
        .unwrap_err()
        .to_string()
        .contains(
            "unknown field `name`, expected one of `indexed`, `fieldnorms`, `fast`, `stored`, \
             `input_formats`, `precision`"
        ));

        assert!(serde_json::from_str::<DateTimeOptions>(
            r#"{
                "indexed": true,
                "fieldnorms": false,
                "stored": "wrong_value",
            }"#
        )
        .unwrap_err()
        .to_string()
        .contains("expected a boolean"));

        assert!(serde_json::from_str::<DateTimeOptions>(
            r#"{
                "indexed": true,
                "fieldnorms": false,
                "stored": false,
                "precision": "hours"
            }"#
        )
        .unwrap_err()
        .to_string()
        .contains("unknown variant `hours`"));
    }

    #[test]
    fn test_strftime_parser() {
        let parse_without_timezone = make_strftime_parser("%Y-%m-%d %H:%M:%S".to_string());
        let date_time = parse_without_timezone("2012-05-21 12:09:14").unwrap();
        assert_eq!(date_time.date(), date!(2012 - 05 - 21));
        assert_eq!(date_time.time(), time!(12:09:14));

        let parse_with_timezone = make_strftime_parser("%Y-%m-%d %H:%M:%S %z".to_string());
        let date_time = parse_with_timezone("2012-05-21 12:09:14 -02:00").unwrap();
        assert_eq!(date_time.date(), date!(2012 - 05 - 21));
        assert_eq!(date_time.time(), time!(14:09:14));
    }

    #[test]
    fn test_timestamp_parser() {
        let now = time::OffsetDateTime::now_utc();

        let parse_with_secs = make_timestamp_parser(DateTimePrecision::Seconds);
        let date_time = parse_with_secs(now.unix_timestamp()).unwrap();
        assert_eq!(date_time.date(), now.date());
        assert_eq!(date_time.time().as_hms(), now.time().as_hms());

        let parse_with_millis = make_timestamp_parser(DateTimePrecision::Milliseconds);
        let ts_millis = now.unix_timestamp_nanos() / 1_000_000;
        let date_time = parse_with_millis(ts_millis as i64).unwrap();
        assert_eq!(date_time.date(), now.date());
        assert_eq!(date_time.time().as_hms_milli(), now.time().as_hms_milli());

        let parse_with_micros = make_timestamp_parser(DateTimePrecision::Microseconds);
        let ts_micros = now.unix_timestamp_nanos() / 1000;
        let date_time = parse_with_micros(ts_micros as i64).unwrap();
        assert_eq!(date_time.date(), now.date());
        assert_eq!(date_time.time().as_hms_micro(), now.time().as_hms_micro());

        let parse_with_nanos = make_timestamp_parser(DateTimePrecision::Nanoseconds);
        let date_time = parse_with_nanos(now.unix_timestamp_nanos() as i64).unwrap();
        assert_eq!(date_time.date(), now.date());
        assert_eq!(date_time.time(), now.time());
    }
}
