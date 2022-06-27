use std::io::{Read, Write};
use std::ops::Deref;
use std::{fmt, io};

use byteorder::{ReadBytesExt, WriteBytesExt};
use chrono::NaiveDate;
use common::BinarySerializable;
use serde::{Deserialize, Serialize};

use super::date_time_options::DateTimeFormat;
use crate::time::format_description::well_known::{Iso8601, Rfc2822, Rfc3339};
use crate::time::{OffsetDateTime, PrimitiveDateTime, UtcOffset};

/// A date/time value with custom precision.
///
/// This DateTime does not carry any timezone information
/// and stores all value in UTC time.
/// Users are responsible for applying needed conversion when
/// directly using this type.
///
/// All constructors and conversions are provided as explicit
/// functions and not by implementing any `From`/`Into` traits
/// to prevent unintended usage.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct PreciseDateTime {
    /// The timestamp component.
    pub(crate) timestamp: i64,
    /// The precision component to denote if the timestamp
    /// value is in seconds, milliseconds, microseconds or nanoseconds.
    pub(crate) precision: DateTimePrecision,
}



impl PreciseDateTime {
    /// Create new instance from UNIX timestamp.
    pub const fn from_unix_timestamp(unix_timestamp: i64) -> Self {
        Self {
            timestamp: unix_timestamp,
            precision: DateTimePrecision::Seconds,
        }
    }

    /// Create a new instance from a timestamp and it's precision.
    pub const fn from_timestamp_with_precision(
        timestamp: i64,
        precision: DateTimePrecision,
    ) -> Self {
        Self {
            timestamp,
            precision,
        }
    }

    /// Creates new DateTime from utc time with precision.
    pub fn from_utc_with_precision(
        utc_datetime: OffsetDateTime,
        precision: DateTimePrecision,
    ) -> Self {
        let timestamp = match precision {
            DateTimePrecision::Seconds => utc_datetime.unix_timestamp(),
            DateTimePrecision::Milliseconds => {
                (utc_datetime.unix_timestamp_nanos() / 1_000_000) as i64
            }
            DateTimePrecision::Microseconds => (utc_datetime.unix_timestamp_nanos() / 1000) as i64,
            DateTimePrecision::Nanoseconds => utc_datetime.unix_timestamp_nanos() as i64,
        };

        Self {
            timestamp,
            precision,
        }
    }

    /// Create new from `OffsetDateTime`
    ///
    /// The given date/time is converted to UTC and the actual
    /// time zone is discarded.
    pub const fn from_utc(dt: OffsetDateTime) -> Self {
        Self::from_unix_timestamp(dt.unix_timestamp())
    }

    /// Create new from `PrimitiveDateTime`
    ///
    /// Implicitly assumes that the given date/time is in UTC!
    pub const fn from_primitive(dt: PrimitiveDateTime) -> Self {
        Self::from_utc(dt.assume_utc())
    }

    /// Convert to UNIX timestamp (seconds)
    pub const fn into_unix_timestamp(self) -> i64 {
        let Self {
            timestamp,
            precision,
        } = self;
        match precision {
            DateTimePrecision::Seconds => timestamp,
            DateTimePrecision::Milliseconds => timestamp / 1_000,
            DateTimePrecision::Microseconds => timestamp / 1_000_000,
            DateTimePrecision::Nanoseconds => timestamp / 1_000_000_000,
        }
    }

    /// Returns the underlying timestamp.
    pub fn get_timestamp(&self) -> i64 {
        self.timestamp
    }

    /// Returns the underlying precision.
    pub fn get_precision(&self) -> DateTimePrecision {
        self.precision
    }

    /// Convert to UTC `OffsetDateTime`
    pub fn into_utc(self) -> OffsetDateTime {
        let Self {
            timestamp,
            precision,
        } = self;
        let utc_datetime =
            precise_timestamp_to_datetime(timestamp, precision).expect("valid UNIX timestamp");
        debug_assert_eq!(UtcOffset::UTC, utc_datetime.offset());
        utc_datetime
    }

    /// Convert to `OffsetDateTime` with the given time zone
    pub fn into_offset(self, offset: UtcOffset) -> OffsetDateTime {
        self.into_utc().to_offset(offset)
    }

    /// Convert to `PrimitiveDateTime` without any time zone
    ///
    /// The value should have been constructed with [`Self::from_primitive()`].
    /// Otherwise the time zone is implicitly assumed to be UTC.
    pub fn into_primitive(self) -> PrimitiveDateTime {
        let utc_datetime = self.into_utc();
        // Discard the UTC time zone offset
        debug_assert_eq!(UtcOffset::UTC, utc_datetime.offset());
        PrimitiveDateTime::new(utc_datetime.date(), utc_datetime.time())
    }

    /// Formats the datetime into string.
    pub fn format(&self, format: DateTimeFormat) -> Result<String, String> {
        let date_time = precise_timestamp_to_datetime(self.timestamp, self.precision)?;
        match format {
            DateTimeFormat::RCF3339 => date_time
                .format(&Rfc3339)
                .map_err(|error| error.to_string()),
            DateTimeFormat::RFC2822 => date_time
                .format(&Rfc2822)
                .map_err(|error| error.to_string()),
            DateTimeFormat::ISO8601 => date_time
                .format(&Iso8601::DEFAULT)
                .map_err(|error| error.to_string()),
            DateTimeFormat::Strftime(str_fmt) => {
                let date = date_time.to_calendar_date();
                let time = date_time.to_hms_nano();
                NaiveDate::from_ymd(date.0, date.1 as u32, date.2 as u32)
                    .and_hms_nano_opt(time.0 as u32, time.1 as u32, time.2 as u32, time.3)
                    .ok_or_else(|| "Couldn't create NaiveDate from OffsetDateTime".to_string())
                    .map(|datetime| datetime.format(&str_fmt).to_string())
            }
            DateTimeFormat::Timestamp(_) => Ok(self.timestamp.to_string()),
        }
    }
}

impl fmt::Debug for PreciseDateTime {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PreciseDateTime")
            .field("timestamp", &self.timestamp)
            .field("precision", &self.precision)
            .finish()
    }
}

/// DateTime Precision
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[repr(u8)]
pub enum DateTimePrecision {
    /// Seconds precision
    Seconds = 0,
    /// Milli-seconds precision.
    Milliseconds = 1,
    /// Micro-seconds precision.
    Microseconds = 2,
    /// Nano-seconds precision.
    Nanoseconds = 3,
}

impl Default for DateTimePrecision {
    fn default() -> Self {
        DateTimePrecision::Milliseconds
    }
}

impl TryFrom<u8> for DateTimePrecision {
    type Error = String;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(DateTimePrecision::Seconds),
            1 => Ok(DateTimePrecision::Milliseconds),
            2 => Ok(DateTimePrecision::Microseconds),
            3 => Ok(DateTimePrecision::Nanoseconds),
            any => Err(format!("Invalid precision `{}` specified.", any)),
        }
    }
}

impl BinarySerializable for DateTimePrecision {
    fn serialize<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        writer.write_u8(*self as u8)
    }
    fn deserialize<R: Read>(reader: &mut R) -> io::Result<Self> {
        let value_u8 = reader.read_u8()?;
        Self::try_from(value_u8).map_err(|error| io::Error::new(io::ErrorKind::Other, error))
    }
}

/// Convert a timestamp with precision to OffsetDateTime.
fn precise_timestamp_to_datetime(
    timestamp: i64,
    precision: DateTimePrecision,
) -> Result<OffsetDateTime, String> {
    match precision {
        DateTimePrecision::Seconds => OffsetDateTime::from_unix_timestamp(timestamp),
        DateTimePrecision::Milliseconds => {
            OffsetDateTime::from_unix_timestamp_nanos((timestamp as i128) * 1_000_000)
        }
        DateTimePrecision::Microseconds => {
            OffsetDateTime::from_unix_timestamp_nanos((timestamp as i128) * 1000)
        }
        DateTimePrecision::Nanoseconds => {
            OffsetDateTime::from_unix_timestamp_nanos(timestamp as i128)
        }
    }
    .map_err(|error| error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::time::format_description::well_known::Iso8601;

    #[test]
    fn test_format_date_time() {
        let dt = PreciseDateTime::from_utc(
            OffsetDateTime::parse("2020-01-02T03:00:00+02:30", &Iso8601::DEFAULT).unwrap(),
        );

        assert_eq!(
            dt.format(DateTimeFormat::ISO8601).unwrap(),
            "2020-01-02T00:30:00.000000000Z"
        );
        assert_eq!(
            dt.format(DateTimeFormat::RFC2822).unwrap(),
            "Thu, 02 Jan 2020 00:30:00 +0000"
        );
        assert_eq!(
            dt.format(DateTimeFormat::RCF3339).unwrap(),
            "2020-01-02T00:30:00Z"
        );

        assert_eq!(
            dt.format(DateTimeFormat::Strftime("%Y-%m-%d %H:%M:%S".to_string()))
                .unwrap(),
            "2020-01-02 00:30:00"
        );
        assert_eq!(
            dt.format(DateTimeFormat::Timestamp(DateTimePrecision::Seconds))
                .unwrap(),
            "1577925000"
        );
    }

    #[test]
    fn test_precise_timestamp_to_datetime() {
        let now = time::OffsetDateTime::now_utc();

        let dt = precise_timestamp_to_datetime(now.unix_timestamp(), DateTimePrecision::Seconds)
            .unwrap();
        assert_eq!(dt.to_ordinal_date(), now.to_ordinal_date());
        assert_eq!(dt.to_hms(), now.to_hms());

        let dt = precise_timestamp_to_datetime(
            (now.unix_timestamp_nanos() / 1_000_000) as i64,
            DateTimePrecision::Milliseconds,
        )
        .unwrap();
        assert_eq!(dt.to_ordinal_date(), now.to_ordinal_date());
        assert_eq!(dt.to_hms_milli(), now.to_hms_milli());

        let dt = precise_timestamp_to_datetime(
            (now.unix_timestamp_nanos() / 1000) as i64,
            DateTimePrecision::Microseconds,
        )
        .unwrap();
        assert_eq!(dt.to_ordinal_date(), now.to_ordinal_date());
        assert_eq!(dt.to_hms_micro(), now.to_hms_micro());

        let dt = precise_timestamp_to_datetime(
            now.unix_timestamp_nanos() as i64,
            DateTimePrecision::Nanoseconds,
        )
        .unwrap();
        assert_eq!(dt.to_ordinal_date(), now.to_ordinal_date());
        assert_eq!(dt.to_hms_nano(), now.to_hms_nano());

        // timestamp not in range
        precise_timestamp_to_datetime(36008724683097897, DateTimePrecision::Seconds).unwrap_err();
    }
}
