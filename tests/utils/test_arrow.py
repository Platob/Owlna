import numpy
import pyarrow
from pyarrow import RecordBatch, array

from owlna.utils.arrow import cast_batch, cast_array, timestamp_to_timestamp
from tests import AthenaTestCase


class ArrowUtilsTests(AthenaTestCase):

    def test_cast_record_batch_iso(self):
        raw = RecordBatch.from_pydict({"string": ["a"]})

        self.assertEqual(raw, cast_batch(raw, raw.schema))

    def test_cast_record_batch_reorder(self):
        raw = RecordBatch.from_pydict({
            "0": ["0"], "1": ["1"], "2": ["2"]
        })
        expected = RecordBatch.from_pydict({
            "2": ["2"], "1": ["1"]
        })

        self.assertEqual(expected, cast_batch(raw, expected.schema))

    def test_cast_record_batch_case_insensitive(self):
        raw = RecordBatch.from_pydict({
            "aAa": ["a"], "bBb": ["b"], "ccC": ["c"]
        })
        expected = RecordBatch.from_pydict({
            "AaA": ["a"], "BbB": ["b"], "DDd": [None]
        })

        self.assertEqual(expected, cast_batch(raw, expected.schema))

    def test_cast_record_batch_fill_empty_columns(self):
        raw = RecordBatch.from_pydict({
            "0": ["0"], "1": ["1"], "2": ["2"]
        })
        expected = RecordBatch.from_pydict({
            "2": ["2"], "1": ["1"], "3": [None]
        })

        self.assertEqual(expected, cast_batch(raw, expected.schema))

    def test_cast_array_string_int(self):
        self.assertEqual(
            array([10, 10.3, None], pyarrow.int8()),
            cast_array(array(["10", "10.3", None]), pyarrow.int8())
        )
        self.assertEqual(
            array([10, 10.3, None], pyarrow.int16()),
            cast_array(array(["10", "10.3", None]), pyarrow.int16())
        )
        self.assertEqual(
            array([10, 10.3, None], pyarrow.int32()),
            cast_array(array(["10", "10.3", None]), pyarrow.int32())
        )
        self.assertEqual(
            array([10, 10.3, None], pyarrow.int64()),
            cast_array(array(["10", "10.3", None]), pyarrow.int64())
        )

    def test_cast_array_string_float(self):
        self.assertEqual(
            array([10, 10.3, None], pyarrow.float32()),
            cast_array(array(["10", "10.3", None]), pyarrow.float32())
        )
        self.assertEqual(
            array([10, 10.3, None], pyarrow.float64()),
            cast_array(array(["10", "10.3", None]), pyarrow.float64())
        )

    def test_cast_array_string_decimal(self):
        self.assertEqual(
            array([10, 10.2, None]).cast(pyarrow.decimal128(15, 8)),
            cast_array(array(["10", "10.2", None]), pyarrow.decimal128(15, 8))
        )
        self.assertEqual(
            array([10, 10.2, None]).cast(pyarrow.decimal256(15, 8)),
            cast_array(array(["10", "10.2", None]), pyarrow.decimal256(15, 8))
        )

    def test_cast_array_string_timestamp_iso(self):
        self.assertEqual(
            array([numpy.datetime64("2022-10-10 12:00:12.123000000"), None]),
            cast_array(array(["2022-10-10 12:00:12.123", None]), pyarrow.timestamp("ns"))
        )
        self.assertEqual(
            array([numpy.datetime64("2022-10-10 12:00:12.123456700"), None]),
            cast_array(array(["2022-10-10 12:00:12.1234567", None]), pyarrow.timestamp("ns"))
        )
        self.assertEqual(
            array([numpy.datetime64("2022-10-10 12:00:12.123456789"), None]),
            cast_array(array(["2022-10-10 12:00:12.123456789", None]), pyarrow.timestamp("ns"))
        )

        self.assertEqual(
            array([numpy.datetime64("2022-10-10T12:00:12.123000000"), None]).cast(pyarrow.timestamp("ms")),
            cast_array(array(["2022-10-10T12:00:12.123", None]), pyarrow.timestamp("ms"))
        )
        self.assertEqual(
            array([numpy.datetime64("2022-10-10T12:00:12.123000000"), None]).cast(pyarrow.timestamp("ms")),
            cast_array(array(["2022-10-10T12:00:12.123", None]), pyarrow.timestamp("ms"))
        )

    def test_cast_array_string_timestamp_iso_with_utc_timezone(self):
        self.assertEqual(
            array([numpy.datetime64("2022-10-10T12:00:12.123000000"), None]).cast(pyarrow.timestamp("ns", "UTC")),
            cast_array(array(["2022-10-10T12:00:12.123Z", None]), pyarrow.timestamp("ns", "UTC"))
        )
        self.assertEqual(
            array([numpy.datetime64("2022-10-10T12:00:12.123456000"), None]).cast(pyarrow.timestamp("ns", "UTC")),
            cast_array(array(["2022-10-10T12:00:12.123456Z", None]), pyarrow.timestamp("ns", "UTC"))
        )

    def test_cast_array_string_timestamp_iso_with_utc_timezone_unsafe(self):
        self.assertEqual(
            array([numpy.datetime64("2022-10-10T12:00:12.123456789"), None])
            .cast(pyarrow.timestamp("ms", "UTC"), safe=False),
            cast_array(array(["2022-10-10T12:00:12.123456Z", None]), pyarrow.timestamp("ms", "UTC"), safe=False)
        )
        self.assertEqual(
            array([numpy.datetime64("2022-10-10T12:00:12.123456789"), None])
            .cast(pyarrow.timestamp("ms", "UTC"), safe=False),
            cast_array(array(["2022-10-10T12:00:12.123456+00:00", None]), pyarrow.timestamp("ms", "UTC"), safe=False)
        )

    def test_cast_array_string_timestamp_iso_with_time_offset_unsafe(self):
        self.assertEqual(
            array([numpy.datetime64("2022-10-10T11:00:12.123456789"), None])
            .cast(pyarrow.timestamp("ms", "UTC"), safe=False),
            cast_array(array(["2022-10-10T12:00:12.123456+01:00", None]), pyarrow.timestamp("ms", "UTC"), safe=False)
        )

    def test_cast_array_naive_timestamp_assume_timezone_unsafe(self):
        self.assertEqual(
            array([numpy.datetime64("2022-11-02T13:27:12.123000000"), None])
            .cast(pyarrow.timestamp("ns", "GMT"), safe=False),
            timestamp_to_timestamp(
                array(["2022-11-02T13:27:12.123", None]).cast(pyarrow.timestamp("ms")),
                pyarrow.timestamp("ns", "GMT"),
                safe=False
            )
        )
        self.assertEqual(
            array([numpy.datetime64("2022-11-02T13:27:12.123000000"), None])
            .cast(pyarrow.timestamp("ns", "UTC"), safe=False),
            timestamp_to_timestamp(
                array(["2022-11-02T13:27:12.123", None]).cast(pyarrow.timestamp("ms")),
                pyarrow.timestamp("ns", "UTC"),
                safe=False
            )
        )
        self.assertEqual(
            array([numpy.datetime64("2022-11-02T12:27:12.123456789"), None])
            .cast(pyarrow.timestamp("ms", "Europe/Paris"), safe=False),
            timestamp_to_timestamp(
                array(["2022-11-02T13:27:12.123", None]).cast(pyarrow.timestamp("ms")),
                pyarrow.timestamp("ms", "Europe/Paris"),
                safe=False
            )
        )
        self.assertEqual(
            array([numpy.datetime64("2022-11-10T11:00:14.123456789"), None])
            .cast(pyarrow.timestamp("us", "Europe/Paris"), safe=False),
            timestamp_to_timestamp(
                array(["2022-11-10T12:00:14.123456", None]).cast(pyarrow.timestamp("us")),
                pyarrow.timestamp("us", "Europe/Paris"),
                safe=False
            )
        )

    def test_cast_array_string_timestamp_iso_naive_unsafe(self):
        self.assertEqual(
            array([numpy.datetime64("2022-10-10T12:00:12.123456789"), None]).cast(pyarrow.timestamp("ms"), safe=False),
            cast_array(array(["2022-10-10T12:00:12.123456Z", None]), pyarrow.timestamp("ms"), safe=False)
        )
