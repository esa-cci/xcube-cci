from unittest import TestCase

from dateutil.relativedelta import relativedelta

from xcube_cci.timeutil import get_time_period, get_timestrings_from_string


class TimeUtilTest(TestCase):

    def test_get_time_period(self) -> None:
        cases = (
            ("3-hours", relativedelta(hours=3), 10),
            ("day", relativedelta(days=1), 8),
            ("5 days", relativedelta(days=5), 8),
            ("month", relativedelta(months=1), 6),
            ("year", relativedelta(years=1), 4),
            ("5-yrs", relativedelta(years=5), 4),
            ("13 years", relativedelta(years=13), 4),
            ("climatology", None, None),
            ("satellite-orbit-frequency", None, None),
            ("unspecified", None, None),
            (None, None, None),
        )
        for frequency, period, minimum_digits in cases:
            with self.subTest(frequency=frequency):
                self.assertEqual(get_time_period(frequency), (period, minimum_digits))

    def test_get_timestrings_with_minimum_precision(self) -> None:
        cases = (
            ("product-20000229T03.nc", 10, "2000-02-29T03:00:00", None),
            ("product-20000229-20000301.nc", 8, "2000-02-29T00:00:00", "2000-03-01T00:00:00"),
            ("product-20000229.nc", 10, None, None),
            ("product-20010229.nc", 8, None, None),
            ("product-20000227-20000228-20000229.nc", 8, None, None),
        )
        for name, minimum_digits, start, end in cases:
            with self.subTest(name=name, minimum_digits=minimum_digits):
                self.assertEqual(
                    get_timestrings_from_string(name, minimum_digits), (start, end)
                )

    def test_get_timestrings_from_string(self):
        timestring_1, timestring_2 = get_timestrings_from_string(
            '20020401-20020406-ESACCI-L3C_AEROSOL-AEX_ENVISAT-AERGOM_5days-fv2.19.nc'
        )
        self.assertEqual('2002-04-01T00:00:00', timestring_1)
        self.assertEqual('2002-04-06T00:00:00', timestring_2)

        timestring_1, timestring_2 = get_timestrings_from_string(
            '20020401-ESACCI-L3C_AEROSOL-AEX-GOMOS_ENVISAT-AERGOM_5days-fv2.19.nc'
        )
        self.assertEqual('2002-04-01T00:00:00', timestring_1)
        self.assertIsNone(timestring_2)

    def test_get_timestrings_from_string_months(self):
        timestring_1, timestring_2 = get_timestrings_from_string(
            'ESACCI-AEROSOL-L3-AAI-MSAAI-39Y1m_APRIL-fv1.7.nc'
        )
        self.assertEqual(4, timestring_1)
        self.assertIsNone(timestring_2)

        timestring_1, timestring_2 = get_timestrings_from_string(
            'ESACCI-AEROSOL-L3-AAI-MSAAI-39Y1m_OCTOBER-fv1.7.nc'
        )
        self.assertEqual(10, timestring_1)
        self.assertIsNone(timestring_2)

