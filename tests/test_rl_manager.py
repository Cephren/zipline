from unittest import TestCase
import pandas as pd

from zipline.rl_manager import InMemoryRLManager, Restriction, \
    RestrictionsController, StaticRestrictedList

str_to_ts = lambda dt_str: pd.Timestamp(dt_str, tz='UTC')


class RLTestCase(TestCase):

    def test_in_memory_rl_manager(self):
        restrictions = [
            Restriction(1, str_to_ts('2011-01-03'), str_to_ts('2011-01-05'),
                        'freeze'),
            Restriction(1, str_to_ts('2011-01-04'), str_to_ts('2011-01-07'),
                        'liquidate'),
            Restriction(2, str_to_ts('2011-01-03'), str_to_ts('2011-01-04'),
                        'liquidate'),
        ]

        rlm1 = InMemoryRLManager(restrictions)

        self.assertEqual(rlm1.restrictions(
            1, str_to_ts('2011-01-03')), {'freeze'})
        self.assertEqual(rlm1.restrictions(
            1, str_to_ts('2011-01-03 14:31')), {'freeze'})
        self.assertEqual(rlm1.restrictions(
            1, str_to_ts('2011-01-04')), {'freeze', 'liquidate'})
        self.assertEqual(rlm1.restrictions(
            1, str_to_ts('2011-01-05')), {'liquidate'})
        self.assertEqual(rlm1.restrictions(
            2, str_to_ts('2011-01-03')), {'liquidate'})

        self.assertTrue(rlm1.is_restricted(1, str_to_ts('2011-01-03')))
        self.assertTrue(rlm1.is_restricted(1, str_to_ts('2011-01-03 14:31')))
        self.assertTrue(rlm1.is_restricted(1, str_to_ts('2011-01-05')))
        self.assertFalse(rlm1.is_restricted(1, str_to_ts('2011-01-07')))

        restrictions = [
            Restriction(1, str_to_ts('2011-01-03'), str_to_ts('2011-01-05'),
                        'long_only'),
            Restriction(1, str_to_ts('2011-01-04'), str_to_ts('2011-01-08'),
                        'reduce_only')
        ]

        rc = RestrictionsController()

        rlm2 = InMemoryRLManager(restrictions)

        rc.add_restrictions(rlm1)
        rc.add_restrictions(rlm2)

        self.assertEqual(rc.restrictions(1, str_to_ts('2011-01-03')),
                         {'freeze', 'long_only'})
        self.assertEqual(rc.restrictions(1, str_to_ts('2011-01-04')),
                         {'freeze', 'long_only', 'liquidate', 'reduce_only'})
        self.assertEqual(rc.restrictions(1, str_to_ts('2011-01-05')),
                         {'liquidate', 'reduce_only'})
        self.assertEqual(rc.restrictions(1, str_to_ts('2011-01-07')),
                         {'reduce_only'})

        self.assertTrue(rc.is_restricted(1, str_to_ts('2011-01-03')))
        self.assertTrue(rc.is_restricted(1, str_to_ts('2011-01-07')))
        self.assertFalse(rc.is_restricted(1, str_to_ts('2011-01-08')))

    def test_static_restricted_list(self):
        rlm = StaticRestrictedList([1, 2])

        self.assertTrue(rlm.is_restricted(1, str_to_ts('2011-01-03')))
        self.assertTrue(rlm.is_restricted(2, str_to_ts('2011-01-03')))
        self.assertFalse(rlm.is_restricted(3, str_to_ts('2011-01-03')))

        self.assertEqual(
            rlm.restrictions(1, str_to_ts('2011-01-03')), {'freeze'})
        self.assertEqual(
            rlm.restrictions(2, str_to_ts('2011-01-03')), {'freeze'})
        self.assertEqual(rlm.restrictions(3, str_to_ts('2011-01-03')), {})


