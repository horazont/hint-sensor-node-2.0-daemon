import unittest

import sn2daemon.datagram_stream as datagram_stream


class TestSerialNumber(unittest.TestCase):
    def setUp(self):
        self.sn_bits = 16

    def tearDown(self):
        pass

    def test_init(self):
        v = datagram_stream.SerialNumber(self.sn_bits)
        self.assertEqual(v._value, 0)
        self.assertEqual(v._mod, 2**self.sn_bits)

        v = datagram_stream.SerialNumber(self.sn_bits, 12)
        self.assertEqual(v._value, 12)
        self.assertEqual(v._mod, 2**self.sn_bits)

    def test_init_rejects_out_of_bounds_value(self):
        with self.assertRaisesRegexp(
                ValueError,
                r"65536 out of bounds for SerialNumber with SERIAL_BITS=16"):
            datagram_stream.SerialNumber(self.sn_bits, 2**16)

        with self.assertRaisesRegexp(
                ValueError,
                r"-1 out of bounds for SerialNumber with SERIAL_BITS=16"):
            datagram_stream.SerialNumber(self.sn_bits, -1)

    def test_init_rejects_non_integer_value(self):
        with self.assertRaisesRegexp(
                TypeError,
                r"SerialNumber objects can only hold integers"):
            datagram_stream.SerialNumber(self.sn_bits, 2.3)

    def test_equality_to_ints(self):
        v = datagram_stream.SerialNumber(self.sn_bits, 156)
        self.assertEqual(v, 156)
        self.assertEqual(156, v)
        self.assertFalse(v != 156)
        self.assertFalse(156 != v)

        self.assertNotEqual(v, 155)
        self.assertNotEqual(155, v)
        self.assertFalse(v == 155)
        self.assertFalse(155 == v)

    def test_equality_to_same_bitted_SerialNumbers(self):
        v1 = datagram_stream.SerialNumber(self.sn_bits, 156)
        v2 = datagram_stream.SerialNumber(self.sn_bits, 155)
        v3 = datagram_stream.SerialNumber(self.sn_bits, 156)

        self.assertNotEqual(v1, v2)
        self.assertEqual(v1, v3)

        self.assertNotEqual(v2, v1)
        self.assertNotEqual(v2, v3)

        self.assertEqual(v3, v1)
        self.assertNotEqual(v3, v2)

    def test_equality_to_different_bitted_SerialNumbers(self):
        v1 = datagram_stream.SerialNumber(self.sn_bits, 156)
        v2 = datagram_stream.SerialNumber(15, 156)

        self.assertNotEqual(v1, v2)
        self.assertNotEqual(v2, v1)

    def test_rfc1982_test_cases(self):
        v0 = datagram_stream.SerialNumber(2, 0)
        v1 = datagram_stream.SerialNumber(2, 1)
        v2 = datagram_stream.SerialNumber(2, 2)
        v3 = datagram_stream.SerialNumber(2, 3)

        self.assertTrue(v1 > v0)
        self.assertTrue(v2 > v1)
        self.assertTrue(v3 > v2)
        self.assertTrue(v0 > v3)

        self.assertTrue(v1 >= v0)
        self.assertTrue(v2 >= v1)
        self.assertTrue(v3 >= v2)
        self.assertTrue(v0 >= v3)

        self.assertTrue(v0 < v1)
        self.assertTrue(v1 < v2)
        self.assertTrue(v2 < v3)
        self.assertTrue(v3 < v0)

        self.assertTrue(v0 <= v1)
        self.assertTrue(v1 <= v2)
        self.assertTrue(v2 <= v3)
        self.assertTrue(v3 <= v0)

        self.assertTrue(v0 <= v0)
        self.assertTrue(v0 >= v0)

        self.assertTrue(v1 <= v1)
        self.assertTrue(v1 >= v1)

        self.assertTrue(v2 <= v2)
        self.assertTrue(v2 >= v2)

        self.assertTrue(v3 <= v3)
        self.assertTrue(v3 >= v3)

    def test_add_plain_integer(self):
        v1 = datagram_stream.SerialNumber(self.sn_bits, 156)
        v2 = v1 + 2

        self.assertEqual(v2, 158)

    def test_radd_plain_integer(self):
        v1 = datagram_stream.SerialNumber(self.sn_bits, 156)
        v2 = 2 + v1

        self.assertEqual(v2, 158)

    def test_radd_negative_integer(self):
        v1 = datagram_stream.SerialNumber(self.sn_bits, 156)
        v2 = (-2) + v1

        self.assertEqual(v2, 154)

    def test_add_rejects_out_of_bounds_addition(self):
        v1 = datagram_stream.SerialNumber(self.sn_bits, 156)
        with self.assertRaisesRegexp(
                ValueError,
                "32768 out of bounds for addition to SerialNumber with "
                "SERIAL_BITS=16"):
            v1 + 2**15
        with self.assertRaisesRegexp(
                ValueError,
                "-32768 out of bounds for addition to SerialNumber with "
                "SERIAL_BITS=16"):
            v1 + (-2**15)

    def test_add_rejects_non_integer_addition(self):
        v1 = datagram_stream.SerialNumber(self.sn_bits, 156)
        with self.assertRaises(TypeError):
            v1 + 1.2

    def test_inplace_addition_creates_copy(self):
        v1 = datagram_stream.SerialNumber(self.sn_bits, 156)
        v1_ref = v1
        v1 += 1
        self.assertIsNot(v1, v1_ref)
        self.assertNotEqual(v1, v1_ref)

    def test_reject_addition_of_serial_number(self):
        v1 = datagram_stream.SerialNumber(self.sn_bits, 156)
        v2 = datagram_stream.SerialNumber(self.sn_bits, 2)

        with self.assertRaisesRegexp(
                TypeError,
                "only integers can be added to SerialNumber objects"):
            v1 + v2

    def test_addition_wraparound(self):
        v1 = datagram_stream.SerialNumber(self.sn_bits, 65500)
        v1 += 500
        self.assertEqual(v1, 464)

    def test_to_int(self):
        v1 = datagram_stream.SerialNumber(self.sn_bits, 156)
        self.assertEqual(v1, v1.to_int())

    def test_sub_plain_integer(self):
        v1 = datagram_stream.SerialNumber(self.sn_bits, 156)
        v2 = v1 - 2

        self.assertEqual(v2, 154)

    def test_sub_negative_integer(self):
        v1 = datagram_stream.SerialNumber(self.sn_bits, 156)
        v2 = v1 - (-2)

        self.assertEqual(v2, 158)

    def test_add_negative_integer(self):
        v1 = datagram_stream.SerialNumber(self.sn_bits, 156)
        v2 = v1 + (-2)

        self.assertEqual(v2, 154)

    def test_reject_rsub(self):
        v1 = datagram_stream.SerialNumber(self.sn_bits, 156)
        with self.assertRaises(TypeError):
            v2 = 2 - v1

    def test_sub_rejects_out_of_bounds_subtraction(self):
        v1 = datagram_stream.SerialNumber(self.sn_bits, 156)
        with self.assertRaisesRegexp(
                ValueError,
                "32768 out of bounds for subtraction from SerialNumber with "
                "SERIAL_BITS=16"):
            v1 - 2**15
        with self.assertRaisesRegexp(
                ValueError,
                "-32768 out of bounds for subtraction from SerialNumber with "
                "SERIAL_BITS=16"):
            v1 - (-2**15)

    def test_sub_rejects_non_integer_subtraction(self):
        v1 = datagram_stream.SerialNumber(self.sn_bits, 156)
        with self.assertRaises(TypeError):
            v1 + 1.2

    def test_sub_serial_number_returns_int(self):
        v1 = datagram_stream.SerialNumber(self.sn_bits, 156)
        v2 = datagram_stream.SerialNumber(self.sn_bits, 2)

        self.assertIsInstance(v1 - v2, int)

    def test_subtraction_yields_correct_sign(self):
        v1 = datagram_stream.SerialNumber(self.sn_bits, 464)
        v2 = datagram_stream.SerialNumber(self.sn_bits, 2)
        v3 = datagram_stream.SerialNumber(self.sn_bits, 65500)

        self.assertEqual(v1 - v2, 462)
        self.assertEqual(v2 - v1, -462)

        self.assertEqual(v1 - v3, 500)
        self.assertEqual(v3 - v1, -500)

    def test_addition_of_difference_yields_original_value(self):
        v1 = datagram_stream.SerialNumber(self.sn_bits, 464)
        v2 = datagram_stream.SerialNumber(self.sn_bits, 2)
        v3 = datagram_stream.SerialNumber(self.sn_bits, 65500)

        self.assertEqual(v1, (v1 - v2) + v2)
        self.assertEqual(v1, (v1 - v3) + v3)

        self.assertEqual(v2, (v2 - v1) + v1)
        self.assertEqual(v2, (v2 - v3) + v3)

        self.assertEqual(v3, (v3 - v1) + v1)
        self.assertEqual(v3, (v3 - v2) + v2)

    def test_subtraction_raises_on_undefined(self):
        v1 = datagram_stream.SerialNumber(2, 0)
        v2 = datagram_stream.SerialNumber(2, 2)

        with self.assertRaisesRegexp(
                ValueError,
                r"difference between 0_\{2\} and 2_\{2\} is undefined"):
            v1 - v2

        with self.assertRaisesRegexp(
                ValueError,
                r"difference between 2_\{2\} and 0_\{2\} is undefined"):
            v2 - v1

    def test_subtraction_wraparound(self):
        v1 = datagram_stream.SerialNumber(self.sn_bits, 464)
        v1 -= 500
        self.assertEqual(v1, 65500)


class TestSerialNumberRangeSet(unittest.TestCase):
    def setUp(self):
        self.rs = datagram_stream.SerialNumberRangeSet()

    def tearDown(self):
        del self.rs

    def test_init(self):
        self.assertEqual(self.rs.nranges, 0)
        self.assertCountEqual(self.rs.iter_ranges(), [])

    def test_add_makes_range(self):
        self.rs.add(1)

        self.assertEqual(self.rs.nranges, 1)
        self.assertCountEqual(self.rs.iter_ranges(),
                              [(1, 1)])

    def test_add_is_idempotent(self):
        self.rs.add(1)
        self.rs.add(1)

        self.assertEqual(self.rs.nranges, 1)
        self.assertCountEqual(self.rs.iter_ranges(),
                              [(1, 1)])

    def test_add_extends_existing_range_to_the_right(self):
        self.rs.add(1)
        self.rs.add(2)

        self.assertEqual(self.rs.nranges, 1)
        self.assertCountEqual(self.rs.iter_ranges(),
                              [(1, 2)])

    def test_add_extends_existing_range_to_the_left(self):
        self.rs.add(1)
        self.rs.add(0)

        self.assertEqual(self.rs.nranges, 1)
        self.assertCountEqual(self.rs.iter_ranges(),
                              [(0, 1)])

    def test_add_makes_range_if_not_extensible(self):
        self.rs.add(1)
        self.rs.add(3)

        self.assertEqual(self.rs.nranges, 2)
        self.assertCountEqual(self.rs.iter_ranges(),
                              [(1, 1), (3, 3)])

    def test_add_merges_ranges(self):
        self.rs.add(1)
        self.rs.add(3)
        self.rs.add(2)

        self.assertEqual(self.rs.nranges, 1)
        self.assertCountEqual(self.rs.iter_ranges(),
                              [(1, 3)])

    def test_discard_up_to_shortens_range(self):
        self.rs.add(1)
        self.rs.add(2)
        self.rs.add(3)

        self.rs.discard_up_to(2)

        self.assertEqual(self.rs.nranges, 1)
        self.assertCountEqual(self.rs.iter_ranges(), [(3, 3)])

    def test_discard_up_to_drops_range(self):
        self.rs.add(1)
        self.rs.add(2)
        self.rs.add(3)

        self.rs.add(5)
        self.rs.add(6)

        self.rs.discard_up_to(4)

        self.assertEqual(self.rs.nranges, 1)
        self.assertCountEqual(self.rs.iter_ranges(), [(5, 6)])

    def test_first_start(self):
        self.assertIsNone(self.rs.first_start)

        self.rs.add(1)

        self.assertEqual(self.rs.first_start, 1)

        self.rs.add(2)
        self.rs.add(5)

        self.assertEqual(self.rs.first_start, 1)

        self.rs.discard_up_to(1)

        self.assertEqual(self.rs.first_start, 2)

        self.rs.discard_up_to(2)

        self.assertEqual(self.rs.first_start, 5)

        self.rs.discard_up_to(5)

        self.assertIsNone(self.rs.first_start)

    def test_first_end(self):
        self.assertIsNone(self.rs.first_end)

        self.rs.add(1)

        self.assertEqual(self.rs.first_end, 1)

        self.rs.add(2)
        self.rs.add(5)

        self.assertEqual(self.rs.first_end, 2)

        self.rs.discard_up_to(1)

        self.assertEqual(self.rs.first_end, 2)

        self.rs.discard_up_to(2)

        self.assertEqual(self.rs.first_end, 5)

        self.rs.discard_up_to(5)

        self.assertIsNone(self.rs.first_end)

    def test_clear(self):
        self.rs.add(1)
        self.rs.add(4)

        self.rs.clear()

        self.assertEqual(self.rs.nranges, 0)
        self.assertCountEqual(self.rs.iter_ranges(), [])
        self.assertIsNone(self.rs.first_end)
        self.assertIsNone(self.rs.first_start)

    def test_contains_check(self):
        self.assertNotIn(2, self.rs)

        self.rs.add(2)

        self.assertIn(2, self.rs)

        self.rs.add(1)
        self.rs.add(3)

        self.assertIn(1, self.rs)
        self.assertIn(2, self.rs)
        self.assertIn(3, self.rs)
