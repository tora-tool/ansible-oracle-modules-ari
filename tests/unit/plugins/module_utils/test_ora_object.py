# -*- coding: utf-8 -*-

# Copyright: (c) 2020, Ari Stark <ari.stark@netcourrier.com>
# GNU General Public License v3.0+ (see COPYING or https://www.gnu.org/licenses/gpl-3.0.txt)

from __future__ import (absolute_import, division, print_function)

__metaclass__ = type

import unittest

from ansible_collections.ari_stark.ansible_oracle_modules.plugins.module_utils.ora_object import Size


class TestSize(unittest.TestCase):
    def test_to_string_unlimited(self):
        size = Size('unlimited')
        self.assertEqual('unlimited', str(size))

    def test_to_string_int(self):
        size = Size(123)
        self.assertEqual('123', str(size))
        size = Size(125952)
        self.assertEqual('123K', str(size))

    def test_to_string_oracle_format(self):
        size = Size('15M')
        self.assertEqual('15M', str(size))
        size = Size('125952K')
        self.assertEqual('123M', str(size))
        size = Size('0.5M')
        self.assertEqual('512K', str(size))
        size = Size('1024E')
        self.assertEqual('1Z', str(size))
        size = Size('1280K')
        self.assertEqual('1280K', str(size))

    def test_equals(self):
        self.assertEqual(Size('10M'), Size('10M'))
        self.assertNotEqual(Size('10M'), Size('20M'))
        self.assertNotEqual(Size('10M'), Size('unlimited'))
        self.assertEqual(Size('unlimited'), Size('unlimited'))
        self.assertNotEqual(Size('1M'), 'foo')

    def test_compare(self):
        size1 = Size('1M')
        size2 = Size('1.5M')
        self.assertGreater(size2, size1)
        self.assertLess(size1, size2)
        self.assertFalse(size1 < size1)
        self.assertFalse(size1 > size1)
        self.assertGreater(Size('unlimited'), size2)
        self.assertFalse(Size('unlimited') < size2)
        self.assertLess(size2, Size('unlimited'))
        self.assertFalse(size2 > Size('unlimited'))


if __name__ == '__main__':
    unittest.main()
