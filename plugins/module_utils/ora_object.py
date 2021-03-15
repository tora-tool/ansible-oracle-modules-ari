# Copyright: (c) 2020, Ari Stark <ari.stark@netcourrier.com>
# GNU General Public License v3.0+ (see COPYING or https://www.gnu.org/licenses/gpl-3.0.txt)

from __future__ import absolute_import, division, print_function

__metaclass__ = type

import re


class Size:
    """
    Class to modelize a size clause.

    More information on a size clause here :
    https://docs.oracle.com/en/database/oracle/oracle-database/19/sqlrf/size_clause.html
    """

    size = 0  # Size in bytes
    unlimited = False
    units = ['K', 'M', 'G', 'T', 'P', 'E']

    def __init__(self, size):
        try:  # If it's an int
            self.size = int(size)
        except (ValueError, TypeError):  # Else, try to convert
            if size.lower() == 'unlimited':
                self.unlimited = True

            m = re.compile(r'^(\d+(?:\.\d+)?)([' + ''.join(self.units) + '])$', re.IGNORECASE).match(size)
            if m:
                value = m.group(1)
                unit = m.group(2).upper()
                self.size = int(float(value) * 1024 ** (self.units.index(unit) + 1))

    def __str__(self):
        if self.unlimited:
            return 'unlimited'
        num = self.size
        for unit in [''] + self.units:
            if num % 1024.0 != 0:
                return '%i%s' % (num, unit)
            num /= 1024.0
        return '%i%s' % (num, 'Z')

    def __eq__(self, other):
        if not isinstance(other, self.__class__):
            return False
        return (self.unlimited and other.unlimited or
                self.size == other.size)

    def __lt__(self, other):
        if self.unlimited:
            return False
        elif other.unlimited:
            return True
        elif self.size < other.size:
            return True
        else:
            return False

    def __gt__(self, other):
        if other.unlimited:
            return False
        elif self.unlimited:
            return True
        elif self.size > other.size:
            return True
        else:
            return False
