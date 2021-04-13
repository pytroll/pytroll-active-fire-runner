#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2021 Adam.Dybbroe

# Author(s):

#   Adam.Dybbroe <a000680@c21856.ad.smhi.se>

# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.

# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

"""Unit testing the utility functions.
"""

import os.path
import pytest
from datetime import datetime
from unittest.mock import patch

from viirs_active_fires.utils import get_edr_times

TESTFILENAME = "AFIMG_npp_d20210413_t0916186_e0917428_b49018_c20210413092919781783_cspp_dev.txt"


@patch('os.path.basename')
def test_get_edr_times(basename):
    """Test getting the start and end times from the edr filename."""

    basename.return_value = TESTFILENAME
    timetup = get_edr_times('myfilename')

    expected = (datetime(2021, 4, 13, 9, 16, 18), datetime(2021, 4, 13, 9, 17, 42))
    assert timetup[0] == expected[0]
    assert timetup[1] == expected[1]
