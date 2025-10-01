#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2024 Adam.Dybbroe

# Author(s):

#   Adam.Dybbroe <a000680@c22526.ad.smhi.se>

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

"""Unit testing the VIIRS AF runner."""

import logging
from viirs_active_fires.config import read_config
from viirs_active_fires.active_fires_runner import ViirsActiveFiresProcessor


def test_check_message_noaa21_okay(fake_yamlconfig_file, fake_message_noaa21, caplog):
    """Test the checking of the message."""
    service_name = "viirs-ibands"
    environment = "test"
    config = read_config(fake_yamlconfig_file, service_name, environment)

    viirs_af_proc = ViirsActiveFiresProcessor(config)
    viirs_af_proc.initialise(service_name)

    with caplog.at_level(logging.INFO):
        result = viirs_af_proc.check_message(fake_message_noaa21)

    assert caplog.text == ''
    assert result is True


def test_check_message_noaa21_not_configured(fake_yamlconfig_file_no_noaa21,
                                             fake_message_noaa21, caplog):
    """Test the checking of the message - no noaa-21 supported in config."""
    service_name = "viirs-ibands"
    environment = "test"
    config = read_config(fake_yamlconfig_file_no_noaa21, service_name, environment)

    viirs_af_proc = ViirsActiveFiresProcessor(config)
    viirs_af_proc.initialise(service_name)

    with caplog.at_level(logging.INFO):
        result = viirs_af_proc.check_message(fake_message_noaa21)

    assert 'Platform name not configured: NOAA-21' in caplog.text
    assert result is False
