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

"""Test getting the yaml configurations from file."""


from viirs_active_fires.config import read_config


def test_get_yaml_configuration_for_alarm_filtering(fake_yamlconfig_file):
    """Test read and get the yaml configuration for the VIIRS AF runner."""
    service_name = "viirs-ibands"
    environment = "test"
    config = read_config(fake_yamlconfig_file, service_name, environment)

    assert config['log_rotation_days'] == 1
    assert config['log_rotation_backup'] == 10
    assert config['tle_dir'] == '/data/24/saf/polar_in/tle'
    assert config['tlefilename'] == 'tle-{time:%Y%m%d%H%M}.txt'
    assert config['site'] == 'nrk'
    assert config['areas_of_interest'] == ['euron1']
    assert config['message_types'] == ['/segment/SDR/1B']
    assert config['publish_topic'] == ['VIIRS/L2/AFI']

    assert config['instrument'] == 'viirs'
    assert config['platforms'] == ['NOAA-20', 'NOAA-21']

    assert config['viirs_af_call'] == 'cspp_active_fire_noaa.sh'
    assert config['num_of_cpus'] == 4
    assert config['output_dir'] == '/data/prodtest/saf/polar_out/viirs_active_fires'
