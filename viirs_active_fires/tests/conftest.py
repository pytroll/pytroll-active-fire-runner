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

"""Fixtures for unittests."""

import pytest
from posttroll.message import Message

TEST_YAML_CONFIG_CONTENT_NO_NOAA21 = """
log_rotation_days: 1
log_rotation_backup: 10

tle_dir: /data/24/saf/polar_in/tle
tlefilename: tle-{time:%Y%m%d%H%M}.txt
site: nrk

viirs-ibands:

  areas_of_interest: [euron1]
  message_types: ['/segment/SDR/1B']
  publish_topic: ['VIIRS/L2/AFI']
  instrument: 'viirs'
  platforms:
    - NOAA-20
    - Suomi-NPP

  viirs_af_call: cspp_active_fire_noaa.sh
  num_of_cpus: 4
  offline:
    output_dir: /home/a000680/data/viirs_active_fires

  prod:
    output_dir: /data/24/saf/polar_out/viirs_active_fires

  test:
    output_dir: /data/prodtest/saf/polar_out/viirs_active_fires

  utv:
    output_dir: /data/proj/safutv/polar_out/viirs_active_fires
"""


TEST_YAML_CONFIG_CONTENT = """
log_rotation_days: 1
log_rotation_backup: 10

tle_dir: /data/24/saf/polar_in/tle
tlefilename: tle-{time:%Y%m%d%H%M}.txt
site: nrk

viirs-ibands:

  areas_of_interest: [euron1]
  message_types: ['/segment/SDR/1B']
  publish_topic: ['VIIRS/L2/AFI']
  instrument: 'viirs'
  platforms:
    - NOAA-20
    - NOAA-21

  viirs_af_call: cspp_active_fire_noaa.sh
  num_of_cpus: 4
  offline:
    output_dir: /home/a000680/data/viirs_active_fires

  prod:
    output_dir: /data/24/saf/polar_out/viirs_active_fires

  test:
    output_dir: /data/prodtest/saf/polar_out/viirs_active_fires

  utv:
    output_dir: /data/proj/safutv/polar_out/viirs_active_fires


viirs-mbands:

  areas_of_interest: [euron1]
  message_types: ['/segment/SDR/1B']
  publish_topic: ['VIIRS/L2/AFM']
  instrument: viirs
  viirs_af_call: cspp_active_fire_noaa.sh
  num_of_cpus: 4
  offline:
    output_dir: /home/a000680/data/viirs_active_fires

  prod:
    output_dir: /data/24/saf/polar_out/viirs_active_fires

  test:
    output_dir: /data/prodtest/saf/polar_out/viirs_active_fires

  utv:
    output_dir: /data/proj/safutv/polar_out/viirs_active_fires
"""

TEST_MESSAGE_SDR_N21 = """pytroll://segment/SDR/1B dataset safusr.t@lxserv2313.smhi.se 2024-10-17T14:57:58.121857 v1.01 application/json {"start_time": "2024-10-17T14:42:17", "end_time": "2024-10-17T14:43:41.700000", "orbit_number": 10035, "platform_name": "NOAA-21", "sensor": "viirs", "format": "SDR", "type": "HDF5", "data_processing_level": "1B", "variant": "DR", "orig_orbit_number": 10034, "dataset": [{"uri": "/san1/polar_in/direct_readout/jpss/viirs/sdr/GMODO_j02_d20241017_t1442170_e1443417_b10035_c20241017145732671983_cspp_dev.h5", "uid": "GMODO_j02_d20241017_t1442170_e1443417_b10035_c20241017145732671983_cspp_dev.h5"}, {"uri": "/san1/polar_in/direct_readout/jpss/viirs/sdr/GMTCO_j02_d20241017_t1442170_e1443417_b10035_c20241017145732555699_cspp_dev.h5", "uid": "GMTCO_j02_d20241017_t1442170_e1443417_b10035_c20241017145732555699_cspp_dev.h5"}, {"uri": "/san1/polar_in/direct_readout/jpss/viirs/sdr/SVM01_j02_d20241017_t1442170_e1443417_b10035_c20241017145747919094_cspp_dev.h5", "uid": "SVM01_j02_d20241017_t1442170_e1443417_b10035_c20241017145747919094_cspp_dev.h5"}, {"uri": "/san1/polar_in/direct_readout/jpss/viirs/sdr/SVM02_j02_d20241017_t1442170_e1443417_b10035_c20241017145747982795_cspp_dev.h5", "uid": "SVM02_j02_d20241017_t1442170_e1443417_b10035_c20241017145747982795_cspp_dev.h5"}, {"uri": "/san1/polar_in/direct_readout/jpss/viirs/sdr/SVM03_j02_d20241017_t1442170_e1443417_b10035_c20241017145748181506_cspp_dev.h5", "uid": "SVM03_j02_d20241017_t1442170_e1443417_b10035_c20241017145748181506_cspp_dev.h5"}, {"uri": "/san1/polar_in/direct_readout/jpss/viirs/sdr/SVM04_j02_d20241017_t1442170_e1443417_b10035_c20241017145748231089_cspp_dev.h5", "uid": "SVM04_j02_d20241017_t1442170_e1443417_b10035_c20241017145748231089_cspp_dev.h5"}, {"uri": "/san1/polar_in/direct_readout/jpss/viirs/sdr/SVM05_j02_d20241017_t1442170_e1443417_b10035_c20241017145748331036_cspp_dev.h5", "uid": "SVM05_j02_d20241017_t1442170_e1443417_b10035_c20241017145748331036_cspp_dev.h5"}, {"uri": "/san1/polar_in/direct_readout/jpss/viirs/sdr/SVM06_j02_d20241017_t1442170_e1443417_b10035_c20241017145748393101_cspp_dev.h5", "uid": "SVM06_j02_d20241017_t1442170_e1443417_b10035_c20241017145748393101_cspp_dev.h5"}, {"uri": "/san1/polar_in/direct_readout/jpss/viirs/sdr/SVM07_j02_d20241017_t1442170_e1443417_b10035_c20241017145748472668_cspp_dev.h5", "uid": "SVM07_j02_d20241017_t1442170_e1443417_b10035_c20241017145748472668_cspp_dev.h5"}, {"uri": "/san1/polar_in/direct_readout/jpss/viirs/sdr/SVM08_j02_d20241017_t1442170_e1443417_b10035_c20241017145748513727_cspp_dev.h5", "uid": "SVM08_j02_d20241017_t1442170_e1443417_b10035_c20241017145748513727_cspp_dev.h5"}, {"uri": "/san1/polar_in/direct_readout/jpss/viirs/sdr/SVM09_j02_d20241017_t1442170_e1443417_b10035_c20241017145748597560_cspp_dev.h5", "uid": "SVM09_j02_d20241017_t1442170_e1443417_b10035_c20241017145748597560_cspp_dev.h5"}, {"uri": "/san1/polar_in/direct_readout/jpss/viirs/sdr/SVM10_j02_d20241017_t1442170_e1443417_b10035_c20241017145748672197_cspp_dev.h5", "uid": "SVM10_j02_d20241017_t1442170_e1443417_b10035_c20241017145748672197_cspp_dev.h5"}, {"uri": "/san1/polar_in/direct_readout/jpss/viirs/sdr/SVM11_j02_d20241017_t1442170_e1443417_b10035_c20241017145748789977_cspp_dev.h5", "uid": "SVM11_j02_d20241017_t1442170_e1443417_b10035_c20241017145748789977_cspp_dev.h5"}, {"uri": "/san1/polar_in/direct_readout/jpss/viirs/sdr/SVM12_j02_d20241017_t1442170_e1443417_b10035_c20241017145748863806_cspp_dev.h5", "uid": "SVM12_j02_d20241017_t1442170_e1443417_b10035_c20241017145748863806_cspp_dev.h5"}, {"uri": "/san1/polar_in/direct_readout/jpss/viirs/sdr/SVM13_j02_d20241017_t1442170_e1443417_b10035_c20241017145748912202_cspp_dev.h5", "uid": "SVM13_j02_d20241017_t1442170_e1443417_b10035_c20241017145748912202_cspp_dev.h5"}, {"uri": "/san1/polar_in/direct_readout/jpss/viirs/sdr/SVM14_j02_d20241017_t1442170_e1443417_b10035_c20241017145749028924_cspp_dev.h5", "uid": "SVM14_j02_d20241017_t1442170_e1443417_b10035_c20241017145749028924_cspp_dev.h5"}, {"uri": "/san1/polar_in/direct_readout/jpss/viirs/sdr/SVM15_j02_d20241017_t1442170_e1443417_b10035_c20241017145749163147_cspp_dev.h5", "uid": "SVM15_j02_d20241017_t1442170_e1443417_b10035_c20241017145749163147_cspp_dev.h5"}, {"uri": "/san1/polar_in/direct_readout/jpss/viirs/sdr/SVM16_j02_d20241017_t1442170_e1443417_b10035_c20241017145749281599_cspp_dev.h5", "uid": "SVM16_j02_d20241017_t1442170_e1443417_b10035_c20241017145749281599_cspp_dev.h5"}, {"uri": "/san1/polar_in/direct_readout/jpss/viirs/sdr/GIMGO_j02_d20241017_t1442170_e1443417_b10035_c20241017145732103478_cspp_dev.h5", "uid": "GIMGO_j02_d20241017_t1442170_e1443417_b10035_c20241017145732103478_cspp_dev.h5"}, {"uri": "/san1/polar_in/direct_readout/jpss/viirs/sdr/GITCO_j02_d20241017_t1442170_e1443417_b10035_c20241017145731946984_cspp_dev.h5", "uid": "GITCO_j02_d20241017_t1442170_e1443417_b10035_c20241017145731946984_cspp_dev.h5"}, {"uri": "/san1/polar_in/direct_readout/jpss/viirs/sdr/SVI01_j02_d20241017_t1442170_e1443417_b10035_c20241017145747209875_cspp_dev.h5", "uid": "SVI01_j02_d20241017_t1442170_e1443417_b10035_c20241017145747209875_cspp_dev.h5"}, {"uri": "/san1/polar_in/direct_readout/jpss/viirs/sdr/SVI02_j02_d20241017_t1442170_e1443417_b10035_c20241017145747391247_cspp_dev.h5", "uid": "SVI02_j02_d20241017_t1442170_e1443417_b10035_c20241017145747391247_cspp_dev.h5"}, {"uri": "/san1/polar_in/direct_readout/jpss/viirs/sdr/SVI03_j02_d20241017_t1442170_e1443417_b10035_c20241017145747537346_cspp_dev.h5", "uid": "SVI03_j02_d20241017_t1442170_e1443417_b10035_c20241017145747537346_cspp_dev.h5"}, {"uri": "/san1/polar_in/direct_readout/jpss/viirs/sdr/SVI04_j02_d20241017_t1442170_e1443417_b10035_c20241017145747675237_cspp_dev.h5", "uid": "SVI04_j02_d20241017_t1442170_e1443417_b10035_c20241017145747675237_cspp_dev.h5"}, {"uri": "/san1/polar_in/direct_readout/jpss/viirs/sdr/SVI05_j02_d20241017_t1442170_e1443417_b10035_c20241017145747845577_cspp_dev.h5", "uid": "SVI05_j02_d20241017_t1442170_e1443417_b10035_c20241017145747845577_cspp_dev.h5"}, {"uri": "/san1/polar_in/direct_readout/jpss/viirs/sdr/GDNBO_j02_d20241017_t1442170_e1443417_b10035_c20241017145731783400_cspp_dev.h5", "uid": "GDNBO_j02_d20241017_t1442170_e1443417_b10035_c20241017145731783400_cspp_dev.h5"}, {"uri": "/san1/polar_in/direct_readout/jpss/viirs/sdr/SVDNB_j02_d20241017_t1442170_e1443417_b10035_c20241017145746973705_cspp_dev.h5", "uid": "SVDNB_j02_d20241017_t1442170_e1443417_b10035_c20241017145746973705_cspp_dev.h5"}, {"uri": "/san1/polar_in/direct_readout/jpss/viirs/sdr/IVCDB_j02_d20241017_t1442170_e1443417_b10035_c20241017145747042504_cspp_dev.h5", "uid": "IVCDB_j02_d20241017_t1442170_e1443417_b10035_c20241017145747042504_cspp_dev.h5"}], "origin": "172.29.4.164:9099"}"""  # noqa


@pytest.fixture
def fake_message_noaa21():
    """Make a fake message with NOAA-21 VIIRS SDR data."""
    yield Message.decode(rawstr=TEST_MESSAGE_SDR_N21)


@pytest.fixture
def fake_yamlconfig_file(tmp_path):
    """Write fake yaml config file."""
    file_path = tmp_path / 'test_viirs_af_config.yaml'
    with open(file_path, 'w') as fpt:
        fpt.write(TEST_YAML_CONFIG_CONTENT)

    yield file_path


@pytest.fixture
def fake_yamlconfig_file_no_noaa21(tmp_path):
    """Write fake yaml config file."""
    file_path = tmp_path / 'test_viirs_af_config_no_noaa21.yaml'
    with open(file_path, 'w') as fpt:
        fpt.write(TEST_YAML_CONFIG_CONTENT_NO_NOAA21)

    yield file_path
