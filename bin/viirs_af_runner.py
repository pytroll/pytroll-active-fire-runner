#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2019 - 2024 Pytroll

# Author(s):

#   Adam Dybbroe <Firstname.Lastname@smhi.se>

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

"""Level-2 CSPP VIIRS Active Fire runner.

From VIIRS SDRs it generates Active Fire outputs on I- and/or M-bands.

"""

import logging
import sys

from viirs_active_fires.config import read_config
from viirs_active_fires.active_fires_runner import viirs_active_fire_runner
from viirs_active_fires.logger import setup_logging

#: Default time format
_DEFAULT_TIME_FORMAT = '%Y-%m-%d %H:%M:%S'

#: Default log format
_DEFAULT_LOG_FORMAT = '[%(levelname)s: %(asctime)s : %(name)s] %(message)s'


def get_arguments():
    """Get command line arguments.

    Return name of the service and the config filepath.
    """
    import argparse

    parser = argparse.ArgumentParser()

    parser.add_argument("-l", "--log-config",
                        help="Log config file to use instead of the standard logging.")
    parser.add_argument('-c', '--config_file',
                        type=str,
                        dest='config_file',
                        default='',
                        help="The file containing " +
                        "configuration parameters e.g. product_filter_config.yaml")
    parser.add_argument("-s", "--service",
                        help="Name of the service (e.g. viirs-ibands)",
                        dest="service",
                        type=str,
                        default="unknown")
    parser.add_argument("-e", "--environment",
                        help="The processing environment (utv/test/prod)",
                        dest="environment",
                        type=str,
                        default="unknown")
    parser.add_argument("--nagios",
                        help="The nagios/monitoring config file path",
                        dest="nagios_file",
                        type=str,
                        default=None)
    parser.add_argument("-v", "--verbose", dest="verbosity", action="count", default=0,
                        help="Verbosity (between 1 and 2 occurrences with more leading to more "
                        "verbose logging). WARN=0, INFO=1, "
                        "DEBUG=2. This is overridden by the log config file if specified.")

    cmd_args = parser.parse_args()

    if cmd_args.config_file == '':
        print("Configuration file required! viirs_af_runner.py <file>")
        sys.exit()
    if cmd_args.environment == '':
        print("Environment required! Use command-line switch -s <service name>")
        sys.exit()
    if cmd_args.service == '':
        print("Service required! Use command-line switch -e <environment>")
        sys.exit()
    if 'template' in cmd_args.config_file:
        print("Template file given as master config, aborting!")
        sys.exit()

    return cmd_args


# ---------------------------------------------------------------------------
if __name__ == "__main__":

    cmdargs = get_arguments()

    service_name = cmdargs.service.lower()
    environ = cmdargs.environment.lower()
    config_filename = cmdargs.config_file
    nagios_config_file = cmdargs.nagios_file

    setup_logging(cmdargs)

    print("Read config from %s" % config_filename)
    viirs_af_config = read_config(config_filename, service_name, environ)
    viirs_af_config['environment'] = environ
    viirs_af_config['nagios_config_file'] = nagios_config_file

    LOG = logging.getLogger('viirs-active-fire-runner')
    viirs_active_fire_runner(viirs_af_config, service_name)
