#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2019 Pytroll

# Author(s):

#   Adam.Dybbroe <adam.dybbroe@smhi.se>

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

"""Level-2 CSPP VIIRS Active Fire runner. From VIIRS SDRs it generates Active
Fire outputs on I- and/or M-bands.

"""

import logging
import os
import sys
from glob import glob
from datetime import datetime, timedelta
from urlparse import urlunsplit
import socket
import netifaces

from active_fires import get_config

#: Default time format
_DEFAULT_TIME_FORMAT = '%Y-%m-%d %H:%M:%S'

#: Default log format
_DEFAULT_LOG_FORMAT = '[%(levelname)s: %(asctime)s : %(name)s] %(message)s'

PATH = os.environ.get('PATH', '')

CSPP_AF_HOME = os.environ.get("CSPP_ACTIVE_FIRE_HOME", '')
CSPP_AF_WORKDIR = os.environ.get("CSPP_ACTIVE_FIRE_WORKDIR", '')
# APPL_HOME = os.environ.get('NPP_SDRPROC', '')


class ViirsActiveFiresProcessor(object):

    """
    Container for the VIIRS Active Fires processing based on CSPP

    """

    def __init__(self, ncpus):
        from multiprocessing.pool import ThreadPool
        self.pool = ThreadPool(ncpus)
        self.ncpus = ncpus

        self.platform_name = 'unknown'  # Ex.: Suomi-NPP
        self.cspp_results = []
        self.pass_start_time = None
        self.result_files = []
        self.sdr_home = OPTIONS['level1_home']
        self.message_data = None

    def initialise(self):
        """Initialise the processor"""
        self.cspp_results = []
        self.pass_start_time = None
        self.result_files = []

    def pack_output_files(self, subd):
        pass

    def run(self, msg):
        """Start the VIIRS Active Fires processing using CSPP on one sdr granule"""

        if msg:
            LOG.debug("Received message: " + str(msg))
        else:
            return True

        self.cspp_results.append(self.pool.apply_async(spawn_cspp, self.result_files))
        LOG.debug("Inside run: Return with a False...")
        return False


def spawn_cspp(sdrfiles, **kwargs):
    """Spawn a CSPP AF run on the set of SDR files given"""

    start_time = kwargs.get('start_time')
    platform_name = kwargs.get('platform_name')

    LOG.info("Start CSPP: SDR files = " + str(sdrfiles))
    working_dir = run_cspp_viirs_af(*sdrfiles)
    LOG.info("CSPP SDR processing finished...")
    # Assume everything has gone well!
    result_files = get_sdr_files(working_dir, platform_name=platform_name)
    LOG.info("SDR file names: %s", str([os.path.basename(f) for f in result_files]))
    if len(result_files) == 0:
        LOG.warning("No SDR files available. CSPP probably failed!")
        return working_dir, []

    LOG.info("Number of results files = " + str(len(result_files)))
    return working_dir, result_files


def viirs_active_fire_runner(options, service_name):
    """The live runner for the CSPP VIIRS AF product generation"""
    from multiprocessing import cpu_count

    LOG.info("Start the VIIRS active fire runner...")
    LOG.debug("Listens for messages of type: %s", str(options['message_types']))

    ncpus_available = cpu_count()
    LOG.info("Number of CPUs available = " + str(ncpus_available))
    ncpus = int(OPTIONS.get('ncpus', 1))
    LOG.info("Will use %d CPUs when running the CSPP VIIRS Active Fires instances", ncpus)
    viirs_af_proc = ViirsActiveFiresProcessor(ncpus)

    with posttroll.subscriber.Subscribe('', options['message_types'], True) as subscr:
        with Publish('viirs_active_fire_runner', 0) as publisher:

            while True:
                viirs_af_proc.initialise()
                for msg in subscr.recv(timeout=300):
                    status = viirs_af_proc.run(msg)
                    if not status:
                        break  # end the loop and reinitialize !

                LOG.debug(
                    "Received message data = %s", str(viirs_af_proc.message_data))
                tobj = viirs_proc.pass_start_time
                LOG.info("Time used in sub-dir name: " +
                         str(tobj.strftime("%Y-%m-%d %H:%M")))

                LOG.info("Get the results from the multiptocessing pool-run")
                for res in viirs_af_proc.cspp_results:
                    working_dir, tmp_result_files = res.get()
                    viirs_af_proc.result_files = tmp_result_files

                    # Here cleanup and publish...

    # #viirs_sdr_dir = "/data/temp/AdamD/xxx/noaa20_20190423_0205_07388"
    # #viirs_sdr_dir = "/data/temp/AdamD/xxx/noaa20_20190423_1017_07393"
    # viirs_sdr_dir = "/data/lang/proj/safworks/fires2019/npp_20190424_1046_38804/"
    # if service_name == "viirs-mbands":
    #     run_cspp_viirs_af(viirs_sdr_dir, mbands=True)
    # elif service_name == "viirs-ibands":
    #     run_cspp_viirs_af(viirs_sdr_dir, mbands=False)

    return


def run_cspp_viirs_af(viirs_sdr_dir, mbands=True):
    """A wrapper for the CSPP VIIRS Active Fire algorithm"""

    from subprocess import Popen, PIPE, STDOUT
    import time
    import tempfile

    viirs_af_call = OPTIONS['viirs_af_call']
    try:
        working_dir = tempfile.mkdtemp(dir=CSPP_AF_WORKDIR)
    except OSError:
        working_dir = tempfile.mkdtemp()

    cmdlist = [viirs_af_call]
    cmdlist.extend(['-d', '-W', working_dir, '--num-cpu', '%d' % int(OPTIONS.get('num_of_cpus', 4))])
    if mbands:
        cmdlist.extend(['-M'])
        cmdlist.extend(glob(os.path.join(viirs_sdr_dir, 'GMTCO*.h5')))
    else:
        # I-bands:
        cmdlist.extend([viirs_sdr_dir])

    t0_clock = time.clock()
    t0_wall = time.time()
    LOG.info("Popen call arguments: " + str(cmdlist))

    my_env = os.environ.copy()
    viirs_af_proc = Popen(cmdlist,
                          cwd=working_dir,
                          shell=False, env=my_env,
                          stderr=PIPE, stdout=PIPE)
    while True:
        line = viirs_af_proc.stdout.readline()
        if not line:
            break
        LOG.info(line.strip('\n'))

    while True:
        errline = viirs_af_proc.stderr.readline()
        if not errline:
            break
        LOG.info(errline.strip('\n'))
    LOG.info("Seconds process time: " + str(time.clock() - t0_clock))
    LOG.info("Seconds wall clock time: " + str(time.time() - t0_wall))

    viirs_af_proc.poll()

    return working_dir


def get_arguments():
    """
    Get command line arguments
    Return
    name of the service and the config filepath
    """
    import argparse

    parser = argparse.ArgumentParser()

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
    parser.add_argument("-v", "--verbose",
                        help="print debug messages too",
                        action="store_true")

    args = parser.parse_args()

    if args.config_file == '':
        print "Configuration file required! viirs_af_runner.py <file>"
        sys.exit()
    if args.environment == '':
        print "Environment required! Use command-line switch -s <service name>"
        sys.exit()
    if args.service == '':
        print "Service required! Use command-line switch -e <environment>"
        sys.exit()

    service = args.service.lower()
    environment = args.environment.lower()

    if 'template' in args.config_file:
        print "Template file given as master config, aborting!"
        sys.exit()

    return environment, service, args.config_file, args.nagios_file


# ---------------------------------------------------------------------------
if __name__ == "__main__":

    handler = logging.StreamHandler(sys.stderr)
    handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter(fmt=_DEFAULT_LOG_FORMAT,
                                  datefmt=_DEFAULT_TIME_FORMAT)

    handler.setFormatter(formatter)
    logging.getLogger('').addHandler(handler)
    logging.getLogger('').setLevel(logging.DEBUG)
    logging.getLogger('posttroll').setLevel(logging.INFO)

    (environ, service_name, config_filename, nagios_config_file) = get_arguments()
    print("Read config from %s" % config_filename)
    OPTIONS = get_config(config_filename, service_name, environ)
    OPTIONS['environment'] = environ
    OPTIONS['nagios_config_file'] = nagios_config_file

    LOG = logging.getLogger('viirs-active-fire-runner')
    viirs_active_fire_runner(OPTIONS, service_name)
