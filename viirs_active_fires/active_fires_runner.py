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

"""The main functionality of the VIIRS Active Fires runner."""

import os
import time
import logging

import posttroll.subscriber
from posttroll.publisher import Publish
from posttroll.message import Message

from viirs_active_fires.utils import (deliver_output_files, cleanup_cspp_workdir,
                                      get_edr_times, get_active_fire_result_files)

from urllib.parse import urlparse
from multiprocessing import cpu_count

LOG = logging.getLogger('active_fires_runner')

ptimer = time.perf_counter

CSPP_AF_HOME = os.environ.get("CSPP_ACTIVE_FIRE_HOME", '')
CSPP_AF_WORKDIR = os.environ.get("CSPP_ACTIVE_FIRE_WORKDIR", '')

VIIRS_SATELLITES = ['Suomi-NPP', 'NOAA-20', 'NOAA-21', 'NOAA-22']


class ViirsActiveFiresProcessor(object):
    """Container for the VIIRS Active Fires processing based on CSPP."""

    def __init__(self, viirs_af_config):
        """Initialize the VIIRS Active Fires Processing instance."""
        from multiprocessing.pool import ThreadPool
        ncpus_available = cpu_count()
        LOG.info("Number of CPUs available = " + str(ncpus_available))
        self.ncpus = int(viirs_af_config.get('ncpus', 1))
        LOG.info("Will use %d CPUs when running the CSPP VIIRS Active Fires instances", self.ncpus)
        self.pool = ThreadPool(self.ncpus)

        self.options = viirs_af_config

        self.supported_platforms = self.options.get("platforms", VIIRS_SATELLITES)
        self.orbit_number = 1  # Initialised orbit number
        self.platform_name = 'unknown'  # Ex.: Suomi-NPP
        self.cspp_results = []
        self.pass_start_time = None
        self.result_files = []
        self.sdr_files = []
        self.result_home = self.options.get('output_dir', '/tmp')
        self.publish_topic = self.options.get('publish_topic')
        self.site = self.options.get('site', 'unknown')
        self.environment = self.options.get('environment')
        self.message_data = None
        self.service = None

    def initialise(self, service):
        """Initialise the processor."""
        self.cspp_results = []
        self.pass_start_time = None
        self.result_files = []
        self.sdr_files = []
        self.service = service

    def deliver_output_files(self, subd=None):
        """Deliver the output files."""
        LOG.debug("Result files: %s", str(self.result_files))
        LOG.debug("Result home dir: %s", str(self.result_home))
        LOG.debug("Sub directory: %s", subd)
        return deliver_output_files(self.result_files, self.result_home, subd)

    def check_message(self, msg):
        """Check if message should trigger processing or not."""
        if msg and ('platform_name' not in msg.data or 'sensor' not in msg.data):
            LOG.debug("No platform_name or sensor in message. Continue...")
            return False
        elif msg and not (msg.data['platform_name'] in VIIRS_SATELLITES and
                          msg.data['sensor'] == 'viirs'):
            LOG.info("Not a supported VIIRS scene. Satellite = %s - Continue...",
                     str(msg.data['platform_name']))
            return False

        if msg.data['platform_name'] not in self.options['platforms']:
            LOG.info(f"Platform name not configured: {msg.data['platform_name']}")
            LOG.info(f"Supported platform names: {str(self.options['platforms'])}")
            return False

        return True

    def run(self, msg):
        """Start the VIIRS Active Fires processing using CSPP on one sdr granule."""
        if msg:
            LOG.debug("Received message: " + str(msg))

        self.platform_name = str(msg.data['platform_name'])
        self.sensor = str(msg.data['sensor'])
        self.message_data = msg.data

        okay2run = self.check_message(msg)
        if not okay2run:
            return True

        if msg.type != 'dataset':
            LOG.info("Not a dataset, don't do anything...")
            return True

        sdr_dataset = msg.data['dataset']
        if len(sdr_dataset) < 1:
            return True

        sdr_files = []
        for sdr in sdr_dataset:
            urlobj = urlparse(sdr['uri'])
            sdr_filename = urlobj.path
            # dummy, fname = os.path.split(rdr_filename)
            # Assume all files are valid sdr files ending with '.h5'
            sdr_files.append(sdr_filename)

        self.sdr_files = sdr_files

        self.cspp_results.append(self.pool.apply_async(self._spawn_cspp, (self.sdr_files, self.service)))
        LOG.debug("Inside run: Return with a False...")
        return False

    def _spawn_cspp(self, sdrfiles, service):
        """Spawn a CSPP AF run on the set of SDR files given."""
        LOG.info("Start CSPP: SDR files = " + str(sdrfiles))
        working_dir = run_cspp_viirs_af(self.options, sdrfiles, service)
        LOG.info("CSPP SDR Active Fires processing finished...")
        # Assume everything has gone well!

        result_files = get_active_fire_result_files(working_dir)
        LOG.info("Active Fires results - file names: %s", str([os.path.basename(f) for f in result_files]))
        if len(result_files) == 0:
            LOG.warning("No files available. CSPP probably failed!")
            return working_dir, []

        LOG.info("Number of results files = " + str(len(result_files)))
        return working_dir, result_files


def publish_af(publisher, edr_files, mda, **kwargs):
    """Publish the messages that VIIRS AF EDR files are ready."""
    if not edr_files:
        return

    # Now publish:
    to_send = mda.copy()
    # Delete the SDR dataset from the message:
    try:
        del (to_send['dataset'])
    except KeyError:
        LOG.warning("Couldn't remove dataset from message")

    if 'orbit' in kwargs:
        to_send["orig_orbit_number"] = to_send["orbit_number"]
        to_send["orbit_number"] = kwargs['orbit']

    publish_topic = kwargs.get('publish_topic', 'Unknown')
    site = kwargs.get('site', 'unknown')
    environment = kwargs.get('environment', 'unknown')

    to_send['data_processing_level'] = '2'
    to_send['format'] = 'edr'

    for viirs_edr in edr_files:
        to_send['uri'] = viirs_edr
        filename = os.path.basename(viirs_edr)
        to_send['uid'] = filename
        if filename.endswith('nc'):
            to_send['type'] = 'netcdf'
        elif filename.endswith('txt'):
            to_send['type'] = 'txt'
        else:
            LOG.error("File type unknown! Don't publish. Filename = %s", filename)
            return

        to_send['start_time'], to_send['end_time'] = get_edr_times(filename)

        LOG.debug('Site = %s', site)
        LOG.debug('Publish topic = %s', publish_topic)
        for topic in publish_topic:
            msg = Message('/'.join(('',
                                    topic,
                                    to_send['format'],
                                    to_send['data_processing_level'],
                                    site,
                                    environment,
                                    'polar',
                                    'direct_readout')),
                          "file", to_send).encode()

            LOG.debug("sending: " + str(msg))
            publisher.send(msg)


def viirs_active_fire_runner(config, service_name):
    """Start the live runner for the CSPP VIIRS AF product generation."""
    LOG.info("Start the VIIRS active fire runner...")
    LOG.debug("Listens for messages of type: %s", str(config['message_types']))

    viirs_af_proc = ViirsActiveFiresProcessor(config)

    with posttroll.subscriber.Subscribe('', config['message_types'], True) as subscr:
        with Publish('viirs_active_fire_runner', 0) as publisher:

            while True:
                viirs_af_proc.initialise(service_name)
                for msg in subscr.recv():
                    status = viirs_af_proc.run(msg)
                    if not status:
                        break  # end the loop and reinitialize !

                LOG.debug(
                    "Received message data = %s", str(viirs_af_proc.message_data))

                LOG.info("Get the results from the multiptocessing pool-run")
                for res in viirs_af_proc.cspp_results:
                    working_dir, tmp_result_files = res.get()
                    viirs_af_proc.result_files = tmp_result_files
                    af_files = viirs_af_proc.deliver_output_files()
                    LOG.info("Cleaning up directory %s", working_dir)
                    cleanup_cspp_workdir(working_dir)
                    publish_af(publisher, af_files,
                               viirs_af_proc.message_data,
                               orbit=viirs_af_proc.orbit_number,
                               publish_topic=viirs_af_proc.publish_topic,
                               environment=viirs_af_proc.environment,
                               site=viirs_af_proc.site)

                LOG.info("Active Fires EDR processing has completed.")

    return


def run_cspp_viirs_af(options, viirs_sdr_files, service):
    """Run the CSPP VIIRS AF algorithm.

    A wrapper for the CSPP VIIRS Active Fire algorithm.
    """
    from subprocess import Popen, PIPE
    import time
    import tempfile

    viirs_af_call = options['viirs_af_call']
    try:
        working_dir = tempfile.mkdtemp(dir=CSPP_AF_WORKDIR)
    except OSError:
        working_dir = tempfile.mkdtemp()

    cmdlist = [viirs_af_call]
    cmdlist.extend(['-d', '-W', working_dir, '--num-cpu', '%d' % int(options.get('num_of_cpus', 4))])
    if service == 'viirs-mbands':
        cmdlist.extend(['-M'])
        LOG.info("M-bands: %s", service)
    elif service == 'viirs-ibands':
        # I-bands:
        LOG.info("I-bands %s", service)
    else:
        LOG.warning("Service not recognized: %s - Assume I-bands", service)

    cmdlist.extend(viirs_sdr_files)

    t0_clock = ptimer()
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
        LOG.info(line.strip())

    while True:
        errline = viirs_af_proc.stderr.readline()
        if not errline:
            break
        LOG.info(errline.strip())

    viirs_af_proc.poll()

    LOG.info("Seconds process time: " + str(ptimer() - t0_clock))
    LOG.info("Seconds wall clock time: " + str(time.time() - t0_wall))

    return working_dir
