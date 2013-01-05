#!/usr/bin/env python
# encoding: utf-8

from optparse import OptionParser
import logging

from bucketbrigade import BucketBrigadeJob

parser = OptionParser()
parser.add_option("-j", "--job", dest="job",
                          help="job configuration file", metavar="JOB")
parser.add_option("-q", "--quiet",
                          action="store_false", dest="verbose", default=True,
                          help="don't print status messages to stdout")
parser.add_option("-p", "--parallel",
                          dest="parallel", default="1",
                          help="number of workers to launch for divide-able jobs like copy")
parser.add_option("-l", "--log",
                          dest="logging", default="WARNING",
                          help="select log level - overrides quiet argument if specified")

(options, args) = parser.parse_args()

if options.verbose:
    logging.basicConfig( level = logging.INFO )

numeric_level = getattr(logging, options.logging.upper(), None)
if not isinstance(numeric_level, int):
        raise ValueError('Invalid log level: %s' % options.logging)
try:
    num_workers = int( options.parallel )
except:
    raise ValueError('Invalid number of workers: %s' % options.parallel)

if __name__ == "__main__":
    if options.job is not None:
        job = BucketBrigadeJob( options.job, num_workers )
        job.run()
    else:
        raise Exception('No valid job specified. Exiting.')

