# -*- coding: utf-8 -*-

"""
Extensions to the ``tdda`` command line tool, to support Pandas dataframes
and CSV files.
"""

from __future__ import print_function

import sys

from tdda.constraints.extension import ExtensionBase

from tdda.constraints.pd.pdconstraints import applicable
from tdda.constraints.pd.pddiscover import PandasDiscoverer
from tdda.constraints.pd.pdverify import PandasVerifier


class TDDAPandasExtension(ExtensionBase):
    def __init__(self, argv, verbose=False):
        ExtensionBase.__init__(self, argv, verbose=verbose)

    def applicable(self):
        return applicable(self.argv)

    def help(self, stream=sys.stdout):
        print('  - CSV files', file=stream)
        print('  - Pandas DataFrames as .feather files', file=stream)

    def discover(self):
        return PandasDiscoverer(self.argv, verbose=self.verbose).discover()

    def verify(self):
        return PandasVerifier(self.argv, verbose=self.verbose).verify()
