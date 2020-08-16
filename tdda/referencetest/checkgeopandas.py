# -*- coding: utf-8 -*-

"""
checkgeopandas.py: comparison mechanism for geopandas geodataframes (and parquet files)

Source repository: http://github.com/tdda/tdda

License: MIT

Copyright (c) Stochastic Solutions Limited 2016-2018
"""

from __future__ import absolute_import
from __future__ import print_function
from __future__ import division

import os
import sys

from collections import OrderedDict

from tdda.referencetest.basecomparison import BaseComparison, Diffs

try:
    import geopandas as gpd
    import numpy as np
except ImportError:
    gpd = None


class GeoPandasNotImplemented(object):
    """
    Null implementation of GeoPandasComparison, used when geopandas not available.
    """

    def __getattr__(self, name):
        return lambda *args, **kwargs: self.method(name, *args, **kwargs)

    def method(self, name, *args, **kwargs):
        raise NotImplementedError("%s: GeoPandas not available." % name)


class GeoPandasComparison(BaseComparison):
    """
    Comparison class for geopandas geodataframes (and parquet files).
    """

    def __new__(cls, *args, **kwargs):
        if gpd is None:
            return GeoPandasNotImplemented()
        return super(GeoPandasComparison, cls).__new__(cls)

    def check_geodataframe(
        self,
        gdf,
        ref_gdf,
        actual_path=None,
        expected_path=None,
        check_data=None,
        check_types=None,
        check_order=None,
        check_extra_cols=None,
        sortby=None,
        condition=None,
        precision=None,
        msgs=None,
    ):
        """
        Compare two geopandas geodataframes.

            *gdf*
                            Actual geodataframe
            *ref_gdf*
                            Expected geodataframe
            *actual_path*
                            Path for file where actual geodataframe originated,
                            used for error messages.
            *expected_path*
                            Path for file where expected geodataframe originated,
                            used for error messages.
            *check_types*
                            Option to specify fields to use to compare typees.
            *check_order*
                            Option to specify fields to use to compare field
                            order.
            *check_data*
                            Option to specify fields to use to compare cell
                            values.
            *check_extra_cols*
                            Option to specify fields in the actual dataset
                            to use to check that there are no unexpected
                            extra columns.
            *sortby*
                            Option to specify fields to sort by before
                            comparing.
            *condition*
                            Filter to be applied to datasets before comparing.
                            It can be ``None``, or can be a function that takes
                            a geodataframe as its single parameter and returns
                            a vector of booleans (to specify which rows should
                            be compared).
            *precision*
                            Number of decimal places to compare float values.
            *msgs*
                            Optional Diffs object.

        Returns a tuple (failures, msgs), containing the number of failures,
        and a Diffs object containing error messages.

        the comparison 'Option' flags can be of any of the following:

            - ``None`` (to apply that kind of comparison to all fields)
            - ``False`` (to skip that kind of comparison completely)
            - a list of field names
            - a function taking a geodataframe as its single parameter, and
              returning a list of field names to use.
        """

        same = True
        if msgs is None:
            msgs = Diffs()

        if precision is None:
            precision = 6

        check_types = resolve_option_flag(check_types, ref_gdf)
        check_extra_cols = resolve_option_flag(check_extra_cols, gdf)

        missing_cols = []
        extra_cols = []
        wrong_types = []
        wrong_ordering = False
        for c in check_types:
            if c not in list(gdf):
                missing_cols.append(c)
            elif gdf[c].dtype != ref_gdf[c].dtype:
                wrong_types.append((c, gdf[c].dtype, ref_gdf[c].dtype))
        if check_extra_cols:
            # todo: shouldn't check_extra_cols be a boolean?
            #       ... and set(check_extra_cols) be set(list(df))
            extra_cols = set(check_extra_cols) - set(list(ref_df))
        if check_order != False and not missing_cols:
            check_order = resolve_option_flag(check_order, ref_gdf)
            c1 = [c for c in list(gdf) if c in check_order]
            c2 = [c for c in list(ref_gdf) if c in check_order]
            wrong_ordering = list(gdf[c1]) != list(ref_gdf[c2])
        same = not any((missing_cols, extra_cols, wrong_types, wrong_ordering))

        if not same:
            self.failure(msgs, "Column check failed.", actual_path, expected_path)
            if missing_cols:
                self.info(msgs, "Missing columns: %s" % missing_cols)
            if extra_cols:
                self.info(msgs, "Extra columns: %s" % list(extra_cols))
            if wrong_types:
                for (c, dtype, ref_dtype) in wrong_types:
                    self.info(
                        msgs,
                        "Wrong column type %s (%s, expected %s)"
                        % (c, dtype, ref_dtype),
                    )
            if wrong_ordering:
                c1 = [c for c in list(gdf) if c in check_types]
                c2 = [c for c in list(ref_gdf) if c in check_types]
                ordermsg = "mysterious difference, they appear to be the same!"
                for i, c in enumerate(c1):
                    if i >= len(c2):
                        ordermsg = "not enough columns"
                        break
                    elif c != c2[i]:
                        ordermsg = "found %s, expected %s" % (c, c2[i])
                        break
                self.info(msgs, "Wrong column ordering: %s" % ordermsg)

        if sortby:
            sortby = resolve_option_flag(sortby, ref_gdf)
            if any([c in sortby for c in missing_cols]):
                self.info("Cannot sort on missing columns")
            else:
                gdf.sort_values(sortby, inplace=True)
                ref_gdf.sort_values(sortby, inplace=True)

        if condition:
            gdf = gdf[condition(gdf)].reindex()
            ref_gdf = ref_gdf[condition(ref_gdf)].reindex()

        same = len(gdf) == len(ref_gdf)
        if not same:
            self.failure(msgs, "Length check failed.", actual_path, expected_path)
            self.info(msgs, "Found %d records, expected %d" % (len(gdf), len(ref_gdf)))
        else:
            check_data = resolve_option_flag(check_data, ref_gdf)
            if check_data:
                check_data = [c for c in check_data if c not in missing_cols]
                gdf = gdf[check_data]
                ref_gdf = ref_gdf[check_data]
                if precision is not None:
                    rounded = gdf.round(precision).reset_index(drop=True)
                    ref_rounded = ref_gdf.round(precision).reset_index(drop=True)
                else:
                    rounded = gdf
                    ref_rounded = ref_gdf
                same = rounded.equals(ref_rounded)
                if not same:
                    self.failure(
                        msgs, "Contents check failed.", actual_path, expected_path
                    )
                    for c in list(ref_rounded):
                        if not rounded[c].equals(ref_rounded[c]):
                            diffs = self.differences(
                                c, rounded[c], ref_rounded[c], precision
                            )
                            self.info(msgs, "Column values differ: %s" % c)
                            self.info(msgs, diffs)

        same = same and not any((missing_cols, extra_cols, wrong_types, wrong_ordering))
        return (0 if same else 1, msgs)

    def differences(self, name, values, ref_values, precision):
        """
        Returns a short summary of where values differ, for two columns.
        """
        for i, val in enumerate(values):
            refval = ref_values[i]
            if val != refval and not (gpd.isnull(val) and gpd.isnull(refval)):
                stop = self.ndifferences(values, ref_values, i)
                summary_vals = self.sample_format(values, i, stop, precision)
                summary_ref_vals = self.sample_format(ref_values, i, stop, precision)
                return "From row %d: [%s] != [%s]" % (
                    i + 1,
                    summary_vals,
                    summary_ref_vals,
                )
        if values.dtype != ref_values.dtype:
            return "Different types"
        else:
            return "But mysteriously appear to be identical!"

    def sample(self, values, start, stop):
        return [
            None if gpd.isnull(values[i]) else values[i] for i in range(start, stop)
        ]

    def sample_format(self, values, start, stop, precision):
        s = self.sample(values, start, stop)
        r = ", ".join(
            [
                "null"
                if gpd.isnull(v)
                else str("%d" % v)
                if type(v) in (np.int, np.int32, np.int64)
                else str("%.*f" % (precision, v))
                if type(v) in (np.float, np.float32, np.float64)
                else str('"%s"' % v)
                if values.dtype == object
                else str(v)
                for v in s
            ]
        )
        if len(s) < stop - start:
            r += " ..."
        return r

    def ndifferences(self, values1, values2, start, limit=10):
        stop = min(start + limit, len(values1))
        for i in range(start, stop):
            v1 = values1[i]
            v2 = values2[i]
            if v1 == v2 or (gpd.isnull(v1) and gpd.isnull(v2)):
                return i
        return stop

    def check_parquet_file(
        self,
        actual_path,
        expected_path,
        loader=None,
        check_data=None,
        check_types=None,
        check_order=None,
        condition=None,
        sortby=None,
        precision=6,
        msgs=None,
        **kwargs
    ):
        """
        Checks two parquet files are the same, by comparing them as geodataframes.

            *actual_path*
                            Pathname for actual parquet file.
            *expected_path*
                            Pathname for expected parquet file.
            *loader*
                            A function to use to read a parquet file to obtain
                            a geopandas geodataframe. If None, then a default parquet
                            loader is used, which takes the same parameters
                            as the standard geopandas gpd.read_parquet() function.
            *\*\*kwargs*
                            Any additional named parameters are passed straight
                            through to the loader function.

        The other parameters are the same as those used by
        :py:mod:`check_geodataframe`.
        Returns a tuple (failures, msgs), containing the number of failures,
        and a Diffs object containing error messages.
        """
        ref_gdf = self.load_parquet(expected_path, loader=loader, **kwargs)
        gdf = self.load_parquet(actual_path, loader=loader, **kwargs)
        return self.check_geodataframe(
            gdf,
            ref_gdf,
            actual_path=actual_path,
            expected_path=expected_path,
            check_data=check_data,
            check_types=check_types,
            check_order=check_order,
            condition=condition,
            sortby=sortby,
            precision=precision,
            msgs=msgs,
        )

    def check_parquet_files(
        self,
        actual_paths,
        expected_paths,
        check_data=None,
        check_types=None,
        check_order=None,
        condition=None,
        sortby=None,
        msgs=None,
        **kwargs
    ):
        """
        Wrapper around the check_parquet_file() method, used to compare
        collections of actual and expected parquet files.

            *actual_paths*
                            List of pathnames for actual parquet file.
            *expected_paths*
                            List of pathnames for expected parquet file.
            *loader*
                            A function to use to read a parquet file to obtain
                            a geopandas geodataframe. If None, then a default parquet
                            loader is used, which takes the same parameters
                            as the standard geopandas gpd.read_parquet() function.
            *\*\*kwargs*
                            Any additional named parameters are passed straight
                            through to the loader function.

        The other parameters are the same as those used by
        :py:mod:`check_geodataframe`.
        Returns a tuple (failures, msgs), containing the number of failures,
        and a list of error messages.

        Returns a tuple (failures, msgs), containing the number of failures,
        and a Diffs object containing error messages.

        Note that this function compares ALL of the pairs of actual/expected
        files, and if there are any differences, then the number of failures
        returned reflects the total number of differences found across all
        of the files, and the msgs returned contains the error messages
        accumulated across all of those comparisons. In other words, it
        doesn't stop as soon as it hits the first error, it continues through
        right to the end.
        """
        if msgs is None:
            msgs = Diffs()
        failures = 0
        for (actual_path, expected_path) in zip(actual_paths, expected_paths):
            try:
                r = self.check_parquet_file(
                    actual_path,
                    expected_path,
                    check_data=check_data,
                    check_types=check_types,
                    check_order=check_order,
                    sortby=sortby,
                    condition=condition,
                    msgs=msgs,
                    **kwargs
                )
                (n, msgs) = r
                failures += n
            except Exception as e:
                self.info(
                    msgs,
                    "Error comparing %s and %s (%s %s)"
                    % (
                        os.path.normpath(actual_path),
                        expected_path,
                        e.__class__.__name__,
                        str(e),
                    ),
                )
                failures += 1
        return (failures, msgs)

    def failure(self, msgs, s, actual_path, expected_path):
        """
        Add a failure to the list of messages, and also display it immediately
        if verbose is set. Also provide information about the two files
        involved.
        """
        if actual_path and expected_path:
            self.info(
                msgs, self.compare_with(os.path.normpath(actual_path), expected_path)
            )
        elif expected_path:
            self.info(msgs, "Expected file %s" % expected_path)
        elif actual_path:
            self.info(msgs, "Actual file %s" % os.path.normpath(actual_path))
        self.info(msgs, s)

    def all_fields_except(self, exclusions):
        """
        Helper function, for using with *check_data*, *check_types* and
        *check_order* parameters to assertion functions for geopandas geodataframes.

        It returns the names of all of the fields in the geodataframe being
        checked, apart from the ones given.

        *exclusions* is a list of field names.
        """
        return lambda gdf: list(set(list(gdf)) - set(exclusions))

    def load_parquet(self, parquetfile, loader=None, **kwargs):
        """
        Function for constructing a geopandas geodataframe from a parquet file.
        """
        if loader is None:
            loader = default_parquet_loader
        return loader(parquetfile, **kwargs)

    def write_parquet(self, gdf, parquetfile, writer=None, **kwargs):
        """
        Function for saving a geopandas geodataframe to a parquet file.
        Used when regenerating geodataframe reference results.
        """
        if writer is None:
            writer = default_parquet_writer
        writer(gdf, parquetfile, **kwargs)


def default_parquet_loader(parquetfile, **kwargs):
    """
    Default function for reading a parquet file.

    Wrapper around the standard geopandas gpd.read_parquet() function, but with
    slightly different defaults:

        - index_col             is ``None``
        - infer_datetime_format is ``True``
        - quotechar             is ``"``
        - quoting               is :py:const:`parquet.QUOTE_MINIMAL`
        - escapechar            is ``\\`` (backslash)
        - na_values             are the empty string, ``"NaN"``, and ``"NULL"``
        - keep_default_na       is ``False``
    """
    options = {
        "index_col": None,
        "infer_datetime_format": True,
        "quotechar": '"',
        "quoting": parquet.QUOTE_MINIMAL,
        "escapechar": "\\",
        "na_values": ["", "NaN", "NULL"],
        "keep_default_na": False,
    }
    options.update(kwargs)

    try:
        gdf = gpd.read_parquet(parquetfile, **options)
    except gpd.errors.ParserError:
        # geopandas parquet reader gets confused by stutter-quoted text that
        # also includes escapechars. So try again, with no escapechar.
        del options["escapechar"]
        gdf = gpd.read_parquet(parquetfile, **options)

    # the reader won't have inferred any datetime columns (even though we
    # told it to), because we didn't explicitly tell it the column names
    # in advance. so.... we'll do it by hand (looking at string columns, and
    # seeing if we can convert them safely to datetimes).
    if options.get("infer_datetime_format"):
        colnames = gdf.columns.tolist()
        for c in colnames:
            if gdf[c].dtype == np.dtype("O"):
                try:
                    datecol = gpd.to_datetime(gdf[c])
                    if datecol.dtype == np.dtype("datetime64[ns]"):
                        gdf[c] = datecol
                except Exception as e:
                    pass
        ngdf = gpd.GeoDataFrame()
        for c in colnames:
            ngdf[c] = gdf[c]

    return ngdf


def default_parquet_writer(gdf, parquetfile, **kwargs):
    """
    Default function for writing a parquet file.

    Wrapper around the standard geopandas gpd.to_parquet() function, but with
    slightly different defaults:

        - index                 is ``False``
        - encoding              is ``utf-8``
    """
    options = {
        "index": False,
        "encoding": "utf-8",
    }
    options.update(kwargs)
    if sys.version_info[0] > 2 and len(gdf) > 0:
        bytes_cols = find_bytes_cols(gdf)
        if bytes_cols:
            gdf = bytes_to_unicode(gdf, bytes_cols)
    return gdf.to_parquet(parquetfile, **options)


def find_bytes_cols(gdf):
    bytes_cols = []
    for c in list(gdf):
        if gdf[c].dtype == "O":
            nonnulls = gdf[gdf[c].notnull()].reset_index()[c]
            if len(nonnulls) > 0 and type(nonnulls[0]) is bytes:
                bytes_cols.append(c)
    return bytes_cols


def bytes_to_unicode(gdf, bytes_cols):
    cols = OrderedDict()
    for c in list(gdf):
        if c in bytes_cols:
            cols[unicode_definite(c)] = gdf[c].str.decode("UTF-8")
        else:
            cols[unicode_definite(c)] = gdf[c]
    return gpd.GeoDataFrame(cols, index=gdf.index.copy())


def unicode_definite(s):
    return s if type(s) == str else s.decode("UTF-8")


def resolve_option_flag(flag, gdf):
    """
    Method to resolve an option flag, which may be any of:

       ``None`` or ``True``:
                use all columns in the geodataframe
       ``None``:
                use no columns
       list of columns
                use these columns
       function returning a list of columns
    """
    if flag is None or flag is True:
        return list(gdf)
    elif flag is False:
        return []
    elif hasattr(flag, "__call__"):
        return flag(gdf)
    else:
        return flag

