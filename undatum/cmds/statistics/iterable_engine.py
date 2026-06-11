"""Iterable-backed statistics computations and display (mixin)."""

import logging
import time

from tqdm import tqdm

from ...common.command_utils import get_iterable_options
from ...common.s3_iterable import open_iterable_with_s3
from ...constants import DEFAULT_DICT_SHARE
from ...utils import dict_generator, get_option, guess_datatype


class IterableStatsMixin:
    """Iterable statistics engine methods for StatProcessor."""

    def _stats_iterable(self, fromfile, options):
        """Compute statistics using iterable engine (row-by-row processing).

        This is the original implementation, now refactored into a separate method.
        """

        iterableargs = get_iterable_options(options)
        iterable_context = open_iterable_with_s3(fromfile, mode="r", iterableargs=iterableargs)
        iterable = iterable_context.__enter__()
        dictshare = get_option(options, "dictshare")

        if dictshare and dictshare.isdigit():
            dictshare = int(dictshare)
        else:
            dictshare = DEFAULT_DICT_SHARE

        profile = {"version": 1.0}
        fielddata = {}
        fieldtypes = {}

        #    data = json.load(open(profile['filename']))
        count = 0

        # Get progress control option (default: show progress)
        show_progress = get_option(options, "progress") is not False
        if "no_progress" in options and options["no_progress"]:
            show_progress = False

        # process data items one by one
        logging.debug(f"Start processing {fromfile}")
        start_time = time.time()
        try:
            # Wrap iterable with tqdm if progress should be shown
            if show_progress:
                iterable_wrapped = tqdm(iterable, desc="Analyzing statistics", unit="rows")
            else:
                iterable_wrapped = iterable

            # Use context manager for tqdm to ensure proper cleanup
            if show_progress:
                with iterable_wrapped as pbar:
                    for item in pbar:
                        count += 1
                        dk = dict_generator(item)
                        if count % 1000 == 0:
                            logging.debug(f"Processing {count} records of {fromfile}")
                            # Update throughput in progress bar
                            elapsed = time.time() - start_time
                            if elapsed > 0:
                                throughput = count / elapsed
                                pbar.set_postfix({"throughput": f"{throughput:.0f} rows/s"})
                        for i in dk:
                            #            print(i)
                            k = ".".join(i[:-1])
                            if len(i) == 0:
                                continue
                            if i[0].isdigit():
                                continue
                            if len(i[0]) == 1:
                                continue
                            v = i[-1]
                            if (
                                k not in fielddata
                            ):  # Use direct dict membership check instead of list()
                                fielddata[k] = {
                                    "key": k,
                                    "uniq": {},
                                    "n_uniq": 0,
                                    "total": 0,
                                    "share_uniq": 0.0,
                                    "minlen": None,
                                    "maxlen": 0,
                                    "avglen": 0,
                                    "totallen": 0,
                                }
                            fd = fielddata[k]
                            uniqval = fd["uniq"].get(v, 0)
                            fd["uniq"][v] = uniqval + 1
                            fd["total"] += 1
                            if uniqval == 0:
                                fd["n_uniq"] += 1
                                fd["share_uniq"] = (fd["n_uniq"] * 100.0) / fd["total"]
                            fl = len(str(v))
                            if fd["minlen"] is None:
                                fd["minlen"] = fl
                            else:
                                fd["minlen"] = fl if fl < fd["minlen"] else fd["minlen"]
                            fd["maxlen"] = fl if fl > fd["maxlen"] else fd["maxlen"]
                            fd["totallen"] += fl
                            fielddata[k] = fd
                            if (
                                k not in fieldtypes
                            ):  # Use direct dict membership check instead of list()
                                fieldtypes[k] = {"key": k, "types": {}}
                            fd = fieldtypes[k]
                            thetype = guess_datatype(v, self.qd)["base"]
                            uniqval = fd["types"].get(thetype, 0)
                            fd["types"][thetype] = uniqval + 1
                            fieldtypes[k] = fd
            else:
                for item in iterable_wrapped:
                    count += 1
                    dk = dict_generator(item)
                    if count % 1000 == 0:
                        logging.debug(f"Processing {count} records of {fromfile}")
                    for i in dk:
                        #            print(i)
                        k = ".".join(i[:-1])
                        if len(i) == 0:
                            continue
                        if i[0].isdigit():
                            continue
                        if len(i[0]) == 1:
                            continue
                        v = i[-1]
                        if k not in fielddata:  # Use direct dict membership check instead of list()
                            fielddata[k] = {
                                "key": k,
                                "uniq": {},
                                "n_uniq": 0,
                                "total": 0,
                                "share_uniq": 0.0,
                                "minlen": None,
                                "maxlen": 0,
                                "avglen": 0,
                                "totallen": 0,
                            }
                        fd = fielddata[k]
                        uniqval = fd["uniq"].get(v, 0)
                        fd["uniq"][v] = uniqval + 1
                        fd["total"] += 1
                        if uniqval == 0:
                            fd["n_uniq"] += 1
                            fd["share_uniq"] = (fd["n_uniq"] * 100.0) / fd["total"]
                        fl = len(str(v))
                        if fd["minlen"] is None:
                            fd["minlen"] = fl
                        else:
                            fd["minlen"] = fl if fl < fd["minlen"] else fd["minlen"]
                        fd["maxlen"] = fl if fl > fd["maxlen"] else fd["maxlen"]
                        fd["totallen"] += fl
                        fielddata[k] = fd
                        if (
                            k not in fieldtypes
                        ):  # Use direct dict membership check instead of list()
                            fieldtypes[k] = {"key": k, "types": {}}
                        fd = fieldtypes[k]
                        thetype = guess_datatype(v, self.qd)["base"]
                        uniqval = fd["types"].get(thetype, 0)
                        fd["types"][thetype] = uniqval + 1
                        fieldtypes[k] = fd
        finally:
            iterable.close()
            iterable_context.__exit__(None, None, None)
        #        print count
        for k, v in fielddata.items():  # Use dict.items() directly, no list() conversion
            fielddata[k]["share_uniq"] = (v["n_uniq"] * 100.0) / v["total"]
            fielddata[k]["avglen"] = v["totallen"] / v["total"]
        profile["count"] = count
        profile["num_fields"] = len(fielddata)

        # Determine field types first so we can use them when building dicts
        finfields = {}
        for fd in fieldtypes.values():  # Use dict.values() directly, no list() conversion
            fdt = list(
                fd["types"].keys()
            )  # Keep list() here as we need to check membership and modify
            if "empty" in fdt:
                del fd["types"]["empty"]
            types_keys = list(fd["types"].keys())  # Need list for len() and indexing
            if len(types_keys) != 1:
                ftype = "str"
            else:
                ftype = types_keys[0]
            finfields[fd["key"]] = ftype

        profile["fieldtypes"] = finfields

        dictkeys = []
        dicts = {}
        #        print(profile)
        profile["fields"] = []
        for fd in fielddata.values():  # Use dict.values() directly, no list() conversion
            #            print(fd['key'])  # , fd['n_uniq'], fd['share_uniq'], fieldtypes[fd['key']]
            field = {"key": fd["key"], "is_uniq": 0 if fd["share_uniq"] < 100 else 1}
            profile["fields"].append(field)
            if fd["share_uniq"] < dictshare:
                dictkeys.append(fd["key"])
                # Use determined field type instead of defaulting to 'str'
                field_type = finfields.get(fd["key"], "str")
                dicts[fd["key"]] = {"items": fd["uniq"], "count": fd["n_uniq"], "type": field_type}
        #            for k, v in fd['uniq'].items():
        #                print fd['key'], k, v
        profile["dictkeys"] = dictkeys

        for k, v in fielddata.items():  # Use dict.items() directly, no list() conversion
            del v["uniq"]
            fielddata[k] = v
        profile["debug"] = {"fieldtypes": fieldtypes.copy(), "fielddata": fielddata}

        # Display enhanced statistics table with profiling metrics
        if not get_option(options, "quiet"):
            self._display_enhanced_statistics_table(fielddata, finfields, dictkeys)
        return profile

    def _display_statistics_table(self, fielddata, finfields, dictkeys):
        """Display statistics table using Rich library.

        Args:
            fielddata: Dictionary of field statistics
            finfields: Dictionary mapping field paths to final types
            dictkeys: List of field paths that are dictionary keys
        """

        # Display enhanced statistics table with profiling metrics
        self._display_enhanced_statistics_table(fielddata, finfields, dictkeys)

    def _display_enhanced_statistics_table(self, fielddata, finfields, dictkeys):
        """Display enhanced statistics table with profiling metrics.

        Args:
            fielddata: Dictionary of field statistics
            finfields: Dictionary mapping field paths to final types
            dictkeys: List of field paths that are dictionary keys
        """
        from rich import print
        from rich.table import Table

        table = []
        for fd in fielddata.values():
            field = [
                fd["key"],
            ]
            field.append(finfields.get(fd["key"], "str"))

            # Type category (categorical/numerical)
            type_category = fd.get("type_category", "mixed")
            field.append(type_category)

            # Missing values
            missing_rate = fd.get("missing_rate", 0.0)
            missing_count = fd.get("missing_count", 0)
            field.append(f"{missing_count} ({missing_rate}%)")

            # Cardinality
            cardinality_pct = fd.get("cardinality_pct", fd.get("share_uniq", 0.0))
            field.append(f"{fd.get('n_uniq', 0)} ({cardinality_pct}%)")

            # Distribution stats (for numerical fields)
            if fd.get("is_numerical"):
                mean = fd.get("mean")
                median = fd.get("median")
                if mean is not None and median is not None:
                    field.append(f"μ={mean:.2f}, m={median:.2f}")
                else:
                    field.append("-")
            else:
                field.append("-")

            # Length stats
            field.append(fd.get("minlen", "-"))
            field.append(fd.get("maxlen", "-"))
            field.append(f"{fd.get('avglen', 0.0):.1f}")

            table.append(field)

        headers = (
            "Field",
            "Type",
            "Category",
            "Missing",
            "Cardinality",
            "Distribution",
            "MinLen",
            "MaxLen",
            "AvgLen",
        )
        reptable = Table(title="Dataset Profile")
        reptable.add_column(headers[0], justify="left", style="magenta")
        for key in headers[1:-1]:
            reptable.add_column(key, justify="left", style="cyan", no_wrap=True)
        reptable.add_column(headers[-1], justify="right", style="cyan")
        for row in table:
            reptable.add_row(*map(str, row))
        print(reptable)
