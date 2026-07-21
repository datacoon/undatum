"""Data selection and filtering module."""

import csv
import logging
import os
import sys
import zipfile

import bson
import orjson

from ..common.command_utils import (
    ITERABLE_OPTIONS_KEYS,
    duckdb_read_expr,
    get_iterable_options,
    quote_sql_identifier,
)  # noqa: F401
from ..common.duckdb_config import create_duckdb_connection, get_duckdb_config_from_options
from ..common.engine_selector import detect_engine
from ..common.errors import (
    FileNotFoundError,
    FormatError,
    PermissionError,
    ValidationError,
    find_similar_files,
)
from ..common.filter import match_filter, translate_filter_to_sql
from ..common.iterable import DataWriter
from ..common.path_utils import validate_file_path
from ..common.s3_iterable import open_path as open_iterable
from ..utils import (
    detect_encoding,
    dict_generator,
    get_dict_value,
    get_file_type,
    get_option,
    normalize_for_json,
    select_fields,
    strip_dict_fields,
)

LINEEND = b"\n"
SELECT_BATCH_SIZE = 1000


class _SelectOutput:
    """Unified output writer for select command (file or stdout)."""

    def __init__(self, to_file, format_out, fields):
        self._to_file = to_file
        self._format_out = format_out
        self._fields = fields
        self._out_iterable = None
        self._stdout_writer = None
        self._stdout_csv_writer = None
        self._stdout_csv_header_written = False

    def write_batch(self, items):
        if not items:
            return
        normalized_items = [normalize_for_json(item) for item in items]
        if self._to_file:
            if self._out_iterable is None:
                to_type = self._format_out or get_file_type(self._to_file)
                output_args = {"keys": self._fields}
                if self._format_out:
                    output_args["format_out"] = self._format_out
                self._out_iterable = open_iterable(
                    self._to_file, mode="w", iterableargs=output_args
                )
            if hasattr(self._out_iterable, "write_bulk"):
                self._out_iterable.write_bulk(normalized_items)
            else:
                for item in normalized_items:
                    self._out_iterable.write(item)
            return

        stdout_type = self._format_out or "jsonl"
        if stdout_type == "csv":
            if self._stdout_csv_writer is None:
                self._stdout_csv_writer = csv.DictWriter(sys.stdout, fieldnames=self._fields)
            if not self._stdout_csv_header_written:
                self._stdout_csv_writer.writeheader()
                self._stdout_csv_header_written = True
            self._stdout_csv_writer.writerows(normalized_items)
        else:
            if self._stdout_writer is None:
                self._stdout_writer = DataWriter(
                    sys.stdout, filetype=stdout_type, fieldnames=self._fields
                )
            self._stdout_writer.write_items(normalized_items)

    def close(self):
        if self._out_iterable is not None:
            self._out_iterable.close()
            self._out_iterable = None


def _has_nested_fields(fields: list[str]) -> bool:
    return any("." in field for field in fields)


def _build_select_query(fields: list[str], source: str, filter_sql: str | None = None) -> str:
    field_sql = ", ".join(quote_sql_identifier(field) for field in fields)
    query = f"SELECT {field_sql} FROM {source}"
    if filter_sql:
        query = f"{query} WHERE {filter_sql}"
    return query


def _duckdb_copy_select(conn, query: str, to_file: str, to_type: str) -> bool:
    """Write select results directly to file via DuckDB COPY. Returns True if handled."""
    escaped = to_file.replace("'", "''")
    if to_type == "csv":
        conn.execute(f"COPY ({query}) TO '{escaped}' (FORMAT CSV, HEADER)")
        return True
    if to_type in ("json", "jsonl"):
        conn.execute(f"COPY ({query}) TO '{escaped}' (FORMAT JSON)")
        return True
    if to_type == "parquet":
        conn.execute(f"COPY ({query}) TO '{escaped}' (FORMAT PARQUET)")
        return True
    return False


def get_iterable_fields_uniq(
    iterable, fields, dolog=False, dq_instance=None
):  # pylint: disable=unused-argument
    """Returns all uniq values of the fields of iterable dictionary."""
    # dq_instance parameter kept for backward compatibility (no longer used)
    n = 0
    uniqval = []
    for row in iterable:
        n += 1
        if dolog and n % 1000 == 0:
            logging.debug("uniq: processing %d records", n)
        try:
            allvals = []
            for field in fields:
                allvals.append(get_dict_value(row, field.split(".")))

            for n1, _ in enumerate(allvals[0]):
                k = []
                for n2, _ in enumerate(allvals):
                    k.append(str(allvals[n2][n1]))
                if k not in uniqval:
                    uniqval.append(k)
        except KeyError:
            pass
    return uniqval


def get_duckdb_fields_uniq(
    filename, fields, filetype=None, duckdb_config=None, dolog=False, dq_instance=None
):  # pylint: disable=unused-argument
    """Returns all uniq values of the fields of the filename using DuckDB."""
    # dq_instance parameter kept for backward compatibility (no longer used)
    if duckdb_config is None:
        duckdb_config = {}

    conn = create_duckdb_connection(**duckdb_config)

    # Determine input format and build appropriate read expression
    source_type = filetype or get_file_type(filename) or "csv"
    if source_type == "csv":
        read_expr = f"read_csv_auto('{filename}', all_varchar=true)"
    elif source_type in ("json", "jsonl"):
        read_expr = f"read_json_auto('{filename}')"
    elif source_type == "parquet":
        read_expr = f"read_parquet('{filename}')"
    else:
        conn.close()
        raise ValueError(f"Unsupported file type for DuckDB: {source_type}")

    fieldstext = ",".join(fields)
    query = f"SELECT DISTINCT {fieldstext} FROM {read_expr}"
    if dolog:
        logging.info(query)

    try:
        relation = conn.execute(query)
        uniqval = relation.fetchall()
        conn.close()
        return uniqval
    except Exception:
        conn.close()
        raise


def get_iterable_fields_freq(
    iterable, fields, dolog=False, filter_expr=None, dq_instance=None
):  # pylint: disable=unused-argument
    """Iterates and returns most frequent values."""
    # dq_instance parameter kept for backward compatibility (no longer used)
    n = 0
    valuedict = {}
    items = []
    for r in iterable:
        n += 1
        if dolog and n % 10000 == 0:
            logging.info("frequency: processing %d records", n)
        if filter_expr is not None:
            if not match_filter(r, filter_expr):
                continue
        try:
            allvals = []
            for field in fields:
                allvals.append(get_dict_value(r, field.split(".")))

            for n1, _ in enumerate(allvals[0]):
                k = []
                for n2, _ in enumerate(allvals):
                    k.append(str(allvals[n2][n1]))
                kx = "\t".join(k)
                v = valuedict.get(kx, 0)
                valuedict[kx] = v + 1
        except KeyError:
            pass
    for k, v in valuedict.items():
        row = k.split("\t")
        row.append(v)
        items.append(row)
    items.sort(key=lambda x: x[-1], reverse=True)
    return items


def get_duckdb_fields_freq(
    filename, fields, filetype=None, duckdb_config=None, dolog=False, dq_instance=None
):  # pylint: disable=unused-argument
    """Returns frequencies for the fields of the filename using DuckDB."""
    # dq_instance parameter kept for backward compatibility (no longer used)
    if duckdb_config is None:
        duckdb_config = {}

    conn = create_duckdb_connection(**duckdb_config)

    # Determine input format and build appropriate read expression
    source_type = filetype or get_file_type(filename) or "csv"
    if source_type == "csv":
        read_expr = f"read_csv_auto('{filename}', all_varchar=true)"
    elif source_type in ("json", "jsonl"):
        read_expr = f"read_json_auto('{filename}')"
    elif source_type == "parquet":
        read_expr = f"read_parquet('{filename}')"
    else:
        conn.close()
        raise ValueError(f"Unsupported file type for DuckDB: {source_type}")

    fieldstext = ",".join(fields)
    query = (
        f"SELECT {fieldstext}, count(*) as c FROM {read_expr} GROUP BY {fieldstext} ORDER BY c DESC"
    )
    if dolog:
        logging.info(query)

    try:
        relation = conn.execute(query)
        uniqval = relation.fetchall()
        conn.close()
        return uniqval
    except Exception:
        conn.close()
        raise


class Selector:
    """Data selection and filtering handler."""

    def __init__(self):
        pass

    def uniq(self, fromfile, options=None):
        """Extracts unique values by field."""
        if options is None:
            options = {}
        logging.debug("Processing %s", fromfile)
        iterableargs = get_iterable_options(options)
        filetype = get_option(options, "filetype")
        to_file = get_option(options, "output")
        engine = get_option(options, "engine")
        if to_file:
            to_type = get_file_type(to_file)
            if not to_type:
                raise FormatError(to_file, to_file.rsplit(".", 1)[-1])
            out = open(to_file, "w", encoding="utf8")
        else:
            to_type = "csv"
            out = sys.stdout
        fields = options["fields"].split(",")
        detected_engine = detect_engine(fromfile, engine, filetype, operation="uniq")
        if detected_engine == "duckdb":
            try:
                duckdb_config = get_duckdb_config_from_options(options)
                output_type = "duckdb"
                uniqval = get_duckdb_fields_uniq(
                    fromfile, fields, filetype=filetype, duckdb_config=duckdb_config, dolog=True
                )
            except Exception as e:
                logging.warning(f"DuckDB uniq failed, falling back to iterable: {e}")
                detected_engine = "iterable"

        if detected_engine == "iterable":
            output_type = "iterable"
            iterable = open_iterable(fromfile, mode="r", iterableargs=iterableargs)
            try:
                logging.info("uniq: looking for fields: {}".format(options["fields"]))
                uniqval = get_iterable_fields_uniq(iterable, fields, dolog=True)
            finally:
                iterable.close()
        else:
            logging.info("Engine not supported. Please choose duckdb or iterable")
            return
        logging.debug(f"{len(uniqval)} unique values found")
        normalized_uniqval = [normalize_for_json(item) for item in uniqval]
        writer = DataWriter(out, filetype=to_type, output_type=output_type, fieldnames=fields)
        writer.write_items(normalized_uniqval)

    def headers(self, fromfile, options=None):
        """Extracts headers values."""
        if options is None:
            options = {}
        limit = get_option(options, "limit")
        iterableargs = get_iterable_options(options)

        iterable = open_iterable(fromfile, mode="r", iterableargs=iterableargs)
        try:
            keys_set = set()  # Use set for O(1) lookup instead of O(n) list operations
            n = 0
            for item in iterable:
                if limit and n > limit:
                    break
                n += 1
                dk = dict_generator(item)
                for i in dk:
                    k = ".".join(i[:-1])
                    keys_set.add(k)
        finally:
            iterable.close()
        keys = list(keys_set)  # Convert to list for backward compatibility
        output = get_option(options, "output")
        if output:
            with open(output, "w", encoding=get_option(options, "encoding")) as f:
                f.write("\n".join(keys))
        else:
            for x in keys:
                print(x.encode("utf8").decode("utf8", "ignore"))

    def frequency(self, fromfile, options=None):
        """Calculates frequency of the values in the file."""
        if options is None:
            options = {}
        logging.debug("Processing %s", fromfile)
        iterableargs = get_iterable_options(options)
        filetype = get_option(options, "filetype")
        to_file = get_option(options, "output")
        engine = get_option(options, "engine")
        if to_file:
            to_type = get_file_type(to_file)
            if not to_type:
                raise FormatError(to_file, to_file.rsplit(".", 1)[-1])
            out = open(to_file, "w", encoding="utf8")
        else:
            to_type = "csv"
            out = sys.stdout
        fields = options["fields"].split(",")
        detected_engine = detect_engine(fromfile, engine, filetype, operation="frequency")
        items = []
        output_type = "iterable"
        if detected_engine == "duckdb":
            try:
                duckdb_config = get_duckdb_config_from_options(options)
                items = get_duckdb_fields_freq(
                    fromfile,
                    fields=fields,
                    filetype=filetype,
                    duckdb_config=duckdb_config,
                    dolog=True,
                )
                output_type = "duckdb"
            except Exception as e:
                logging.warning(f"DuckDB frequency failed, falling back to iterable: {e}")
                detected_engine = "iterable"
        elif detected_engine == "iterable":
            output_type = "iterable"
            iterable = open_iterable(fromfile, mode="r", iterableargs=iterableargs)
            try:
                if iterable is not None:
                    items = get_iterable_fields_freq(iterable, fields, dolog=True)
                else:
                    logging.info("File type not supported")
                    return
            finally:
                iterable.close()
        else:
            logging.debug("Data processing engine is not set and not detected")
            return
        logging.debug(f"frequency: {len(items)} unique values found")
        fields.append("count")
        normalized_items = [normalize_for_json(item) for item in items]
        writer = DataWriter(out, filetype=to_type, output_type=output_type, fieldnames=fields)
        writer.write_items(normalized_items)

    def select(self, fromfile, options=None):
        """Select or re-order columns from file."""
        if options is None:
            options = {}

        # Validate input file exists and is readable
        try:
            validate_file_path(fromfile, check_read=True)
        except FileNotFoundError as e:
            suggestions = find_similar_files(fromfile)
            raise FileNotFoundError(fromfile, suggestions) from e
        except PermissionError as e:
            raise PermissionError(fromfile, operation="read") from e

        iterableargs = get_iterable_options(options)
        to_file = get_option(options, "output")
        format_out = get_option(options, "format_out")
        filetype = get_option(options, "format_in")
        engine = get_option(options, "engine")
        fields_value = get_option(options, "fields")
        if not fields_value:
            raise ValidationError(
                "select requires 'fields' option (comma-separated list of fields)", field="fields"
            )
        fields = [field.strip() for field in fields_value.split(",") if field.strip()]
        if not fields:
            raise ValidationError(
                "select requires at least one field name in 'fields'", field="fields"
            )

        to_type = None
        if to_file:
            to_type = format_out or get_file_type(to_file)
            if not to_type:
                raise FormatError(to_file, to_file.rsplit(".", 1)[-1])

        fields_list = [field.split(".") for field in fields]
        output = _SelectOutput(to_file, format_out, fields)

        filter_expr = get_option(options, "filter")
        detected_engine = detect_engine(fromfile, engine, filetype, operation="select")
        filter_sql = None

        if _has_nested_fields(fields):
            logging.info("select: nested fields require iterable engine")
            detected_engine = "iterable"
        elif detected_engine == "duckdb" and filter_expr:
            filter_sql = translate_filter_to_sql(filter_expr)
            if filter_sql is None:
                logging.info("select: filter not translatable to SQL, falling back to iterable")
                detected_engine = "iterable"

        n = 0
        batch = []
        if detected_engine == "duckdb":
            try:
                duckdb_config = get_duckdb_config_from_options(options)
                conn = create_duckdb_connection(**duckdb_config)
                source = duckdb_read_expr(fromfile, filetype, iterableargs, all_varchar=True)
                query = _build_select_query(fields, source, filter_sql)

                if to_file and _duckdb_copy_select(conn, query, to_file, to_type):
                    conn.close()
                    logging.info("select: completed using DuckDB COPY")
                    return

                relation = conn.execute(query)
                while True:
                    rows = relation.fetchmany(SELECT_BATCH_SIZE)
                    if not rows:
                        break
                    batch = [dict(zip(fields, row)) for row in rows]
                    n += len(batch)
                    output.write_batch(batch)
                conn.close()
            except Exception as exc:
                if n > 0:
                    logging.error("select: DuckDB failed after output (%s)", exc)
                    raise
                logging.warning("select: DuckDB failed (%s), falling back to iterable", exc)
                detected_engine = "iterable"

        if detected_engine == "iterable":
            iterable = open_iterable(fromfile, mode="r", iterableargs=iterableargs)
            try:
                for r in iterable:
                    n += 1
                    if filter_expr is not None:
                        if not match_filter(r, filter_expr):
                            continue
                    r_selected = select_fields(r, fields_list)
                    batch.append(r_selected)
                    if len(batch) >= SELECT_BATCH_SIZE:
                        output.write_batch(batch)
                        batch = []
                if batch:
                    output.write_batch(batch)
            finally:
                iterable.close()

        output.close()

    def split_new(self, fromfile, options=None):
        """Splits the given file with data into chunks based on chunk size or field value."""
        if options is None:
            options = {}
        iterableargs = get_iterable_options(options)
        open_iterable(fromfile, mode="r", iterableargs=iterableargs)
        get_option(options, "output")

    def split(self, fromfile, options=None):
        """Splits the given file with data into chunks based on chunk size or field value."""
        if options is None:
            options = {}
        f_type = get_file_type(fromfile) if options["format_in"] is None else options["format_in"]
        if options["zipfile"]:
            z = zipfile.ZipFile(fromfile, mode="r")
            fnames = z.namelist()
            finfilename = fnames[0]
            if f_type == "bson":
                infile = z.open(fnames[0], "rb")
            else:
                infile = z.open(fnames[0], "r")
        elif options["gzipfile"]:
            import gzip

            infile = gzip.open(fromfile, "rb")
            finfilename = fromfile.split(".", 1)[0] + "." + f_type
        else:
            finfilename = fromfile
            if f_type == "bson":
                infile = open(fromfile, "rb")
            else:
                if "encoding" in options.keys():
                    infile = open(fromfile, encoding=get_option(options, "encoding"))
                else:
                    detected_enc = detect_encoding(fromfile, limit=100000)
                    if detected_enc:
                        infile = open(fromfile, encoding=detected_enc["encoding"])
                    else:
                        infile = open(fromfile, encoding="utf8")
        fields = options["fields"].split(",") if options["fields"] is not None else None
        valuedict = {}
        delimiter = get_option(options, "delimiter")
        if f_type == "csv":
            reader = csv.DictReader(infile, delimiter=delimiter)
            n = 0
            chunknum = 1
            if options["fields"] is None:
                splitname = finfilename.rsplit(".", 1)[0] + f"_{chunknum}.csv"
                out = open(splitname, "w", encoding=get_option(options, "encoding"))
                writer = csv.DictWriter(out, fieldnames=reader.fieldnames, delimiter=delimiter)
                writer.writeheader()
                for r in reader:
                    n += 1
                    if n % 10000 == 0:
                        logging.info(f"split: processing {n} records of {fromfile}")
                    if options["filter"] is not None:
                        if not match_filter(r, options["filter"]):
                            continue
                    writer.writerow(r)
                    if n % options["chunksize"] == 0:
                        out.close()
                        chunknum += 1
                        splitname = finfilename.rsplit(".", 1)[0] + f"_{chunknum}.csv"
                        out = open(splitname, "w", encoding=get_option(options, "encoding"))
                        writer = csv.DictWriter(
                            out, fieldnames=reader.fieldnames, delimiter=delimiter
                        )
                        writer.writeheader()
        elif f_type == "jsonl":
            n = 0
            chunknum = 1
            if options["fields"] is None:
                splitname = finfilename.rsplit(".", 1)[0] + f"_{chunknum}.jsonl"
                out = open(splitname, "wb")  # , encoding=get_option(options, 'encoding'))

                for line in infile:
                    n += 1
                    if n % 10000 == 0:
                        logging.info(f"split: processing {n} records of {fromfile}")
                    r = orjson.loads(line)
                    if options["filter"] is not None:
                        if not match_filter(r, options["filter"]):
                            continue
                    out.write(orjson.dumps(r, option=orjson.OPT_APPEND_NEWLINE))
                    if n % options["chunksize"] == 0:
                        out.close()
                        chunknum += 1
                        splitname = finfilename.rsplit(".", 1)[0] + f"_{chunknum}.jsonl"
                        logging.info(f"split: new chunk {splitname}")
                        out = open(splitname, "wb")  # , encoding=get_option(options, 'encoding'))
            else:
                for line in infile:
                    n += 1
                    if n % 10000 == 0:
                        logging.info(f"split: processing {n} records of {fromfile}")
                    r = orjson.loads(line)
                    if options["filter"] is not None:
                        if not match_filter(r, options["filter"]):
                            continue
                    try:
                        kx = get_dict_value(r, fields[0].split("."))[0]
                    except IndexError:
                        continue
                        kx = "None"
                    if kx is None:
                        continue
                    kx = (
                        kx.replace("\\", "-")
                        .replace("/", "-")
                        .replace("?", "-")
                        .replace("<", "-")
                        .replace(">", "-")
                        .replace("\n", "")
                    )
                    v = valuedict.get(kx, None)
                    if v is None:
                        # splitname = finfilename.rsplit('.', 1)[0] + '_%s.jsonl' % (kx)
                        splitname = f"{kx}.jsonl"
                        if options["dirname"] is not None:
                            splitname = os.path.join(options["dirname"], splitname)
                        valuedict[kx] = open(splitname, "w", encoding="utf8")
                    valuedict[kx].write(line)
                #                    valuedict[kx].write(l.decode('utf8'))#.decode('utf8')#)
                for opened in valuedict.values():
                    opened.close()
        elif f_type == "bson":
            bson_iter = bson.decode_file_iter(infile)
            n = 0
            for r in bson_iter:
                n += 1
                #                print(r)
                strip_dict_fields(r, fields, 0)
                #                out.write(json.dumps(r_selected)+'\n')
                if n % 10000 == 0:
                    logging.info(f"split: processing {n} records of {fromfile}")

        else:
            logging.info("File type not supported")
            return
        logging.debug(f"split: {n} records processed")
