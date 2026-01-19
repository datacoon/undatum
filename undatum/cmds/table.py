"""Table command module - pretty print data as formatted table."""
import logging

from iterable.helpers.detect import open_iterable
from rich.console import Console
from rich.table import Table

from ..utils import get_option

ITERABLE_OPTIONS_KEYS = ['tagname', 'delimiter', 'encoding', 'start_line', 'page']


def get_iterable_options(options):
    """Extract iterable-specific options from options dictionary."""
    out = {}
    for k in ITERABLE_OPTIONS_KEYS:
        if k in options.keys():
            out[k] = options[k]
    return out


class TableFormatter:
    """Table command handler - pretty print data."""
    def __init__(self):
        self.console = Console()

    def table(self, fromfile, options=None):
        """Display data in a formatted, aligned table."""
        if options is None:
            options = {}
        logging.debug('Processing %s', fromfile)
        iterableargs = get_iterable_options(options)
        limit = get_option(options, 'limit') or 20
        fields = get_option(options, 'fields')

        if fields:
            field_list = [f.strip() for f in fields.split(',')]
        else:
            field_list = None

        # First pass: collect headers and sample data
        iterable = open_iterable(fromfile, mode='r', iterableargs=iterableargs)
        headers = set()
        items = []
        try:
            count = 0
            for item in iterable:
                if count >= limit:
                    break
                if isinstance(item, dict):
                    if field_list:
                        # Filter to selected fields
                        filtered_item = {k: v for k, v in item.items() if k in field_list}
                        items.append(filtered_item)
                        headers.update(filtered_item.keys())
                    else:
                        items.append(item)
                        headers.update(item.keys())
                count += 1
        finally:
            iterable.close()

        if not items:
            print("No data to display")
            return

        # Create table
        table = Table(show_header=True, header_style="bold magenta")

        # Determine columns
        if field_list:
            columns = field_list
        else:
            columns = sorted(headers)

        # Add columns
        for col in columns:
            table.add_column(col, overflow="fold")

        # Add rows
        for item in items:
            row_values = []
            for col in columns:
                value = item.get(col, '')
                # Truncate long values
                str_value = str(value)
                if len(str_value) > 50:
                    str_value = str_value[:47] + '...'
                row_values.append(str_value)
            table.add_row(*row_values)

        # Print table
        self.console.print(table)

        if count >= limit:
            print(f"\n(Showing first {limit} rows)")
