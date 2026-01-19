"""Diff command module - compare two files and show differences."""
import json
import logging

from iterable.helpers.detect import open_iterable

from ..utils import get_option

ITERABLE_OPTIONS_KEYS = ['tagname', 'delimiter', 'encoding', 'start_line', 'page']


def get_iterable_options(options):
    """Extract iterable-specific options from options dictionary."""
    out = {}
    for k in ITERABLE_OPTIONS_KEYS:
        if k in options.keys():
            out[k] = options[k]
    return out


def _get_key_value(item, key_fields):
    """Get key value for comparison."""
    if not key_fields:
        # Use all fields as key
        return tuple(sorted((k, v) for k, v in item.items() if v is not None))
    else:
        # Use specified key fields
        return tuple(item.get(field) for field in key_fields)


class Differ:
    """Differ command handler - compare two files."""
    def __init__(self):
        pass

    def diff(self, file1, file2, options=None):
        """Compare two files and show differences."""
        if options is None:
            options = {}
        logging.debug('Comparing %s and %s', file1, file2)

        key_fields = get_option(options, 'key')
        format_type = get_option(options, 'format') or 'json'
        to_file = get_option(options, 'output')

        key_field_list = None
        if key_fields:
            key_field_list = [f.strip() for f in key_fields.split(',')]

        iterableargs = get_iterable_options(options)

        # Load file1 into dictionary by key
        iterable1 = open_iterable(file1, mode='r', iterableargs=iterableargs)
        file1_items = {}

        try:
            count1 = 0
            for item in iterable1:
                count1 += 1
                if isinstance(item, dict):
                    key = _get_key_value(item, key_field_list)
                    file1_items[key] = item
        finally:
            iterable1.close()

        # Load file2 into dictionary by key
        iterable2 = open_iterable(file2, mode='r', iterableargs=iterableargs)
        file2_items = {}

        try:
            count2 = 0
            for item in iterable2:
                count2 += 1
                if isinstance(item, dict):
                    key = _get_key_value(item, key_field_list)
                    file2_items[key] = item
        finally:
            iterable2.close()

        # Find differences
        added = []  # In file2 but not in file1
        removed = []  # In file1 but not in file2
        changed = []  # Same key but different values

        for key, item2 in file2_items.items():
            if key not in file1_items:
                added.append(item2)
            else:
                item1 = file1_items[key]
                if item1 != item2:
                    changed.append({'key': key, 'old': item1, 'new': item2})

        for key, item1 in file1_items.items():
            if key not in file2_items:
                removed.append(item1)

        # Format output
        result = {
            'added': added,
            'removed': removed,
            'changed': changed,
            'summary': {
                'file1_count': count1,
                'file2_count': count2,
                'added_count': len(added),
                'removed_count': len(removed),
                'changed_count': len(changed)
            }
        }

        if format_type == 'unified':
            # Unified diff format (simplified)
            lines = []
            lines.append(f"--- {file1}")
            lines.append(f"+++ {file2}")
            lines.append("@@ Summary @@")
            lines.append(f"- Removed: {len(removed)}")
            lines.append(f"+ Added: {len(added)}")
            lines.append(f"~ Changed: {len(changed)}")
            if removed:
                lines.append("\n=== Removed ===")
                for item in removed[:10]:  # Limit to first 10
                    lines.append(f"- {item}")
            if added:
                lines.append("\n=== Added ===")
                for item in added[:10]:  # Limit to first 10
                    lines.append(f"+ {item}")
            if changed:
                lines.append("\n=== Changed ===")
                for change in changed[:10]:  # Limit to first 10
                    lines.append(f"~ {change['key']}")
                    lines.append(f"  Old: {change['old']}")
                    lines.append(f"  New: {change['new']}")
            output_text = '\n'.join(lines)
        else:
            # JSON format (default)
            output_text = json.dumps(result, indent=2, default=str)

        if to_file:
            out = open(to_file, 'w', encoding='utf8')
            out.write(output_text)
            out.write('\n')
            out.close()
        else:
            print(output_text)

        logging.debug('diff: file1=%d rows, file2=%d rows, added=%d, removed=%d, changed=%d',
                     count1, count2, len(added), len(removed), len(changed))
