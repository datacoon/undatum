"""Sniff command module - detect file properties."""
import json
import logging

from iterable.helpers.detect import detect_file_type, open_iterable

from ..utils import get_option

ITERABLE_OPTIONS_KEYS = ['tagname', 'delimiter', 'encoding', 'start_line', 'page']


def get_iterable_options(options):
    """Extract iterable-specific options from options dictionary."""
    out = {}
    for k in ITERABLE_OPTIONS_KEYS:
        if k in options.keys():
            out[k] = options[k]
    return out


class Sniffer:
    """Sniffer command handler - detect file properties."""
    def __init__(self):
        pass

    def sniff(self, fromfile, options=None):
        """Detect file properties (delimiter, encoding, types, record count)."""
        if options is None:
            options = {}
        logging.debug('Sniffing %s', fromfile)
        format_type = get_option(options, 'format') or 'text'

        # Detect file type and encoding
        ftype = detect_file_type(fromfile)
        filetype = 'unknown'
        encoding = 'unknown'
        delimiter = None

        if ftype['success']:
            filetype = ftype['datatype'].id()
            if ftype['codec'] is not None:
                encoding = ftype['codec'].id()

        # Get delimiter from options or try to detect
        iterableargs = get_iterable_options(options)
        if 'delimiter' in iterableargs and iterableargs['delimiter']:
            delimiter = iterableargs['delimiter']

        # Sample first few items to detect types and headers
        iterable = open_iterable(fromfile, mode='r', iterableargs=iterableargs)
        sample_items = []
        field_types = {}
        has_header = False

        try:
            count = 0
            for item in iterable:
                if isinstance(item, dict):
                    sample_items.append(item)
                    count += 1

                    # Detect field types from sample
                    for field, value in item.items():
                        if field not in field_types:
                            field_types[field] = {
                                'type': 'string',
                                'examples': []
                            }

                        if value is not None:
                            if isinstance(value, bool):
                                field_types[field]['type'] = 'boolean'
                            elif isinstance(value, int):
                                if field_types[field]['type'] == 'string':
                                    field_types[field]['type'] = 'integer'
                            elif isinstance(value, float):
                                field_types[field]['type'] = 'number'
                            elif isinstance(value, (list, dict)):
                                field_types[field]['type'] = 'object'

                            if len(field_types[field]['examples']) < 3:
                                field_types[field]['examples'].append(str(value)[:50])

                    # Sample first 100 rows for type detection
                    if count >= 100:
                        break
        except Exception as e:
            logging.warning(f'sniff: error sampling data: {e}')
        finally:
            iterable.close()

        # Estimate total record count (read through once)
        iterable = open_iterable(fromfile, mode='r', iterableargs=iterableargs)
        total_count = 0
        try:
            for _ in iterable:
                total_count += 1
                if total_count % 100000 == 0:
                    logging.debug('sniff: counting records... %d', total_count)
        finally:
            iterable.close()

        # Build result
        result = {
            'file': fromfile,
            'filetype': filetype,
            'encoding': encoding,
            'delimiter': delimiter or 'N/A',
            'has_header': has_header,
            'record_count': total_count,
            'fields': field_types,
            'sample_size': len(sample_items)
        }

        # Format output
        if format_type == 'json':
            output_text = json.dumps(result, indent=2, default=str)
        elif format_type == 'yaml':
            try:
                import yaml
                output_text = yaml.dump(result, default_flow_style=False)
            except ImportError:
                logging.warning('yaml library not available, falling back to JSON')
                output_text = json.dumps(result, indent=2, default=str)
        else:
            # Text format (default)
            lines = []
            lines.append(f"File: {fromfile}")
            lines.append(f"Type: {filetype}")
            lines.append(f"Encoding: {encoding}")
            if delimiter:
                lines.append(f"Delimiter: {delimiter}")
            lines.append(f"Records: {total_count}")
            lines.append(f"\nFields ({len(field_types)}):")
            for field, info in sorted(field_types.items()):
                lines.append(f"  {field}: {info['type']}")
                if info['examples']:
                    lines.append(f"    Examples: {', '.join(info['examples'][:3])}")
            output_text = '\n'.join(lines)

        to_file = get_option(options, 'output')
        if to_file:
            out = open(to_file, 'w', encoding='utf8')
            out.write(output_text)
            out.write('\n')
            out.close()
        else:
            print(output_text)

        logging.debug('sniff: detected %d records, %d fields', total_count, len(field_types))
