"""Data validation module."""
import csv
import json
import logging
import sys
import zipfile
from collections import defaultdict

import bson
import orjson

from ..common.filter import match_filter
from ..common.s3_iterable import open_iterable_with_s3
from ..common.validation_rules import ValidationRuleError, parse_validation_rules
from ..utils import get_dict_value, get_file_type, get_option
from ..validate import VALIDATION_RULEMAP


class Validator:
    """Data validation handler."""
    def __init__(self):
        pass

    def validate(self, fromfile, options=None):
        """Validates data against validation rules.
        
        Supports two modes:
        1. Rule file mode: Use --rules option with YAML/JSON rule file
        2. Legacy CLI mode: Use --fields and --rule options (backward compatible)
        """
        from ..common.errors import FileNotFoundError, PermissionError, find_similar_files
        from ..common.path_utils import validate_file_path
        
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
        
        logging.debug('Processing %s', fromfile)
        
        # Check if using rule file mode
        rules_file = get_option(options, 'rules')
        if rules_file:
            self._validate_with_rules(fromfile, options, rules_file)
        else:
            self._validate_legacy(fromfile, options)
    
    def _validate_with_rules(self, fromfile, options, rules_file):
        """Validate using rule file."""
        try:
            rule_set = parse_validation_rules(rules_file)
        except ValidationRuleError as e:
            logging.error(f"Failed to parse rule file: {e}")
            raise
        
        # Process records and collect violations
        all_violations = []
        total_records = 0
        
        format_in = get_option(options, 'format_in')
        f_type = get_file_type(fromfile) if format_in is None else format_in
        
        # Use iterable for consistent record processing
        from iterable.helpers.detect import open_iterable
        from ..common.path_utils import is_s3_uri
        
        iterableargs = {}
        if is_s3_uri(fromfile):
            iterable_context = open_iterable_with_s3(fromfile, mode='r', iterableargs=iterableargs)
            it_in = iterable_context.__enter__()
            use_context = True
        else:
            it_in = open_iterable(fromfile, mode='r', iterableargs=iterableargs)
            use_context = False
        
        try:
            for record_index, record in enumerate(it_in):
                total_records += 1
                filter_expr = options.get('filter')
                if filter_expr:
                    if not match_filter(record, filter_expr):
                        continue
                
                violations = rule_set.validate_record(record, record_index)
                all_violations.extend(violations)
        finally:
            if use_context:
                iterable_context.__exit__(None, None, None)
        
        # Generate reports
        self._generate_validation_report(all_violations, total_records, options)
    
    def _validate_legacy(self, fromfile, options):
        """Legacy validation mode (backward compatible)."""
        format_in = get_option(options, 'format_in')
        f_type = get_file_type(fromfile) if format_in is None else format_in
        zipfile_enabled = options.get('zipfile', False)
        if zipfile_enabled:
            z = zipfile.ZipFile(fromfile, mode='r')
            fnames = z.namelist()
            if f_type == 'bson':
                infile = z.open(fnames[0], 'rb')
            else:
                infile = z.open(fnames[0], 'r')
        else:
            if f_type == 'bson':
                infile = open(fromfile, 'rb')
            else:
                infile = open(fromfile, encoding=get_option(options, 'encoding'))
        to_file = get_option(options, 'output')
        if to_file:
            get_file_type(to_file)
            if not to_file:
                logging.debug('Output file type not supported')
                return
            out = open(to_file, 'w', encoding='utf8')
        else:
            out = sys.stdout
        fields_value = get_option(options, 'fields')
        if not fields_value:
            raise ValueError("validate requires 'fields' option (comma-separated list of fields)")
        fields = fields_value.split(',')
        rule = get_option(options, 'rule')
        if not rule:
            raise ValueError("validate requires 'rule' option")
        val_func = VALIDATION_RULEMAP[rule]
        logging.info('uniq: looking for fields: %s', fields_value)
        validated = []
        stats = {'total': 0, 'invalid': 0, 'novalue' : 0}
        if f_type == 'csv':
            delimiter = get_option(options, 'delimiter')
            reader = csv.DictReader(infile, delimiter=delimiter)
            n = 0
            for r in reader:
                n += 1
                if n % 1000 == 0:
                    logging.info('uniq: processing %d records of %s', n, fromfile)
                filter_expr = options.get('filter')
                if filter_expr is not None:
                    if not match_filter(r, filter_expr):
                        continue
                res = val_func(r[fields[0]])
                stats['total'] += 1
                if not res:
                    stats['invalid'] += 1
                validated.append({fields[0] : r[fields[0]], fields[0] + '_valid' : res})

        elif f_type == 'jsonl':
            n = 0
            for l in infile:
                n += 1
                if n % 10000 == 0:
                    logging.info('uniq: processing %d records of %s', n, fromfile)
                r = orjson.loads(l)
                filter_expr = options.get('filter')
                if filter_expr is not None:
                    if not match_filter(r, filter_expr):
                        continue
                stats['total'] += 1
                values = get_dict_value(r, fields[0].split('.'))
                if len(values) > 0:
                    res = val_func(values[0])
                    if not res:
                        stats['invalid'] += 1
                    validated.append({fields[0] : values[0], fields[0] + '_valid' : res})
                else:
                    stats['novalue'] += 1

        elif f_type == 'bson':
            bson_iter = bson.decode_file_iter(infile)
            n = 0
            for r in bson_iter:
                n += 1
                if n % 1000 == 0:
                    logging.info('uniq: processing %d records of %s', n, fromfile)
                filter_expr = options.get('filter')
                if filter_expr is not None:
                    if not match_filter(r, filter_expr):
                        continue
                stats['total'] += 1
                values = get_dict_value(r, fields[0].split('.'))
                if len(values) > 0:
                    res = val_func(values[0])
                    if not res:
                        stats['invalid'] += 1
                    validated.append({fields[0] : values[0], fields[0] + '_valid' : res})
                else:
                    stats['novalue'] += 1
        else:
            logging.error('Invalid filed format provided')
            if not zipfile_enabled:
                infile.close()
            return
        if not zipfile_enabled:
            infile.close()
        stats['share'] = 100.0 * stats['invalid'] / stats['total'] if stats['total'] > 0 else 0
        novalue_share = 100.0 * stats['novalue'] / stats['total'] if stats['total'] > 0 else 0
        logging.debug('validate: complete, %d records (%.2f%%) not valid and %d '
                     '(%.2f%%) not found of %d against %s',
                     stats['invalid'], stats['share'], stats['novalue'],
                     novalue_share, stats['total'], rule)
        mode = options.get('mode', 'stats')
        if mode != 'stats':
            fieldnames = [fields[0], fields[0] + '_valid']
            writer = csv.DictWriter(out, fieldnames=fieldnames,
                                    delimiter=get_option(options, 'delimiter'))
            for row in validated:
                if mode == 'invalid':
                    if not row[fields[0] + '_valid']:
                        writer.writerow(row)
                elif mode == 'all':
                    writer.writerow(row)
        else:
            out.write(str(orjson.dumps(stats, option=orjson.OPT_INDENT_2)))
        if to_file:
            out.close()
        if options.get('zipfile'):
            z.close()
    
    def _generate_validation_report(self, violations, total_records, options):
        """Generate validation report from violations.
        
        Args:
            violations: List of violation dictionaries
            total_records: Total number of records processed
            options: Validation options
        """
        output_format = get_option(options, 'output_format') or 'text'
        severity_filter = get_option(options, 'severity') or 'all'
        violation_report_file = get_option(options, 'violation_report')
        
        # Filter violations by severity
        if severity_filter != 'all':
            violations = [v for v in violations if v['severity'] == severity_filter]
        
        # Calculate statistics
        stats = {
            'total_records': total_records,
            'total_violations': len(violations),
            'errors': len([v for v in violations if v['severity'] == 'error']),
            'warnings': len([v for v in violations if v['severity'] == 'warning']),
            'info': len([v for v in violations if v['severity'] == 'info']),
            'passed': total_records - len(set(v['record_index'] for v in violations))
        }
        
        # Group violations by field and rule
        violations_by_field = defaultdict(list)
        violations_by_rule = defaultdict(list)
        for v in violations:
            if v['field']:
                violations_by_field[v['field']].append(v)
            violations_by_rule[v['rule_name']].append(v)
        
        # Generate report
        if output_format == 'json':
            self._generate_json_report(violations, stats, violations_by_field, violations_by_rule, options)
        else:
            self._generate_text_report(violations, stats, violations_by_field, violations_by_rule, options)
        
        # Write detailed violation report if requested
        if violation_report_file:
            with open(violation_report_file, 'w', encoding='utf-8') as f:
                json.dump({
                    'statistics': stats,
                    'violations': violations,
                    'violations_by_field': {k: len(v) for k, v in violations_by_field.items()},
                    'violations_by_rule': {k: len(v) for k, v in violations_by_rule.items()}
                }, f, indent=2, default=str)
    
    def _generate_text_report(self, violations, stats, violations_by_field, violations_by_rule, options):
        """Generate text format validation report."""
        from rich import print
        from rich.table import Table
        from rich.console import Console
        
        console = Console()
        
        # Summary table
        summary_table = Table(title="Validation Summary", show_header=True, header_style="bold magenta")
        summary_table.add_column("Metric", style="cyan")
        summary_table.add_column("Value", style="green", justify="right")
        summary_table.add_column("Percentage", style="yellow", justify="right")
        
        summary_table.add_row("Total Records", str(stats['total_records']), "100.0%")
        summary_table.add_row("Errors", str(stats['errors']), 
                            f"{stats['errors']/stats['total_records']*100:.2f}%" if stats['total_records'] > 0 else "0.0%")
        summary_table.add_row("Warnings", str(stats['warnings']),
                            f"{stats['warnings']/stats['total_records']*100:.2f}%" if stats['total_records'] > 0 else "0.0%")
        summary_table.add_row("Info", str(stats['info']),
                            f"{stats['info']/stats['total_records']*100:.2f}%" if stats['total_records'] > 0 else "0.0%")
        summary_table.add_row("Passed", str(stats['passed']),
                            f"{stats['passed']/stats['total_records']*100:.2f}%" if stats['total_records'] > 0 else "0.0%")
        
        console.print(summary_table)
        console.print()
        
        # Violations by field
        if violations_by_field:
            field_table = Table(title="Violations by Field", show_header=True, header_style="bold magenta")
            field_table.add_column("Field", style="cyan")
            field_table.add_column("Total", style="green", justify="right")
            field_table.add_column("Errors", style="red", justify="right")
            field_table.add_column("Warnings", style="yellow", justify="right")
            
            for field, field_violations in sorted(violations_by_field.items(), key=lambda x: len(x[1]), reverse=True):
                errors = len([v for v in field_violations if v['severity'] == 'error'])
                warnings = len([v for v in field_violations if v['severity'] == 'warning'])
                field_table.add_row(field, str(len(field_violations)), str(errors), str(warnings))
            
            console.print(field_table)
            console.print()
        
        # Show sample violations
        if violations:
            max_violations = options.get('max_violations', 10)
            sample_violations = violations[:max_violations]
            
            violations_table = Table(title=f"Sample Violations (showing {len(sample_violations)} of {len(violations)})", 
                                   show_header=True, header_style="bold magenta")
            violations_table.add_column("Record", style="cyan", justify="right")
            violations_table.add_column("Field", style="yellow")
            violations_table.add_column("Severity", style="red")
            violations_table.add_column("Message", style="white")
            
            for v in sample_violations:
                severity_style = {'error': 'red', 'warning': 'yellow', 'info': 'blue'}.get(v['severity'], 'white')
                violations_table.add_row(
                    str(v['record_index']),
                    v['field'] or 'cross-field',
                    f"[{severity_style}]{v['severity']}[/{severity_style}]",
                    v['message']
                )
            
            console.print(violations_table)
            
            if len(violations) > max_violations:
                console.print(f"\n[dim]... and {len(violations) - max_violations} more violations. Use --violation-report to see all.[/dim]")
        
        # Exit code based on failures
        fail_on_warnings = options.get('fail_on_warnings', False)
        if stats['errors'] > 0 or (fail_on_warnings and stats['warnings'] > 0):
            sys.exit(1)
    
    def _generate_json_report(self, violations, stats, violations_by_field, violations_by_rule, options):
        """Generate JSON format validation report."""
        report = {
            'statistics': stats,
            'violations_by_field': {k: len(v) for k, v in violations_by_field.items()},
            'violations_by_rule': {k: len(v) for k, v in violations_by_rule.items()},
            'violations': violations[:options.get('max_violations', 100)]  # Limit for JSON output
        }
        
        print(orjson.dumps(report, option=orjson.OPT_INDENT_2).decode('utf-8'))
        
        # Exit code based on failures
        fail_on_warnings = options.get('fail_on_warnings', False)
        if stats['errors'] > 0 or (fail_on_warnings and stats['warnings'] > 0):
            sys.exit(1)
