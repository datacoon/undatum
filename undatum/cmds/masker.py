# -*- coding: utf8 -*-
"""Data masking command module."""
import logging
from typing import Optional

from iterable.helpers.detect import open_iterable

from ..common.masking import mask_value
from ..common.path_utils import is_s3_uri, validate_file_path
from ..common.s3_iterable import open_iterable_with_s3
from ..common.errors import FileNotFoundError, PermissionError, ValidationError, find_similar_files
from ..utils import get_option


ITERABLE_OPTIONS_KEYS = ['tagname', 'delimiter', 'encoding', 'start_line', 'page']


def get_iterable_options(options):
    """Extract iterable-specific options from options dictionary."""
    out = {}
    for k in ITERABLE_OPTIONS_KEYS:
        if k in options.keys():
            out[k] = options[k]
    return out


class Masker:
    """Data masking handler for anonymizing sensitive fields."""

    def __init__(self):
        pass

    def mask(self, fromfile: str, tofile: Optional[str], options: Optional[dict] = None):
        """Mask sensitive fields in a data file.

        Args:
            fromfile: Path to input file
            tofile: Path to output file (None for stdout)
            options: Dictionary of options including:
                - fields: Comma-separated list of fields to mask
                - method: Masking method ('redact', 'hash', 'randomize')
                - salt: Optional salt for hashing
                - format_in: Input format override
                - format_out: Output format override
        """
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

        # Parse fields to mask
        fields_str = get_option(options, 'fields')
        if not fields_str:
            raise ValidationError("--fields option is required. Specify fields to mask (e.g., --fields email,phone)", field='fields')
        
        fields_to_mask = [f.strip() for f in fields_str.split(',') if f.strip()]
        if not fields_to_mask:
            raise ValidationError("No valid fields specified for masking", field='fields')

        # Get masking method
        method = get_option(options, 'method') or 'redact'
        if method not in ('redact', 'hash', 'randomize'):
            raise ValidationError(
                f"Invalid masking method: '{method}'. Must be one of: redact, hash, randomize",
                field='method',
                suggestions=['redact', 'hash', 'randomize']
            )

        # Get optional salt for hashing
        salt = get_option(options, 'salt')

        # Get iterable options
        iterableargs = get_iterable_options(options)

        # Determine output format
        format_out = get_option(options, 'format_out')
        if format_out:
            iterableargs['format_out'] = format_out

        logging.info(f'Masking fields: {fields_to_mask} using method: {method}')

        # Open input file (with S3 support)
        if is_s3_uri(fromfile):
            iterable_context = open_iterable_with_s3(fromfile, mode='r', iterableargs=iterableargs)
            it_in = iterable_context.__enter__()
        else:
            it_in = open_iterable(fromfile, mode='r', iterableargs=iterableargs)

        # Open output file
        import os
        if tofile:
            if is_s3_uri(tofile):
                # For S3 output, we'll need to write to temp file first
                import tempfile
                from ..formats.s3 import S3Writer, parse_s3_uri
                suffix = os.path.splitext(parse_s3_uri(tofile)[1])[1] or '.tmp'
                temp_fd, temp_output = tempfile.mkstemp(suffix=suffix)
                os.close(temp_fd)
                s3_writer = S3Writer(tofile)
                s3_writer.__enter__()
            else:
                temp_output = tofile
                s3_writer = None
        else:
            temp_output = None
            s3_writer = None

        try:
            # Determine output keys from first record
            first_record = None
            keys = None
            
            # Read first record to determine schema
            try:
                first_record = next(it_in)
                if isinstance(first_record, dict):
                    keys = list(first_record.keys())
                elif hasattr(it_in, 'reset'):
                    it_in.reset()
            except StopIteration:
                pass

            # Open output iterable
            if temp_output:
                it_out = open_iterable(temp_output, mode='w', iterableargs={'keys': keys} if keys else {})
            else:
                # Write to stdout
                it_out = open_iterable('-', mode='w', iterableargs={'keys': keys} if keys else {})

            try:
                # Process first record if we read it
                if first_record is not None:
                    masked_record = self._mask_record(first_record, fields_to_mask, method, salt)
                    if hasattr(it_out, 'write'):
                        it_out.write(masked_record)
                    else:
                        it_out.write_bulk([masked_record])

                # Process remaining records
                count = 0
                batch = []
                batch_size = 10000

                for record in it_in:
                    masked_record = self._mask_record(record, fields_to_mask, method, salt)
                    batch.append(masked_record)
                    count += 1

                    if len(batch) >= batch_size:
                        if hasattr(it_out, 'write_bulk'):
                            it_out.write_bulk(batch)
                        else:
                            for item in batch:
                                it_out.write(item)
                        batch = []

                    if count % 100000 == 0:
                        logging.info(f'Masked {count} records')

                # Write remaining batch
                if batch:
                    if hasattr(it_out, 'write_bulk'):
                        it_out.write_bulk(batch)
                    else:
                        for item in batch:
                            it_out.write(item)

                logging.info(f'Successfully masked {count + (1 if first_record else 0)} records')

            finally:
                it_out.close()

        finally:
            it_in.close()
            if is_s3_uri(fromfile):
                iterable_context.__exit__(None, None, None)

            # Upload to S3 if needed
            if s3_writer and temp_output:
                import shutil
                with open(temp_output, 'rb') as f:
                    s3_writer.client.upload_fileobj(f, s3_writer.bucket, s3_writer.key)
                try:
                    os.remove(temp_output)
                except OSError:
                    pass
                s3_writer.__exit__(None, None, None)

    def _mask_record(self, record: dict, fields_to_mask: list[str], method: str, salt: Optional[str] = None) -> dict:
        """Mask specified fields in a record.

        Args:
            record: Record dictionary
            fields_to_mask: List of field names to mask
            method: Masking method
            salt: Optional salt for hashing

        Returns:
            Record with masked fields
        """
        masked = dict(record)
        for field in fields_to_mask:
            if field in masked:
                masked[field] = mask_value(masked[field], method, field_name=field, salt=salt)
        return masked
