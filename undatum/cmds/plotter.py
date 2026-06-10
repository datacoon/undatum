# -*- coding: utf8 -*-
"""Data plotting module for generating visualizations."""
import logging
import sys
from typing import Optional, List, Dict, Any

try:
    import matplotlib
    matplotlib.use('Agg')  # Use non-interactive backend by default
    import matplotlib.pyplot as plt
    MATPLOTLIB_AVAILABLE = True
except ImportError:
    MATPLOTLIB_AVAILABLE = False
    plt = None

from iterable.helpers.detect import open_iterable
from tqdm import tqdm

from ..common.path_utils import is_s3_uri
from ..common.s3_iterable import open_iterable_with_s3
from ..utils import get_option

logger = logging.getLogger(__name__)


class Plotter:
    """Generate data visualizations from data files."""
    
    def __init__(self):
        if not MATPLOTLIB_AVAILABLE:
            raise ImportError("matplotlib is required for plotting. Install with: pip install matplotlib")
    
    def plot(self, fromfile: str, field: str, plot_type: str = 'auto',
             output: Optional[str] = None, output_format: Optional[str] = None,
             title: Optional[str] = None, xlabel: Optional[str] = None,
             ylabel: Optional[str] = None, width: float = 10, height: float = 6,
             dpi: int = 100, color: Optional[str] = None, **options):
        """Generate a plot from data file.
        
        Args:
            fromfile: Path to input file
            field: Field name(s) to plot (comma-separated for multiple)
            plot_type: Plot type ('histogram', 'bar', 'scatter', 'line', 'auto')
            output: Output file path (None for display)
            output_format: Output format ('png', 'svg', 'pdf', None for auto-detect)
            title: Plot title
            xlabel: X-axis label
            ylabel: Y-axis label
            width: Figure width in inches
            height: Figure height in inches
            dpi: Resolution for raster formats
            color: Color scheme name
            **options: Additional options
        """
        if not MATPLOTLIB_AVAILABLE:
            raise ImportError("matplotlib is required for plotting. Install with: pip install matplotlib")
        
        # Parse field names
        fields = [f.strip() for f in field.split(',')]
        
        # Auto-detect plot type if needed
        if plot_type == 'auto':
            plot_type = self._detect_plot_type(fromfile, fields, options)
        
        # Determine output format
        if output_format is None and output:
            output_format = self._detect_output_format(output)
        elif output_format is None:
            output_format = 'png'  # Default format
        
        # Read data
        data = self._read_data(fromfile, fields, options)
        
        # Generate plot
        if plot_type == 'histogram':
            self._plot_histogram(data, fields, output, output_format, title, xlabel, ylabel, width, height, dpi, color)
        elif plot_type == 'bar':
            self._plot_bar(data, fields, output, output_format, title, xlabel, ylabel, width, height, dpi, color)
        elif plot_type == 'scatter':
            self._plot_scatter(data, fields, output, output_format, title, xlabel, ylabel, width, height, dpi, color)
        elif plot_type == 'line':
            self._plot_line(data, fields, output, output_format, title, xlabel, ylabel, width, height, dpi, color)
        else:
            raise ValueError(f"Unsupported plot type: {plot_type}")
    
    def _detect_plot_type(self, fromfile: str, fields: List[str], options: Dict[str, Any]) -> str:
        """Auto-detect appropriate plot type based on field types."""
        # Sample data to detect types
        try:
            iterable_context = open_iterable_with_s3(fromfile, mode='r', iterableargs={})
            iterable = iterable_context.__enter__()
            try:
                sample = next(iterable)
                if isinstance(sample, dict):
                    # Check field types
                    for field in fields:
                        value = sample.get(field)
                        if value is None:
                            continue
                        # If numeric, suggest histogram
                        if isinstance(value, (int, float)):
                            return 'histogram'
                        # If string/categorical, suggest bar
                        elif isinstance(value, str):
                            return 'bar'
            finally:
                iterable.close()
                iterable_context.__exit__(None, None, None)
        except Exception:
            pass
        
        # Default to histogram
        return 'histogram'
    
    def _detect_output_format(self, output: str) -> str:
        """Detect output format from file extension."""
        ext = output.lower().split('.')[-1]
        if ext in ('png', 'jpg', 'jpeg'):
            return 'png'
        elif ext == 'svg':
            return 'svg'
        elif ext == 'pdf':
            return 'pdf'
        else:
            return 'png'  # Default
    
    def _read_data(self, fromfile: str, fields: List[str], options: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Read data from file for plotting."""
        data = []
        iterable_context = open_iterable_with_s3(fromfile, mode='r', iterableargs={})
        iterable = iterable_context.__enter__()
        
        try:
            for record in tqdm(iterable, desc="Reading data"):
                if isinstance(record, dict):
                    # Extract only requested fields
                    filtered_record = {f: record.get(f) for f in fields}
                    data.append(filtered_record)
                else:
                    # Handle non-dict records
                    if len(fields) == 1 and len(record) > 0:
                        data.append({fields[0]: record[0] if isinstance(record, (list, tuple)) else record})
        finally:
            iterable.close()
            iterable_context.__exit__(None, None, None)
        
        return data
    
    def _plot_histogram(self, data: List[Dict[str, Any]], fields: List[str],
                       output: Optional[str], output_format: str, title: Optional[str],
                       xlabel: Optional[str], ylabel: Optional[str], width: float,
                       height: float, dpi: int, color: Optional[str]):
        """Generate histogram plot."""
        fig, axes = plt.subplots(1, len(fields), figsize=(width, height), squeeze=False)
        axes = axes.flatten()
        
        for idx, field in enumerate(fields):
            ax = axes[idx]
            values = [r.get(field) for r in data if r.get(field) is not None]
            numeric_values = [v for v in values if isinstance(v, (int, float))]
            
            if not numeric_values:
                logger.warning(f"Field {field} has no numeric values, skipping")
                continue
            
            ax.hist(numeric_values, bins=30, color=color, edgecolor='black', alpha=0.7)
            ax.set_xlabel(xlabel or field)
            ax.set_ylabel(ylabel or 'Frequency')
            ax.set_title(title or f'Distribution of {field}')
            ax.grid(True, alpha=0.3)
        
        # Hide unused subplots
        for idx in range(len(fields), len(axes)):
            axes[idx].set_visible(False)
        
        plt.tight_layout()
        self._save_or_show(output, output_format, dpi)
    
    def _plot_bar(self, data: List[Dict[str, Any]], fields: List[str],
                  output: Optional[str], output_format: str, title: Optional[str],
                  xlabel: Optional[str], ylabel: Optional[str], width: float,
                  height: float, dpi: int, color: Optional[str]):
        """Generate bar chart plot."""
        fig, axes = plt.subplots(1, len(fields), figsize=(width, height), squeeze=False)
        axes = axes.flatten()
        
        for idx, field in enumerate(fields):
            ax = axes[idx]
            values = [r.get(field) for r in data if r.get(field) is not None]
            
            # Count frequencies
            from collections import Counter
            counts = Counter(values)
            
            categories = list(counts.keys())
            frequencies = list(counts.values())
            
            # Sort by frequency (descending)
            sorted_pairs = sorted(zip(categories, frequencies), key=lambda x: x[1], reverse=True)
            categories, frequencies = zip(*sorted_pairs) if sorted_pairs else ([], [])
            
            ax.bar(range(len(categories)), frequencies, color=color)
            ax.set_xticks(range(len(categories)))
            ax.set_xticklabels(categories, rotation=45, ha='right')
            ax.set_xlabel(xlabel or field)
            ax.set_ylabel(ylabel or 'Frequency')
            ax.set_title(title or f'Frequency of {field}')
            ax.grid(True, alpha=0.3, axis='y')
        
        # Hide unused subplots
        for idx in range(len(fields), len(axes)):
            axes[idx].set_visible(False)
        
        plt.tight_layout()
        self._save_or_show(output, output_format, dpi)
    
    def _plot_scatter(self, data: List[Dict[str, Any]], fields: List[str],
                     output: Optional[str], output_format: str, title: Optional[str],
                     xlabel: Optional[str], ylabel: Optional[str], width: float,
                     height: float, dpi: int, color: Optional[str]):
        """Generate scatter plot."""
        if len(fields) < 2:
            raise ValueError("Scatter plot requires at least 2 fields")
        
        fig, ax = plt.subplots(1, 1, figsize=(width, height))
        
        x_field = fields[0]
        y_field = fields[1]
        
        x_values = [r.get(x_field) for r in data if r.get(x_field) is not None]
        y_values = [r.get(y_field) for r in data if r.get(y_field) is not None]
        
        # Filter to numeric pairs
        pairs = [(x, y) for x, y in zip(x_values, y_values) if isinstance(x, (int, float)) and isinstance(y, (int, float))]
        if pairs:
            x_vals, y_vals = zip(*pairs)
            ax.scatter(x_vals, y_vals, color=color, alpha=0.6)
        
        ax.set_xlabel(xlabel or x_field)
        ax.set_ylabel(ylabel or y_field)
        ax.set_title(title or f'{x_field} vs {y_field}')
        ax.grid(True, alpha=0.3)
        
        plt.tight_layout()
        self._save_or_show(output, output_format, dpi)
    
    def _plot_line(self, data: List[Dict[str, Any]], fields: List[str],
                  output: Optional[str], output_format: str, title: Optional[str],
                  xlabel: Optional[str], ylabel: Optional[str], width: float,
                  height: float, dpi: int, color: Optional[str]):
        """Generate line plot."""
        fig, ax = plt.subplots(1, 1, figsize=(width, height))
        
        for field in fields:
            values = [r.get(field) for r in data if r.get(field) is not None]
            numeric_values = [v for v in values if isinstance(v, (int, float))]
            
            if numeric_values:
                ax.plot(numeric_values, label=field, color=color, alpha=0.7)
        
        ax.set_xlabel(xlabel or 'Index')
        ax.set_ylabel(ylabel or 'Value')
        ax.set_title(title or 'Line Plot')
        ax.legend()
        ax.grid(True, alpha=0.3)
        
        plt.tight_layout()
        self._save_or_show(output, output_format, dpi)
    
    def _save_or_show(self, output: Optional[str], output_format: str, dpi: int):
        """Save plot to file or display it."""
        if output:
            plt.savefig(output, format=output_format, dpi=dpi, bbox_inches='tight')
            logger.info(f"Plot saved to {output}")
        else:
            # Try to use interactive backend for display
            try:
                matplotlib.use('TkAgg')  # Try TkAgg backend
                plt.show()
            except Exception:
                logger.warning("Could not display plot interactively. Specify --output to save to file.")
                # Save to temporary file as fallback
                import tempfile
                with tempfile.NamedTemporaryFile(suffix='.png', delete=False) as tmp:
                    plt.savefig(tmp.name, format='png', dpi=dpi, bbox_inches='tight')
                    logger.info(f"Plot saved to temporary file: {tmp.name}")
        
        plt.close()
