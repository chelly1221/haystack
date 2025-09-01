"""
PDF Table Extractor main module.
This is the main entry point that imports from the refactored modules.
"""

# Import all functions from refactored modules
from .table_extractor import (
    extract_tables_as_text,
    extract_page_content_with_tables,
    extract_table_with_linebreaks,
    get_cell_bbox
)

from .table_formatter import (
    table_to_text,
    table_to_text_with_positions,
    preprocess_table_data,
    is_header_row,
    is_numeric
)

from .table_analyzer import (
    analyze_table_structure,
    analyze_hierarchical_structure,
    analyze_hierarchical_structure_with_positions,
    extract_symbol_pattern
)

from .cell_extractor import (
    extract_table_cells_with_positions,
    extract_text_from_bbox,
    extract_text_excluding_regions
)

from .table_replacer import (
    replace_tables_with_text,
    integrate_text_tables_in_text,
    find_minimal_table_region,
    verify_table_content,
    normalize_text
)

# Export all functions for backward compatibility
__all__ = [
    # Main extraction functions
    'extract_tables_as_text',
    'extract_page_content_with_tables',
    'extract_table_with_linebreaks',
    'get_cell_bbox',
    
    # Formatting functions
    'table_to_text',
    'table_to_text_with_positions',
    'preprocess_table_data',
    'is_header_row',
    'is_numeric',
    
    # Analysis functions
    'analyze_table_structure',
    'analyze_hierarchical_structure',
    'analyze_hierarchical_structure_with_positions',
    'extract_symbol_pattern',
    
    # Cell extraction functions
    'extract_table_cells_with_positions',
    'extract_text_from_bbox',
    'extract_text_excluding_regions',
    
    # Table replacement functions
    'replace_tables_with_text',
    'integrate_text_tables_in_text',
    'find_minimal_table_region',
    'verify_table_content',
    'normalize_text'
]