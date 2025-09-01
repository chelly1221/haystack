# PDF processing module exports

# Import from refactored modules
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

# Import from existing modules (unchanged)
from .pdf_splitter import (
    clean_text_by_fixed_margins,
    clean_text_by_fixed_margins_with_tables,
    split_pdf_by_token_window,
    split_pdf_by_pages,
    split_pdf_by_section_headings
)

from .pdf_image_extractor import (
    extract_images_from_pdf,
    insert_images_in_text
)

from .pdf_text_processor import (
    extract_page_content_with_tables,
    replace_tables_with_text,
    integrate_text_tables_in_text
)

__all__ = [
    # Main functions from pdf_splitter
    'clean_text_by_fixed_margins',
    'clean_text_by_fixed_margins_with_tables',
    'split_pdf_by_token_window',
    'split_pdf_by_pages',
    'split_pdf_by_section_headings',
    
    # Image functions
    'extract_images_from_pdf',
    'insert_images_in_text',
    
    # Table extraction functions (from refactored modules)
    'extract_tables_as_text',
    'extract_table_with_linebreaks',
    'get_cell_bbox',
    
    # Table formatting functions
    'table_to_text',
    'table_to_text_with_positions',
    'preprocess_table_data',
    'is_header_row',
    'is_numeric',
    
    # Table analysis functions
    'analyze_table_structure',
    'analyze_hierarchical_structure',
    'analyze_hierarchical_structure_with_positions',
    'extract_symbol_pattern',
    
    # Cell extraction functions
    'extract_table_cells_with_positions',
    'extract_text_from_bbox',
    'extract_text_excluding_regions',
    
    # Text processing and table replacement functions
    'extract_page_content_with_tables',
    'replace_tables_with_text',
    'integrate_text_tables_in_text',
    'find_minimal_table_region',
    'verify_table_content',
    'normalize_text'
]