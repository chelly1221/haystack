"""
Table extractor core module.
Main interface for extracting tables from PDF files.
"""
import pdfplumber
import io
from collections import defaultdict
from .table_formatter import table_to_text, table_to_text_with_positions
from .cell_extractor import extract_table_cells_with_positions, extract_text_from_bbox


def extract_tables_as_text(pdf_path: str, page_num: int = None) -> dict:
    """
    Extract tables from PDF as structured text format for LLM understanding.
    Now extracts cell-level information for better hierarchy analysis.
    
    Args:
        pdf_path: Path to PDF file
        page_num: Specific page number to extract (None for all pages)
    
    Returns:
        Dictionary mapping page numbers to list of text-formatted tables
    """
    text_tables = {}
    
    with pdfplumber.open(pdf_path) as pdf:
        pages_to_process = [pdf.pages[page_num - 1]] if page_num else pdf.pages
        
        for page in pages_to_process:
            page_tables = []
            tables = page.find_tables()
            
            for table_idx, table in enumerate(tables):
                try:
                    # Extract table data with cell positions
                    data = table.extract()
                    
                    if not data or len(data) == 0:
                        continue
                    
                    # Extract detailed cell information
                    cell_data = extract_table_cells_with_positions(table, page)
                    
                    # Convert to text format with cell position awareness
                    text_table = table_to_text_with_positions(data, cell_data, table_idx + 1, page.page_number)
                    
                    # Get table position info
                    bbox = table.bbox
                    
                    page_tables.append({
                        'text': text_table,
                        'bbox': bbox,
                        'table_index': table_idx + 1,
                        'raw_data': data,
                        'cell_data': cell_data
                    })
                    
                except Exception as e:
                    print(f"❌ Error extracting table {table_idx + 1} from page {page.page_number}: {e}")
                    continue
            
            if page_tables:
                text_tables[page.page_number] = page_tables
    
    return text_tables


def extract_table_with_linebreaks(table, page):
    """
    Extract table data while preserving line breaks within cells.
    Uses character-level extraction for better control.
    """
    # Get the basic table structure
    basic_data = table.extract()
    
    if not basic_data:
        return []
    
    # Get table bbox
    table_bbox = table.bbox
    
    # For each cell, extract text with line breaks preserved
    rows = []
    for row_idx, row in enumerate(basic_data):
        new_row = []
        for col_idx, cell in enumerate(row):
            if cell is None:
                new_row.append("")
            else:
                # Get cell bbox if possible
                try:
                    # Extract cell content with line breaks
                    cell_bbox = get_cell_bbox(table, row_idx, col_idx)
                    if cell_bbox:
                        cell_text = extract_text_from_bbox(page, cell_bbox, preserve_linebreaks=True)
                        new_row.append(cell_text)
                    else:
                        # Fallback to original cell content
                        new_row.append(str(cell))
                except:
                    # Fallback to original cell content
                    new_row.append(str(cell))
        rows.append(new_row)
    
    return rows


def get_cell_bbox(table, row_idx, col_idx):
    """
    Get the bounding box of a specific cell in a table.
    """
    try:
        # Get table cells
        cells = table.cells
        
        # Find the cell at the specified position
        for cell in cells:
            if cell[0] == row_idx and cell[1] == col_idx:
                # Return bbox (x0, y0, x1, y1)
                return (cell[2], cell[3], cell[4], cell[5])
        
        return None
    except:
        return None


def extract_page_content_with_tables(page, page_num: int) -> str:
    """
    Extract page content with tables converted to structured text, maintaining proper order.
    Enhanced to preserve line breaks within table cells.
    """
    # Find all tables on the page - simple settings to avoid version issues
    tables = page.find_tables()
    
    if not tables:
        # No tables, just return regular text
        return page.extract_text(x_tolerance=1, y_tolerance=1) or ""
    
    # Get page dimensions
    page_height = float(page.height)
    page_width = float(page.width)
    
    # Create a list of content blocks with their positions
    content_blocks = []
    
    # Add tables as content blocks
    for table_idx, table in enumerate(tables):
        try:
            # Get table data with custom extraction to preserve line breaks
            data = extract_table_with_linebreaks(table, page)
            
            if not data or len(data) == 0:
                continue
            
            bbox = table.bbox  # (x0, y0, x1, y1)
            
            # Convert to structured text
            text_table = table_to_text(data, table_idx + 1, page_num)
            
            content_blocks.append({
                'type': 'table',
                'content': text_table,
                'bbox': bbox,
                'top': bbox[1],  # y0 is top
                'data': data
            })
            
        except Exception as e:
            print(f"❌ Error processing table {table_idx + 1} on page {page_num}: {e}")
            continue
    
    # Extract text excluding table regions
    table_bboxes = [block['bbox'] for block in content_blocks if block['type'] == 'table']
    
    # Get all text with positions
    chars = page.chars
    
    # Group characters into words and lines, excluding table regions
    lines = extract_lines_excluding_regions(chars, table_bboxes)
    
    # Convert lines to content blocks
    for line in lines:
        if line['text'].strip():
            content_blocks.append({
                'type': 'text',
                'content': line['text'],
                'top': line['top'],
                'bbox': (line['x0'], line['top'], line['x1'], line['bottom'])
            })
    
    # Sort all content blocks by vertical position (top to bottom)
    content_blocks.sort(key=lambda x: x['top'])
    
    # Combine all content in order
    result_parts = []
    for block in content_blocks:
        result_parts.append(block['content'])
    
    return '\n'.join(result_parts)


def extract_lines_excluding_regions(chars, exclude_bboxes):
    """
    Extract text lines from characters, excluding certain regions (like tables).
    Returns list of line dictionaries with position info.
    """
    # Filter out characters in excluded regions
    filtered_chars = []
    for char in chars:
        char_in_excluded = False
        
        for bbox in exclude_bboxes:
            x0, y0, x1, y1 = bbox
            # Check if character is inside the excluded region
            if (x0 <= char['x0'] <= x1 and y0 <= char['top'] <= y1):
                char_in_excluded = True
                break
        
        if not char_in_excluded:
            filtered_chars.append(char)
    
    if not filtered_chars:
        return []
    
    # Sort by position (top to bottom, left to right)
    filtered_chars.sort(key=lambda c: (c['top'], c['x0']))
    
    # Group into lines
    lines = []
    current_line_chars = []
    current_top = None
    line_tolerance = 3
    
    for char in filtered_chars:
        if current_top is None or abs(char['top'] - current_top) <= line_tolerance:
            current_line_chars.append(char)
            if current_top is None:
                current_top = char['top']
        else:
            # Process completed line
            if current_line_chars:
                line = process_line_chars(current_line_chars)
                if line:
                    lines.append(line)
            
            # Start new line
            current_line_chars = [char]
            current_top = char['top']
    
    # Process final line
    if current_line_chars:
        line = process_line_chars(current_line_chars)
        if line:
            lines.append(line)
    
    return lines


def process_line_chars(chars):
    """
    Convert list of characters into a line dictionary with text and position.
    """
    if not chars:
        return None
    
    # Sort by x position
    chars.sort(key=lambda c: c['x0'])
    
    # Build line text with proper spacing
    line_text = ""
    last_x1 = None
    
    for char in chars:
        # Add space if there's a gap
        if last_x1 is not None:
            gap = char['x0'] - last_x1
            # Add space if gap is significant (more than 30% of character width)
            if gap > char['width'] * 0.3:
                line_text += " "
        
        line_text += char['text']
        last_x1 = char['x1']
    
    # Calculate line boundaries
    x0 = min(c['x0'] for c in chars)
    x1 = max(c['x1'] for c in chars)
    top = min(c['top'] for c in chars)
    bottom = max(c['bottom'] for c in chars)
    
    return {
        'text': line_text,
        'x0': x0,
        'x1': x1,
        'top': top,
        'bottom': bottom
    }