import pdfplumber
import re
import hashlib
from collections import defaultdict


def extract_text_excluding_regions(page, exclude_bboxes):
    """
    Extract text from page excluding certain regions (like tables).
    """
    # Get all text with position information
    chars = page.chars
    
    filtered_chars = []
    for char in chars:
        char_in_table = False
        
        # Check if character is inside any excluded region
        for bbox in exclude_bboxes:
            x0, y0, x1, y1 = bbox
            if (x0 <= char['x0'] <= x1 and 
                y0 <= char['top'] <= y1):
                char_in_table = True
                break
        
        if not char_in_table:
            filtered_chars.append(char)
    
    # Reconstruct text from filtered characters
    if not filtered_chars:
        return ""
    
    # Sort by position (top to bottom, left to right)
    filtered_chars.sort(key=lambda c: (c['top'], c['x0']))
    
    # Group into lines
    lines = []
    current_line = []
    current_top = None
    line_tolerance = 3
    
    for char in filtered_chars:
        if current_top is None or abs(char['top'] - current_top) <= line_tolerance:
            current_line.append(char)
            if current_top is None:
                current_top = char['top']
        else:
            # New line
            lines.append(current_line)
            current_line = [char]
            current_top = char['top']
    
    if current_line:
        lines.append(current_line)
    
    # Convert lines to text
    text_lines = []
    for line in lines:
        line.sort(key=lambda c: c['x0'])
        line_text = ""
        last_x1 = None
        
        for char in line:
            # Add space if there's a gap
            if last_x1 is not None and char['x0'] - last_x1 > char['width'] * 0.3:
                line_text += " "
            line_text += char['text']
            last_x1 = char['x1']
        
        text_lines.append(line_text)
    
    return "\n".join(text_lines)


def extract_page_content_with_tables(page, page_num: int) -> str:
    """
    Extract page content with tables converted to structured text, maintaining proper order.
    Enhanced to preserve line breaks within table cells.
    """
    from .pdf_table_extractor import table_to_text
    
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


def extract_text_from_bbox(page, bbox, preserve_linebreaks=True):
    """
    Extract text from a specific bounding box on the page.
    Preserves line breaks when preserve_linebreaks=True.
    """
    x0, y0, x1, y1 = bbox
    
    # Get all characters in the bbox
    chars = []
    for char in page.chars:
        if (x0 <= char['x0'] <= x1 and 
            y0 <= char['top'] <= y1):
            chars.append(char)
    
    if not chars:
        return ""
    
    # Sort by position (top to bottom, left to right)
    chars.sort(key=lambda c: (c['top'], c['x0']))
    
    # Group into lines
    lines = []
    current_line = []
    current_top = None
    line_tolerance = 3
    
    for char in chars:
        if current_top is None or abs(char['top'] - current_top) <= line_tolerance:
            current_line.append(char)
            if current_top is None:
                current_top = char['top']
        else:
            # New line
            if current_line:
                lines.append(current_line)
            current_line = [char]
            current_top = char['top']
    
    if current_line:
        lines.append(current_line)
    
    # Convert lines to text
    text_lines = []
    for line in lines:
        line.sort(key=lambda c: c['x0'])
        line_text = ""
        last_x1 = None
        
        for char in line:
            # Add space if there's a gap
            if last_x1 is not None and char['x0'] - last_x1 > char['width'] * 0.3:
                line_text += " "
            line_text += char['text']
            last_x1 = char['x1']
        
        text_lines.append(line_text)
    
    # Return with or without line breaks
    if preserve_linebreaks:
        return "\n".join(text_lines)
    else:
        return " ".join(text_lines)


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


def replace_tables_with_text(text: str, page_num: int, text_tables: dict) -> str:
    """
    Replace table regions with structured text tables using precise detection.
    Works only within the current page to avoid cross-page confusion.
    """
    if page_num not in text_tables:
        return text
    
    # Find all table positions in this page
    table_regions = []
    
    for idx, table_info in enumerate(text_tables[page_num]):
        raw_data = table_info['raw_data']
        text_table = table_info['text']
        
        # Find table region using conservative approach
        region = find_minimal_table_region(text, raw_data)
        
        if region:
            table_regions.append({
                'start': region[0],
                'end': region[1],
                'text': text_table,
                'index': idx,
                'raw_data': raw_data
            })
            print(f"✅ Page {page_num}, Table {idx + 1}: found at {region[0]}-{region[1]} ({region[1]-region[0]} chars)")
        else:
            print(f"❌ Page {page_num}, Table {idx + 1}: not found, will append at end")
    
    # Sort by position (process from end to beginning)
    table_regions.sort(key=lambda x: x['start'], reverse=True)
    
    # Replace each table region
    result = text
    replaced_indices = set()
    
    for region in table_regions:
        # Double-check that we're replacing actual table content
        replaced_text = result[region['start']:region['end']]
        if verify_table_content(replaced_text, region['raw_data']):
            result = result[:region['start']] + f"\n{region['text']}\n" + result[region['end']:]
            replaced_indices.add(region['index'])
        else:
            print(f"⚠️ Region doesn't match table content well enough, skipping replacement")
    
    # Append tables that couldn't be found
    for idx, table_info in enumerate(text_tables[page_num]):
        if idx not in replaced_indices:
            result += f"\n\n{table_info['text']}\n\n"
            print(f"⚠️ Page {page_num}, Table {idx + 1}: appended at end")
    
    return result


def find_minimal_table_region(text: str, raw_data: list) -> tuple:
    """
    Find the minimal text region that contains the table.
    Returns (start, end) or None if not found.
    """
    if not raw_data or not raw_data[0]:
        return None
    
    # Find unique markers for table start and end
    start_markers = []
    end_markers = []
    
    # Get first row cells as start markers
    for cell in raw_data[0]:
        if cell and str(cell).strip() and len(str(cell).strip()) > 2:
            start_markers.append(str(cell).strip())
    
    # Get last row cells as end markers
    for cell in raw_data[-1]:
        if cell and str(cell).strip() and len(str(cell).strip()) > 2:
            end_markers.append(str(cell).strip())
    
    if not start_markers or not end_markers:
        return None
    
    # Find the first occurrence of any start marker
    start_pos = len(text)
    start_marker_used = None
    for marker in start_markers:
        pos = text.find(marker)
        if pos != -1 and pos < start_pos:
            start_pos = pos
            start_marker_used = marker
    
    if start_pos == len(text):
        return None
    
    # Find the last occurrence of any end marker after the start position
    end_pos = start_pos
    for marker in end_markers:
        # Search only after the start position to avoid crossing tables
        search_start = start_pos + len(start_marker_used)
        pos = text.rfind(marker, search_start)
        if pos != -1:
            end_pos = max(end_pos, pos + len(marker))
    
    # If end position is too close to start, try to find more cells
    if end_pos - start_pos < 50:  # Tables are usually longer than 50 chars
        # Look for any cell from the table
        for row in raw_data[1:]:  # Skip first row as we already found it
            for cell in row:
                if cell and str(cell).strip():
                    cell_text = str(cell).strip()
                    pos = text.rfind(cell_text, start_pos)
                    if pos != -1:
                        end_pos = max(end_pos, pos + len(cell_text))
    
    # Validate that we found a reasonable region
    if end_pos <= start_pos:
        return None
    
    # Fine-tune boundaries to line breaks if possible
    # Move start back to previous line break if close
    for i in range(max(0, start_pos - 50), start_pos):
        if text[i] == '\n':
            start_pos = i + 1
            break
    
    # Move end forward to next line break if close
    next_newline = text.find('\n', end_pos)
    if next_newline != -1 and next_newline - end_pos < 50:
        end_pos = next_newline
    
    return (start_pos, end_pos)


def verify_table_content(text: str, raw_data: list) -> bool:
    """
    Verify that the text region actually contains table content.
    More strict verification to avoid replacing non-table text.
    """
    if not text or not raw_data:
        return False
    
    # Count cells found in the text
    cells_found = 0
    total_cells = 0
    unique_cells = set()
    
    for row in raw_data:
        for cell in row:
            if cell and str(cell).strip() and len(str(cell).strip()) > 1:
                cell_text = str(cell).strip()
                total_cells += 1
                if cell_text in text and cell_text not in unique_cells:
                    cells_found += 1
                    unique_cells.add(cell_text)
    
    # Require at least 60% of unique cells to be found
    if total_cells == 0:
        return False
    
    cell_ratio = cells_found / total_cells
    if cell_ratio < 0.6:
        print(f"  Cell ratio too low: {cell_ratio:.2f} ({cells_found}/{total_cells} cells)")
        return False
    
    # Check that the text isn't too long relative to table content
    expected_table_length = sum(len(' '.join(str(c) for c in row if c)) for row in raw_data)
    if len(text) > expected_table_length * 3:  # More than 3x expected length is suspicious
        print(f"  Text too long: {len(text)} chars vs expected ~{expected_table_length}")
        return False
    
    # Check line structure - tables typically have multiple short lines
    lines = text.strip().split('\n')
    if len(lines) < len(raw_data) * 0.5:  # Should have at least half as many lines as table rows
        print(f"  Too few lines: {len(lines)} vs {len(raw_data)} table rows")
        return False
    
    return True


def normalize_text(text: str) -> str:
    """
    Normalize text for comparison by removing extra spaces and standardizing.
    """
    # Remove extra spaces and normalize
    text = ' '.join(text.split())
    # Remove zero-width spaces and other invisible characters
    text = text.replace('\u200b', '').replace('\u00a0', ' ')
    return text.strip()


def integrate_text_tables_in_text(text: str, page_num: int, text_tables: dict) -> str:
    """
    Replace table regions with structured text tables.
    This is the main function to use for integrating tables as text.
    """
    return replace_tables_with_text(text, page_num, text_tables)