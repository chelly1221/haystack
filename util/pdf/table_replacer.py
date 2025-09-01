"""
Table replacer module.
Responsible for replacing table regions in text with structured text tables.
"""
import re
import hashlib


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