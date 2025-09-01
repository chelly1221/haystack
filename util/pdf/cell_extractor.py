"""
Cell extractor module.
Responsible for extracting cell-level information from tables.
"""
from collections import defaultdict


def extract_table_cells_with_positions(table, page):
    """
    Extract detailed cell information including positions and content.
    """
    cell_info = []
    
    try:
        # Get the table cells with their positions
        cells = table.cells
        
        # Group cells by row and column
        cell_dict = {}
        for cell in cells:
            row_idx, col_idx, x0, y0, x1, y1 = cell
            
            # Extract text from this specific cell area
            cell_bbox = (x0, y0, x1, y1)
            cell_text = extract_text_from_bbox(page, cell_bbox, preserve_linebreaks=True)
            
            if (row_idx, col_idx) not in cell_dict:
                cell_dict[(row_idx, col_idx)] = {
                    'row': row_idx,
                    'col': col_idx,
                    'x0': x0,
                    'y0': y0,
                    'x1': x1,
                    'y1': y1,
                    'text': cell_text,
                    'bbox': cell_bbox
                }
        
        # Convert to list sorted by row and column
        for key in sorted(cell_dict.keys()):
            cell_info.append(cell_dict[key])
            
    except Exception as e:
        print(f"Error extracting cell positions: {e}")
        
    return cell_info


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