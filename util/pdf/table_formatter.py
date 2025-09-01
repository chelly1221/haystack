"""
Table formatter module.
Responsible for converting table data to structured text format.
"""
import re
from .table_analyzer import (
    analyze_table_structure, 
    analyze_hierarchical_structure_with_positions,
    is_same_or_higher_level,
    extract_symbol_pattern  # 추가
)


def table_to_text(data: list, table_num: int, page_num: int) -> str:
    """
    Convert table data to structured text format that LLMs can easily understand.
    Backward compatibility function that calls the enhanced version.
    """
    return table_to_text_with_positions(data, [], table_num, page_num)


def table_to_text_with_positions(data: list, cell_data: list, table_num: int, page_num: int) -> str:
    """
    Convert table data to structured text format with cell position awareness.
    Enhanced to handle multi-line cells and complex table structures.
    """
    if not data:
        return ""
    
    # DEBUG: Print raw table data
    print(f"\n{'='*60}")
    print(f"🔍 디버깅: 표 {table_num} - 페이지 {page_num}")
    print(f"{'='*60}")
    print(f"원본 데이터 (총 {len(data)}행):")
    for idx, row in enumerate(data):
        print(f"행 {idx}: {row}")
        # Debug first cell specifically
        if row and row[0]:
            print(f"  첫 번째 셀 (repr): {repr(row[0])}")
            print(f"  strip 후: {repr(str(row[0]).strip())}")
    print(f"{'='*60}\n")
    
    # Preprocess data to handle multi-line cells
    processed_data = preprocess_table_data(data)
    
    # Build text representation
    text_parts = []
    text_parts.append(f"[표 {table_num} - 페이지 {page_num} 시작]")
    
    # Check if first row should be header using similarity analysis
    first_row_is_header = analyze_header_with_similarity(processed_data) if len(processed_data) >= 3 else (is_header_row(processed_data[0]) if processed_data else False)
    
    if first_row_is_header and len(processed_data) > 1:
        # Extract headers
        headers = processed_data[0]
        data_rows = processed_data[1:]
        
        print(f"헤더 감지됨: {headers}")
        print(f"데이터 행 수: {len(data_rows)}")
        
        # Analyze table structure
        table_type = analyze_table_type(headers, data_rows)
        print(f"테이블 유형: {table_type}")
        
        if table_type == 'complex_hierarchical':
            # Process complex hierarchical table
            text_parts.extend(process_complex_hierarchical_table(headers, data_rows))
        elif table_type == 'simple_multicolumn':
            # Process as simple multi-column table
            text_parts.extend(process_simple_multicolumn_table(headers, data_rows))
        else:
            # Default processing
            text_parts.extend(process_simple_table(headers, data_rows))
        
    else:
        # No headers detected
        text_parts.extend(process_table_without_headers(processed_data))
    
    text_parts.append(f"[표 {table_num} - 페이지 {page_num} 끝]")
    
    # Improved debug output
    print(f"\n최종 출력:")
    print(f"전체 text_parts 길이: {len(text_parts)}")
    
    # Join with newlines first for debugging
    final_text_debug = '\n'.join(text_parts)
    print(f"최종 텍스트 길이: {len(final_text_debug)} 문자")
    
    # Print first 1000 characters to verify content
    print(f"\n최종 텍스트 미리보기 (처음 1000자):")
    print(final_text_debug[:1000])
    print("...")
    
    # Convert newlines to <br> for final output
    final_text = '<br>'.join(text_parts)
    
    return final_text


def analyze_table_type(headers, data_rows):
    """
    Analyze table structure to determine the best processing method.
    """
    if not data_rows:
        return 'empty'
    
    # Check for specific header patterns
    has_checkpoint_headers = any('하한치' in str(h) or '표준치' in str(h) or '상한치' in str(h) for h in headers)
    
    # Check for complex multi-line patterns in first column
    complex_multiline = False
    for row in data_rows:
        if row and row[0]:
            first_cell = str(row[0]).strip()
            # Check if cell has multiple lines
            if '\n' in first_cell:
                complex_multiline = True
                break
    
    # Also check for hierarchical patterns even without multiline
    has_hierarchical_content = False
    for row in data_rows:
        if row and row[0]:
            first_cell = str(row[0]).strip()
            # Check for section numbering patterns (e.g., 3.1.1)
            if re.match(r'^\d+\.\d+', first_cell):
                has_hierarchical_content = True
                break
    
    # If we have checkpoint headers, it's likely a hierarchical table
    if has_checkpoint_headers:
        return 'complex_hierarchical'
    elif has_checkpoint_headers:
        return 'simple_multicolumn'
    else:
        return 'simple'


def process_complex_hierarchical_table(headers, data_rows):
    """
    Process complex hierarchical tables with multiple levels and value columns.
    Modified to handle rows without arrow symbols as top-level items.
    Enhanced error handling and empty data processing.
    """
    text_parts = []
    
    try:
        # Clean headers for display
        cleaned_headers = [clean_whitespace(h) if h else "" for h in headers]
        
        # Show headers only if they have content
        if any(cleaned_headers):
            header_line = "헤더: " + "\t".join(cleaned_headers)
            text_parts.append(header_line)
    except Exception as e:
        print(f"⚠️ 헤더 처리 중 오류: {e}")
        cleaned_headers = headers  # Fallback to original headers
    
    # Process each row
    for row_idx, row in enumerate(data_rows):
        try:
            if not row:
                continue
            
            # Check if all cells are empty
            if all(not cell or not str(cell).strip() for cell in row):
                continue
            
            # Parse first cell - aggressive whitespace handling
            first_cell_raw = str(row[0]) if row[0] else ""
            # Clean whitespace but preserve newlines for structure detection
            first_cell = clean_whitespace(first_cell_raw, preserve_newlines=True)
            
            # Debug output with repr to see hidden characters
            print(f"\n행 {row_idx}: 원본={repr(first_cell_raw)} -> 처리={repr(first_cell)}")
            
            # Get value columns
            value_data = {}
            has_any_value = False
            
            for col_idx in range(1, len(headers)):
                try:
                    if col_idx < len(row) and row[col_idx]:
                        # Clean header name for use as key
                        header_name = clean_whitespace(headers[col_idx]) if col_idx < len(headers) and headers[col_idx] else f"컬럼{col_idx}"
                        # Clean value but preserve newlines if they exist
                        cell_value = clean_whitespace(str(row[col_idx]), preserve_newlines=True)
                        if cell_value:
                            value_data[header_name] = cell_value
                            has_any_value = True
                except Exception as e:
                    print(f"  ⚠️ 값 처리 중 오류 (행 {row_idx}, 열 {col_idx}): {e}")
                    continue
            
            print(f"  값: {value_data}")
            
            # Handle empty first cell but with values in other columns
            if not first_cell.strip() and has_any_value:
                print(f"  -> 첫 번째 셀은 비어있지만 다른 값이 있음")
                # Treat as continuation of previous item or standalone values
                text_parts.append("")  # Empty line for separation
                text_parts.append("(계속)")  # Indicate continuation
                for header, value in value_data.items():
                    text_parts.append(f"  {header}: {value}")
                continue
            
            # Skip if both first cell and all values are empty
            if not first_cell.strip() and not has_any_value:
                print(f"  -> 빈 행, 스킵")
                continue
            
            # Check if this is a multi-line cell
            if '\n' not in first_cell:
                # Single line item - treat as top-level hierarchy
                print(f"  -> 단일 라인 항목으로 처리")
                # Don't add empty line before if it starts with arrow
                if not first_cell.startswith('→'):
                    text_parts.append("")  # Empty line for separation
                text_parts.append(f"{first_cell.strip()}")
                # Add all values with proper indentation
                for header, value in value_data.items():
                    text_parts.append(f"  {header}: {value}")
            else:
                # Multi-line cell - parse structure
                print(f"  -> 멀티라인 셀로 처리")
                lines = [clean_whitespace(line) for line in first_cell.split('\n') if clean_whitespace(line)]
                structure = parse_cell_structure(lines, value_data)
                
                # Output structured content
                print(f"  구조 출력 시작: {len(structure)}개 항목")
                for idx, item in enumerate(structure):
                    print(f"    항목 {idx}: {item['text'][:30]}... (level={item['level']}, children={len(item.get('children', []))})")
                    output_structured_item(text_parts, item, 0, is_first_in_section=(idx == 0))
                    
        except Exception as e:
            print(f"❌ 행 {row_idx} 처리 중 오류: {e}")
            # Try to at least output the raw row data
            try:
                text_parts.append(f"\n(행 {row_idx} - 오류 발생, 원본 데이터):")
                for col_idx, cell in enumerate(row):
                    if cell and str(cell).strip():
                        header_name = headers[col_idx] if col_idx < len(headers) else f"컬럼{col_idx}"
                        text_parts.append(f"  {header_name}: {str(cell).strip()}")
            except Exception as e2:
                print(f"  ❌ 원본 데이터 출력도 실패: {e2}")
            continue
    
    return text_parts


def parse_cell_structure(lines, value_data):
    """
    Parse a multi-line cell into a hierarchical structure with associated values.
    Modified to handle common values (like 관련 조항) correctly.
    Enhanced to properly recognize string+number patterns.
    Level is determined by position and context, not by pattern type.
    """
    structure = []
    
    try:
        print(f"  멀티라인 파싱: {lines}")
        
        # Analyze line patterns
        line_infos = []
        current_level = 0  # Track current level
        symbol_levels = {}  # Track which level each symbol type has been assigned
        last_symbol = None
        
        for i, line in enumerate(lines):
            if not line:  # Already cleaned in the calling function
                continue
            
            info = {
                'text': line,
                'level': 0,
                'type': 'item',
                'symbol': None
            }
            
            # First line is always top-level (level 0)
            if i == 0:
                info['type'] = 'main'
                info['level'] = 0
                current_level = 0
                print(f"    라인 {i}: '{line}' -> main (첫 번째 라인, 레벨 0)")
            else:
                # Check for pattern using extract_symbol_pattern
                symbol = extract_symbol_pattern(line)
                info['symbol'] = symbol
                
                if symbol:
                    print(f"    라인 {i}: '{line}' -> 심볼 패턴 발견: {symbol}")
                    
                    # Check if we've seen this symbol type before
                    if symbol in symbol_levels:
                        # Go back to the level of this symbol type
                        current_level = symbol_levels[symbol]
                        print(f"      -> 이전에 같은 심볼 발견, 레벨 {current_level}로 복귀")
                    else:
                        # New symbol type
                        if last_symbol and last_symbol != symbol:
                            # Different symbol from previous - go one level deeper
                            current_level = line_infos[-1]['level'] + 1
                            print(f"      -> 새로운 심볼 (이전과 다름), 레벨 {current_level}로 설정")
                        elif last_symbol == symbol:
                            # Same symbol as previous - keep same level
                            current_level = line_infos[-1]['level']
                            print(f"      -> 이전과 같은 심볼, 레벨 {current_level} 유지")
                        else:
                            # First symbol after main
                            current_level = 1
                            print(f"      -> 첫 번째 심볼, 레벨 1로 설정")
                        
                        # Record this symbol's level
                        symbol_levels[symbol] = current_level
                    
                    info['level'] = current_level
                    info['type'] = 'item'
                    last_symbol = symbol
                else:
                    # No symbol pattern found - treat as text under previous item
                    if line_infos:
                        current_level = line_infos[-1]['level'] + 1
                    else:
                        current_level = 1
                    info['level'] = current_level
                    info['type'] = 'text'
                    print(f"    라인 {i}: '{line}' -> text (패턴 없음, 레벨 {current_level})")
                    last_symbol = None
            
            line_infos.append(info)
        
        # Build structure and distribute values
        if not line_infos:
            return structure
        
        # Parse values if they contain newlines
        parsed_values = {}
        for header, value in value_data.items():
            try:
                if '\n' in value:
                    parsed_values[header] = [clean_whitespace(v) for v in value.split('\n') if clean_whitespace(v)]
                else:
                    parsed_values[header] = [clean_whitespace(value)]
            except Exception as e:
                print(f"    ⚠️ 값 파싱 중 오류 ({header}): {e}")
                parsed_values[header] = [str(value)]
        
        print(f"    파싱된 값: {parsed_values}")
        
        # Identify common values (values that have only one entry)
        common_values = {}
        distributed_values = {}
        
        for header, values in parsed_values.items():
            if len(values) == 1:
                # This is a common value for the entire section
                common_values[header] = values[0]
            else:
                # These values need to be distributed
                distributed_values[header] = values
        
        print(f"    공통 값: {common_values}")
        print(f"    분배할 값: {distributed_values}")
        
        # Build hierarchical structure
        current_items = {}  # Track current item at each level
        value_index = 0  # Track which value to assign for distributed values
        
        # First, calculate how many leaf items we have (items with no children)
        leaf_items = []
        for i, info in enumerate(line_infos):
            is_leaf = (i == len(line_infos) - 1) or (i < len(line_infos) - 1 and line_infos[i+1]['level'] <= info['level'])
            if is_leaf and info['level'] > 0:  # Don't count main items as leaves
                leaf_items.append(i)
        
        print(f"    총 리프 아이템 수: {len(leaf_items)}")
        
        # Create hierarchical structure
        for i, info in enumerate(line_infos):
            level = info['level']
            
            # Create item
            item = {
                'text': info['text'],
                'level': level,
                'values': {},
                'children': []
            }
            
            # Add common values to level 0 items only
            if level == 0:
                item['values'] = common_values.copy()
            
            # Check if this is a leaf item
            if i in leaf_items:
                # Assign distributed values to leaf items
                for header, values in distributed_values.items():
                    if value_index < len(values):
                        item['values'][header] = values[value_index]
                value_index += 1
                print(f"      리프 아이템 '{info['text']}' (레벨 {level})에 값 할당, index={value_index-1}")
            
            # Add item to structure or parent
            if level == 0:
                # Top-level item
                structure.append(item)
                current_items[0] = item
                # Clear lower level items
                for l in range(1, 10):  # Support up to 10 levels
                    if l in current_items:
                        del current_items[l]
            else:
                # Find parent at level - 1
                parent = current_items.get(level - 1)
                if parent:
                    parent['children'].append(item)
                    current_items[level] = item
                    # Clear lower level items
                    for l in range(level + 1, 10):
                        if l in current_items:
                            del current_items[l]
                else:
                    # No parent found, add to root
                    structure.append(item)
                    current_items[level] = item
                    
    except Exception as e:
        print(f"  ❌ 셀 구조 파싱 중 오류: {e}")
        # Return simple structure as fallback
        for line in lines:
            if line:
                structure.append({
                    'text': line,
                    'level': 0,
                    'values': value_data if structure == [] else {},
                    'children': []
                })
    
    return structure


def is_likely_subitem(prev_symbol, current_symbol):
    """
    Determine if current symbol is likely a sub-item of previous symbol.
    """
    # Define typical hierarchy relationships
    hierarchy_rules = [
        # Numbered patterns followed by string+number patterns
        (['N)', 'N.', 'N-', '(N)', '[N]', 'N:', 'N번'], 
         ['ALPHA_NUM', 'alpha_num', 'Alpha_Num', '한글_숫자', 
          'ALPHA_NUM_PREFIX', 'alpha_num_prefix', '한글_숫자_PREFIX']),
        
        # Numbered patterns followed by letter patterns
        (['N)', 'N.', 'N-', '(N)', '[N]', 'N:', 'N번'], 
         ['a)', 'A)', 'a.', 'A.', '(a)', '(A)', '[a]', '[A]']),
        
        # String+number patterns followed by sub-string+number (rare but possible)
        (['ALPHA_NUM', 'alpha_num', 'Alpha_Num', '한글_숫자'], 
         ['a)', 'A)', 'a.', 'A.', '(a)', '(A)']),
        
        # Special symbols followed by anything
        (['→', '▶', '◆', '■'], 
         ['N)', 'N.', 'N-', '-', '*', '•']),
    ]
    
    for parent_patterns, child_patterns in hierarchy_rules:
        if prev_symbol in parent_patterns and current_symbol in child_patterns:
            return True
    
    # If both are the same type, they're likely at the same level
    if prev_symbol == current_symbol:
        return False
    
    # Default: if symbols are different and no rule matches, assume same level
    return False


def output_structured_item(text_parts, item, base_indent, is_first_in_section=False):
    """
    Output a structured item with proper indentation and values.
    Enhanced error handling.
    """
    try:
        # Calculate actual indentation based on item level
        # Level 0: no indent
        # Level 1: 2 spaces
        # Level 2: 4 spaces
        # Level 3: 6 spaces
        indent = "  " * item['level']
        
        # Add newline before main items (level 0) unless it's the first item
        if item['level'] == 0 and not is_first_in_section:
            text_parts.append("")  # Empty line for separation
        
        # Always output the item text first
        output_line = f"{indent}{item['text']}"
        text_parts.append(output_line)
        print(f"      출력: '{output_line}'")
        
        # Output values if present
        if item.get('values'):
            value_indent = "  " * (item['level'] + 1)
            for header, value in item['values'].items():
                try:
                    value_line = f"{value_indent}{header}: {value}"
                    text_parts.append(value_line)
                    print(f"      값 출력: '{value_line}'")
                except Exception as e:
                    print(f"      ⚠️ 값 출력 중 오류 ({header}): {e}")
        
        # Output children recursively
        if item.get('children'):
            for child in item['children']:
                output_structured_item(text_parts, child, base_indent, is_first_in_section=False)
                
    except Exception as e:
        print(f"    ❌ 구조화된 항목 출력 중 오류: {e}")
        # Try to output at least the text
        try:
            text_parts.append(f"{item.get('text', '(오류 발생)')}")
        except:
            pass


def process_simple_multicolumn_table(headers, data_rows):
    """
    Process a simple multi-column table.
    Enhanced error handling and empty data processing.
    """
    text_parts = []
    
    try:
        # Show headers
        header_line = "헤더: " + " | ".join(h.strip() if h else "" for h in headers)
        text_parts.append(header_line)
    except Exception as e:
        print(f"⚠️ 헤더 처리 중 오류: {e}")
    
    # Process each row
    for row_idx, row in enumerate(data_rows):
        try:
            # Check if row has any content
            has_content = False
            for cell in row:
                if cell and str(cell).strip():
                    has_content = True
                    break
            
            if not has_content:
                continue
            
            # Get first cell for the main item
            first_cell = str(row[0]).strip() if row[0] else ""
            
            if first_cell:
                # Use first cell as the main identifier
                text_parts.append(f"\n{first_cell}")
                
                # Add values from other columns
                for col_idx in range(1, len(row)):
                    try:
                        if col_idx < len(headers) and row[col_idx]:
                            header_name = headers[col_idx].strip() if headers[col_idx] else f"컬럼{col_idx}"
                            value_str = str(row[col_idx]).strip()
                            
                            if value_str:
                                text_parts.append(f"  {header_name}: {value_str}")
                    except Exception as e:
                        print(f"  ⚠️ 값 처리 중 오류 (행 {row_idx}, 열 {col_idx}): {e}")
            else:
                # First cell is empty but row has other values
                text_parts.append(f"\n레코드 {row_idx + 1}:")
                
                # Output all non-empty columns
                for col_idx, value in enumerate(row):
                    try:
                        if value and str(value).strip():
                            if col_idx < len(headers):
                                header_name = headers[col_idx].strip() if headers[col_idx] else f"컬럼{col_idx + 1}"
                            else:
                                header_name = f"컬럼{col_idx + 1}"
                            
                            value_str = str(value).strip()
                            text_parts.append(f"  {header_name}: {value_str}")
                    except Exception as e:
                        print(f"  ⚠️ 값 처리 중 오류 (행 {row_idx}, 열 {col_idx}): {e}")
                        
        except Exception as e:
            print(f"❌ 행 {row_idx} 처리 중 오류: {e}")
            continue
    
    return text_parts


def process_simple_table(headers, data_rows):
    """
    Process a simple table as individual records.
    Enhanced error handling.
    """
    text_parts = []
    
    # Show headers
    if headers:
        try:
            header_line = "헤더: " + " | ".join(h.strip() if h else "" for h in headers)
            text_parts.append(header_line)
        except Exception as e:
            print(f"⚠️ 헤더 처리 중 오류: {e}")
    
    # Process each row
    for idx, row in enumerate(data_rows):
        try:
            if not any(str(cell).strip() for cell in row):
                continue
            
            text_parts.append(f"\n레코드 {idx + 1}:")
            
            for col_idx, value in enumerate(row):
                try:
                    if col_idx < len(headers):
                        field_name = headers[col_idx].strip() if headers[col_idx] else f"열{col_idx + 1}"
                    else:
                        field_name = f"열{col_idx + 1}"
                    
                    value_str = str(value).strip() if value else ""
                    
                    if value_str:
                        if '\n' in value_str:
                            text_parts.append(f"  {field_name}:")
                            for line in value_str.split('\n'):
                                if line.strip():
                                    text_parts.append(f"    - {line.strip()}")
                        else:
                            text_parts.append(f"  {field_name}: {value_str}")
                except Exception as e:
                    print(f"  ⚠️ 값 처리 중 오류 (행 {idx}, 열 {col_idx}): {e}")
                    
        except Exception as e:
            print(f"❌ 행 {idx} 처리 중 오류: {e}")
            continue
    
    return text_parts


def process_table_without_headers(data_rows):
    """
    Process table without headers using generic field names.
    Enhanced error handling and empty data processing.
    """
    text_parts = []
    
    try:
        max_cols = max(len(row) for row in data_rows) if data_rows else 0
    except:
        max_cols = 0
    
    for idx, row in enumerate(data_rows):
        try:
            has_content = any(str(cell).strip() for cell in row)
            
            if has_content:
                text_parts.append(f"\n레코드 {idx + 1}:")
                
                for col_idx in range(max_cols):
                    try:
                        if col_idx < len(row):
                            value = row[col_idx]
                            value_str = str(value).strip() if value else ""
                        else:
                            value_str = ""
                        
                        field_name = f"항목{col_idx + 1}"
                        
                        if value_str:
                            if '\n' in value_str:
                                text_parts.append(f"  {field_name}:")
                                for line in value_str.split('\n'):
                                    if line.strip():
                                        text_parts.append(f"    - {line.strip()}")
                            else:
                                text_parts.append(f"  {field_name}: {value_str}")
                    except Exception as e:
                        print(f"  ⚠️ 값 처리 중 오류 (행 {idx}, 열 {col_idx}): {e}")
                        
        except Exception as e:
            print(f"❌ 행 {idx} 처리 중 오류: {e}")
            continue
    
    return text_parts


def clean_whitespace(text: str, preserve_newlines: bool = False) -> str:
    """
    Remove all types of whitespace including special unicode spaces.
    If preserve_newlines is True, keep newline characters.
    Enhanced error handling.
    """
    if not text:
        return ""
    
    try:
        # Convert to string first
        text = str(text)
    except Exception as e:
        print(f"⚠️ 텍스트 변환 중 오류: {e}")
        return ""
    
    # Replace various unicode spaces with regular space
    unicode_spaces = [
        '\u00A0',  # Non-breaking space
        '\u2000',  # En quad
        '\u2001',  # Em quad
        '\u2002',  # En space
        '\u2003',  # Em space
        '\u2004',  # Three-per-em space
        '\u2005',  # Four-per-em space
        '\u2006',  # Six-per-em space
        '\u2007',  # Figure space
        '\u2008',  # Punctuation space
        '\u2009',  # Thin space
        '\u200A',  # Hair space
        '\u200B',  # Zero-width space
        '\u202F',  # Narrow no-break space
        '\u205F',  # Medium mathematical space
        '\u3000',  # Ideographic space
        '\t',      # Tab
        '\r',      # Carriage return
    ]
    
    try:
        for space in unicode_spaces:
            text = text.replace(space, ' ')
    except Exception as e:
        print(f"⚠️ 유니코드 공백 처리 중 오류: {e}")
    
    if preserve_newlines:
        # Process each line separately to preserve newlines
        lines = text.split('\n')
        cleaned_lines = []
        for line in lines:
            # Replace multiple spaces with single space in each line
            cleaned_line = ' '.join(line.split())
            cleaned_lines.append(cleaned_line)
        return '\n'.join(cleaned_lines)
    else:
        # Replace multiple spaces with single space
        text = ' '.join(text.split())
        return text.strip()


def preprocess_table_data(data: list) -> list:
    """
    Preprocess table data to handle multi-line cells and clean up content.
    Enhanced error handling.
    """
    processed = []
    for row_idx, row in enumerate(data):
        try:
            processed_row = []
            for cell in row:
                if cell is None:
                    processed_row.append("")
                else:
                    try:
                        # Convert to string and clean whitespace while preserving structure
                        # Do NOT clean newlines here as they are important for structure
                        cell_str = str(cell)
                        processed_row.append(cell_str)
                    except Exception as e:
                        print(f"⚠️ 셀 전처리 중 오류 (행 {row_idx}): {e}")
                        processed_row.append("")
            processed.append(processed_row)
        except Exception as e:
            print(f"❌ 행 {row_idx} 전처리 중 오류: {e}")
            # Try to append empty row to maintain structure
            try:
                processed.append([""] * len(data[0]))
            except:
                processed.append([])
    return processed


def is_header_row(row: list) -> bool:
    """
    Heuristically determine if a row is likely a header row.
    This is kept for backward compatibility but should be used with analyze_header_with_similarity.
    """
    if not row:
        return False
    
    # Check for header keywords
    header_keywords = [
        '번호', '이름', '제목', '구분', '항목', '내용', '비고',
        'No.', 'Name', 'Title', 'Type', 'Item', 'Content', 'Remark',
        '날짜', 'Date', '금액', 'Amount', '수량', 'Quantity',
        '관련', '조항', '하한치', '표준치', '상한치', '점검', '내용',
        '값', 'Value', '단위', 'Unit', '기준', 'Standard'
    ]
    
    for cell in row:
        if cell and any(keyword in str(cell) for keyword in header_keywords):
            return True
    
    # Check if most cells are non-empty and short
    non_empty_count = sum(1 for cell in row if cell and str(cell).strip())
    
    if non_empty_count >= len(row) * 0.7:
        avg_length = sum(len(str(cell)) for cell in row if cell) / non_empty_count
        if avg_length < 20:
            return True
    
    return False


def analyze_header_with_similarity(data: list) -> bool:
    """
    Analyze if the first row is a header by comparing structural similarity.
    Compares first row with other rows using multiple metrics.
    """
    if len(data) < 3:  # Need at least 3 rows for meaningful comparison
        return is_header_row(data[0]) if data else False
    
    first_row = data[0]
    other_rows = data[1:]
    
    # Calculate various similarity metrics
    metrics = {}
    
    # 1. Cell type pattern (empty/text/numeric/multiline)
    def get_cell_pattern(row):
        pattern = []
        for cell in row:
            if not cell or not str(cell).strip():
                pattern.append('empty')
            elif '\n' in str(cell):
                pattern.append('multiline')
            elif is_numeric(str(cell).strip()):
                pattern.append('numeric')
            else:
                pattern.append('text')
        return pattern
    
    # 2. Cell length pattern
    def get_length_pattern(row):
        return [len(str(cell).strip()) if cell else 0 for cell in row]
    
    # 3. Empty cell positions
    def get_empty_positions(row):
        return [i for i, cell in enumerate(row) if not cell or not str(cell).strip()]
    
    # 4. Special character presence
    def has_special_chars(text):
        special_chars = [':', '-', '/', '(', ')', '[', ']', '•', '·', '▪', '►']
        return any(char in text for char in special_chars)
    
    def get_special_char_pattern(row):
        return [has_special_chars(str(cell)) if cell else False for cell in row]
    
    # 5. Character type pattern (Korean/English/Number/Mixed)
    def get_char_type_pattern(row):
        pattern = []
        for cell in row:
            if not cell or not str(cell).strip():
                pattern.append('empty')
            else:
                text = str(cell).strip()
                has_korean = any('\uAC00' <= char <= '\uD7AF' for char in text)
                has_english = any('a' <= char.lower() <= 'z' for char in text)
                has_number = any(char.isdigit() for char in text)
                
                # Determine primary character type
                if has_korean and not has_english and not has_number:
                    pattern.append('korean')
                elif has_english and not has_korean and not has_number:
                    pattern.append('english')
                elif has_number and not has_korean and not has_english:
                    pattern.append('number')
                elif has_korean and (has_english or has_number):
                    pattern.append('korean_mixed')
                elif has_english and has_number and not has_korean:
                    pattern.append('english_number')
                else:
                    pattern.append('mixed')
        return pattern
    
    # Calculate similarity for each metric
    first_row_patterns = {
        'type': get_cell_pattern(first_row),
        'length': get_length_pattern(first_row),
        'empty': set(get_empty_positions(first_row)),
        'special': get_special_char_pattern(first_row),
        'char_type': get_char_type_pattern(first_row)  # New metric
    }
    
    # Calculate similarities
    similarities = {
        'inter_row': {},  # Similarity among other rows (excluding first)
        'first_vs_others': {}  # Similarity between first and each other row
    }
    
    for metric in ['type', 'length', 'empty', 'special', 'char_type']:
        # Inter-row similarity (among rows 2+)
        if len(other_rows) > 1:
            inter_scores = []
            for i in range(len(other_rows)):
                for j in range(i + 1, len(other_rows)):
                    score = calculate_row_similarity(
                        other_rows[i], other_rows[j], metric,
                        get_cell_pattern, get_length_pattern, 
                        get_empty_positions, get_special_char_pattern,
                        get_char_type_pattern
                    )
                    inter_scores.append(score)
            similarities['inter_row'][metric] = sum(inter_scores) / len(inter_scores) if inter_scores else 0
        else:
            similarities['inter_row'][metric] = 1.0  # Only one other row
        
        # First vs others similarity
        first_scores = []
        for other_row in other_rows:
            score = calculate_row_similarity(
                first_row, other_row, metric,
                get_cell_pattern, get_length_pattern,
                get_empty_positions, get_special_char_pattern,
                get_char_type_pattern
            )
            first_scores.append(score)
        similarities['first_vs_others'][metric] = sum(first_scores) / len(first_scores)
    
    # Calculate differences for each metric
    differences = {}
    for metric in ['type', 'length', 'empty', 'special', 'char_type']:
        inter_sim = similarities['inter_row'][metric]
        first_sim = similarities['first_vs_others'][metric]
        differences[metric] = inter_sim - first_sim
    
    # Debug output
    print(f"\n📊 Header Analysis:")
    print(f"First row: {[str(cell)[:20] + '...' if cell and len(str(cell)) > 20 else str(cell) for cell in first_row]}")
    print(f"First row char types: {first_row_patterns['char_type']}")
    print(f"\nSimilarity Differences (inter_row - first_vs_others):")
    for metric, diff in differences.items():
        print(f"  {metric}: {diff:.3f} (inter: {similarities['inter_row'][metric]:.3f}, first: {similarities['first_vs_others'][metric]:.3f})")
    
    # Find the metric with the largest difference
    max_diff_metric = max(differences.items(), key=lambda x: x[1])
    print(f"\nMax difference: {max_diff_metric[0]} = {max_diff_metric[1]:.3f}")
    
    # If the maximum difference is positive and significant (> 0.3),
    # it means other rows are more similar to each other than to the first row
    # This suggests the first row is different (likely a header)
    is_header = max_diff_metric[1] > 0.3
    
    # Additional check: if first row has header keywords, increase confidence
    if not is_header and is_header_row(first_row):
        is_header = True
        print("  -> Has header keywords, marking as header")
    
    print(f"  -> Is header: {is_header}")
    
    return is_header


def calculate_row_similarity(row1, row2, metric, 
                           get_cell_pattern, get_length_pattern,
                           get_empty_positions, get_special_char_pattern,
                           get_char_type_pattern=None):
    """
    Calculate similarity between two rows based on a specific metric.
    """
    if metric == 'type':
        pattern1 = get_cell_pattern(row1)
        pattern2 = get_cell_pattern(row2)
        # Compare patterns
        if len(pattern1) != len(pattern2):
            return 0
        matches = sum(1 for p1, p2 in zip(pattern1, pattern2) if p1 == p2)
        return matches / len(pattern1)
    
    elif metric == 'length':
        lengths1 = get_length_pattern(row1)
        lengths2 = get_length_pattern(row2)
        if len(lengths1) != len(lengths2):
            return 0
        # Calculate normalized difference
        diffs = []
        for l1, l2 in zip(lengths1, lengths2):
            max_len = max(l1, l2, 1)  # Avoid division by zero
            diff = abs(l1 - l2) / max_len
            diffs.append(1 - diff)  # Convert to similarity
        return sum(diffs) / len(diffs)
    
    elif metric == 'empty':
        empty1 = set(get_empty_positions(row1))
        empty2 = set(get_empty_positions(row2))
        if not empty1 and not empty2:
            return 1.0
        if not empty1 or not empty2:
            return 0.0
        # Jaccard similarity
        intersection = len(empty1 & empty2)
        union = len(empty1 | empty2)
        return intersection / union if union > 0 else 0
    
    elif metric == 'special':
        special1 = get_special_char_pattern(row1)
        special2 = get_special_char_pattern(row2)
        if len(special1) != len(special2):
            return 0
        matches = sum(1 for s1, s2 in zip(special1, special2) if s1 == s2)
        return matches / len(special1)
    
    elif metric == 'char_type' and get_char_type_pattern:
        pattern1 = get_char_type_pattern(row1)
        pattern2 = get_char_type_pattern(row2)
        if len(pattern1) != len(pattern2):
            return 0
        matches = sum(1 for p1, p2 in zip(pattern1, pattern2) if p1 == p2)
        return matches / len(pattern1)
    
    return 0


def is_numeric(text: str) -> bool:
    """
    Check if text represents a numeric value.
    """
    if not text:
        return False
    
    # Remove common number formatting and units
    cleaned = text.replace(',', '').replace(' ', '').strip()
    
    # Remove units
    units = ['PPS', 'pps', '㎲', '㎾', '㎒', 'MHz', 'kW', '%', '원', '$']
    for unit in units:
        cleaned = cleaned.replace(unit, '')
    
    # Handle special cases
    if cleaned in ['-', '—', '–']:
        return False
    
    if cleaned.startswith('≤') or cleaned.startswith('≥'):
        cleaned = cleaned[1:]
    
    try:
        float(cleaned)
        return True
    except ValueError:
        return False