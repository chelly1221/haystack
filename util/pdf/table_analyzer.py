"""
Table structure analyzer module.
Responsible for analyzing table structure and determining processing methods.
"""
import re
from collections import Counter


def analyze_table_structure(headers, data_rows):
    """
    Analyze table structure to determine the best processing method.
    Returns a dictionary with structure type and metadata.
    """
    if not data_rows:
        return {'type': 'empty'}
    
    # Check for multi-line cells in first column
    has_multiline_first_column = any('\n' in str(row[0]) for row in data_rows if row and row[0])
    
    if has_multiline_first_column:
        return {'type': 'hierarchical_multiline'}
    
    # Check for various table patterns
    structure_indicators = {
        'has_hierarchy_symbols': False,
        'has_grouped_values': False,
        'has_empty_first_cells': False,
        'has_multiline_cells': False,
        'value_rows_count': 0,
        'hierarchy_rows_count': 0,
        'mixed_rows_count': 0
    }
    
    # Analyze each row
    row_analysis = []
    for idx, row in enumerate(data_rows):
        row_info = analyze_single_row(row, headers)
        row_analysis.append(row_info)
        
        # Update indicators
        if row_info['has_hierarchy']:
            structure_indicators['has_hierarchy_symbols'] = True
            structure_indicators['hierarchy_rows_count'] += 1
        
        if row_info['is_value_only']:
            structure_indicators['value_rows_count'] += 1
        
        if row_info['has_empty_first_cell'] and row_info['has_values']:
            structure_indicators['has_empty_first_cells'] = True
        
        if row_info['has_multiline_cells']:
            structure_indicators['has_multiline_cells'] = True
        
        if row_info['has_hierarchy'] and row_info['has_values']:
            structure_indicators['mixed_rows_count'] += 1
    
    # Determine table type based on patterns
    table_type = determine_table_type(structure_indicators, row_analysis)
    
    # For hierarchical tables, perform additional analysis
    if table_type == 'hierarchical':
        hierarchical_analysis = analyze_hierarchical_structure(data_rows)
        if hierarchical_analysis:
            return {
                'type': 'hierarchical',
                'indicators': structure_indicators,
                'row_analysis': row_analysis,
                **hierarchical_analysis
            }
    
    return {
        'type': table_type,
        'indicators': structure_indicators,
        'row_analysis': row_analysis
    }


def analyze_single_row(row, headers):
    """
    Analyze a single row to extract its characteristics.
    """
    if not row:
        return {
            'is_empty': True,
            'has_hierarchy': False,
            'has_values': False,
            'is_value_only': False,
            'has_empty_first_cell': False,
            'has_multiline_cells': False
        }
    
    # Clean special spaces before processing
    unicode_spaces = [
        '\u00A0', '\u2000', '\u2001', '\u2002', '\u2003', '\u2004',
        '\u2005', '\u2006', '\u2007', '\u2008', '\u2009', '\u200A',
        '\u200B', '\u202F', '\u205F', '\u3000'
    ]
    
    first_cell_raw = str(row[0]) if row[0] else ""
    for space in unicode_spaces:
        first_cell_raw = first_cell_raw.replace(space, ' ')
    first_cell = first_cell_raw.strip()
    
    # Check for hierarchy patterns
    hierarchy_symbols = extract_symbol_pattern(first_cell) if first_cell else None
    has_hierarchy = bool(hierarchy_symbols)
    
    # Check for special markers
    has_special_marker = any(marker in first_cell for marker in ['→', '▶', '◆', '■'])
    
    # Check for values in other columns
    values = []
    for col_idx in range(1, len(row)):
        if col_idx < len(headers) and row[col_idx] and str(row[col_idx]).strip():
            values.append({
                'col_idx': col_idx,
                'header': headers[col_idx] if col_idx < len(headers) else f"Column {col_idx}",
                'value': str(row[col_idx]).strip()
            })
    
    # Check for multiline cells
    has_multiline = any('\n' in str(cell) for cell in row if cell)
    
    return {
        'is_empty': False,
        'has_hierarchy': has_hierarchy or has_special_marker,
        'hierarchy_symbol': hierarchy_symbols,
        'has_values': len(values) > 0,
        'values': values,
        'is_value_only': not first_cell and len(values) > 0,
        'has_empty_first_cell': not first_cell,
        'has_multiline_cells': has_multiline,
        'first_cell': first_cell,
        'has_special_marker': has_special_marker
    }


def determine_table_type(indicators, row_analysis):
    """
    Determine the type of table based on structural indicators.
    """
    # Pattern 1: Grouped values (values appear before their labels)
    if indicators['value_rows_count'] > 0 and indicators['hierarchy_rows_count'] > 0:
        # Check if values consistently appear before hierarchy
        value_before_hierarchy = False
        for i in range(len(row_analysis) - 1):
            if (row_analysis[i]['is_value_only'] and 
                row_analysis[i + 1]['has_hierarchy']):
                value_before_hierarchy = True
                break
        
        if value_before_hierarchy:
            return 'grouped_values'
    
    # Pattern 2: Hierarchical table with symbols
    if indicators['has_hierarchy_symbols']:
        return 'hierarchical'
    
    # Pattern 3: Mixed table (hierarchy and values in same row)
    if indicators['mixed_rows_count'] > len(row_analysis) * 0.5:
        return 'mixed'
    
    # Pattern 4: Simple table
    return 'simple'


def analyze_hierarchical_structure(data_rows):
    """
    Analyze table structure based on symbols in first column.
    Legacy function for backward compatibility.
    """
    if not data_rows:
        return False
    
    # Track unique symbols and their first appearance order
    symbol_order = []
    symbol_to_level = {}
    
    # Check for hierarchy patterns
    has_hierarchy_patterns = False
    
    # First pass: collect all unique symbols in order of appearance
    for row in data_rows:
        if not row or not row[0]:
            continue
            
        first_cell = str(row[0]).strip()
        
        # Split by lines and check each line
        lines = first_cell.split('\n')
        for line in lines:
            line = line.strip()
            if line:
                symbol = extract_symbol_pattern(line)
                if symbol and symbol not in symbol_to_level:
                    symbol_order.append(symbol)
                    symbol_to_level[symbol] = len(symbol_order) - 1
                    has_hierarchy_patterns = True
    
    if not has_hierarchy_patterns:
        return False
    
    # Analyze rows and build hierarchy
    hierarchy_data = []
    
    for row_idx, row in enumerate(data_rows):
        if not row or len(row) == 0:
            continue
            
        first_cell = str(row[0]).strip()
        if not first_cell:
            continue
        
        # Check if this row contains hierarchy symbols
        row_has_hierarchy = False
        row_symbols = []
        
        lines = first_cell.split('\n')
        for line_idx, line in enumerate(lines):
            line = line.strip()
            if line:
                symbol = extract_symbol_pattern(line)
                if symbol:
                    row_has_hierarchy = True
                    level = symbol_to_level.get(symbol, 0)
                    row_symbols.append({
                        'symbol': symbol,
                        'text': line,
                        'line_in_cell': line_idx,
                        'level': level
                    })
        
        # Collect value cells
        value_cells = []
        for col_idx in range(1, len(row)):
            if row[col_idx]:
                value_cells.append({
                    'col_idx': col_idx,
                    'value': str(row[col_idx]).strip()
                })
        
        hierarchy_data.append({
            'row_idx': row_idx,
            'first_cell': first_cell,
            'has_hierarchy': row_has_hierarchy,
            'symbols': row_symbols,
            'value_cells': value_cells,
            'is_value_row': not row_has_hierarchy and len(value_cells) > 0
        })
    
    # Reorder data
    reordered_data = reorder_hierarchy_with_values(hierarchy_data, data_rows)
    
    if reordered_data:
        return {
            'hierarchy_data': hierarchy_data,
            'reordered_data': reordered_data,
            'symbol_order': symbol_order,
            'symbol_to_level': symbol_to_level,
            'is_hierarchical': True
        }
    
    return False


def reorder_hierarchy_with_values(hierarchy_data, original_rows):
    """
    Reorder data so that values appear after their corresponding hierarchy labels.
    """
    if not hierarchy_data:
        return None
    
    reordered = []
    i = 0
    
    while i < len(hierarchy_data):
        current = hierarchy_data[i]
        
        # If this row has hierarchy symbols
        if current['has_hierarchy']:
            # Add the hierarchy row
            reordered.append({
                'type': 'hierarchy',
                'row_idx': current['row_idx'],
                'data': current
            })
            
            # Look ahead for value rows that should belong to this hierarchy
            j = i + 1
            while j < len(hierarchy_data):
                next_row = hierarchy_data[j]
                
                # If we hit another hierarchy row, stop
                if next_row['has_hierarchy']:
                    break
                
                # If this is a value row, associate it with current hierarchy
                if next_row['is_value_row']:
                    reordered.append({
                        'type': 'value',
                        'row_idx': next_row['row_idx'],
                        'data': next_row,
                        'parent_hierarchy': current
                    })
                    j += 1
                else:
                    break
            
            i = j
        else:
            # Standalone value row (shouldn't happen in well-structured tables)
            reordered.append({
                'type': 'value',
                'row_idx': current['row_idx'],
                'data': current,
                'parent_hierarchy': None
            })
            i += 1
    
    return reordered


def analyze_hierarchical_structure_with_positions(data_rows, cell_data, headers):
    """
    Analyze table structure with cell position information.
    This helps identify when values appear above their labels due to layout.
    """
    if not data_rows or not cell_data:
        return analyze_hierarchical_structure(data_rows)  # Fallback to regular analysis
    
    # Create a map of cell positions to content
    position_map = {}
    for cell in cell_data:
        if cell['row'] > 0:  # Skip header row
            position_map[(cell['row']-1, cell['col'])] = {
                'text': cell['text'],
                'y0': cell['y0'],
                'y1': cell['y1'],
                'x0': cell['x0'],
                'x1': cell['x1']
            }
    
    # Track unique symbols and their first appearance order
    symbol_order = []
    symbol_to_level = {}
    
    # Analyze structure row by row
    row_analysis = []
    
    for row_idx, row in enumerate(data_rows):
        if not row:
            continue
        
        # Process first cell with aggressive whitespace handling
        first_cell_raw = str(row[0]) if row[0] else ""
        
        # Remove special Unicode spaces
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
        ]
        
        first_cell_cleaned = first_cell_raw
        for space in unicode_spaces:
            first_cell_cleaned = first_cell_cleaned.replace(space, ' ')
        
        first_cell = first_cell_cleaned.strip()
        
        # Debug: print raw vs cleaned vs stripped
        if first_cell_raw != first_cell:
            print(f"🔍 행 {row_idx}: 원본 '{repr(first_cell_raw)}' -> 정리 '{repr(first_cell_cleaned)}' -> 최종 '{first_cell}'")
        
        # Get position info for this row
        row_positions = []
        for col_idx in range(len(row)):
            if (row_idx, col_idx) in position_map:
                row_positions.append(position_map[(row_idx, col_idx)])
            else:
                row_positions.append(None)
        
        # Analyze first cell for hierarchy patterns
        hierarchy_items = []
        if first_cell:
            lines = first_cell.split('\n')
            
            for line_idx, line in enumerate(lines):
                # Clean special spaces from each line too
                line_cleaned = line
                for space in unicode_spaces:
                    line_cleaned = line_cleaned.replace(space, ' ')
                line = line_cleaned.strip()
                
                if line:
                    # Debug specific patterns
                    if re.match(r'^\d+\)', line):
                        print(f"✅ 번호 패턴 발견: '{line}' (행 {row_idx})")
                    elif re.match(r'^[A-Za-z가-힣]+\s*\d+', line):
                        print(f"✅ 문자열+숫자 패턴 발견: '{line}' (행 {row_idx})")
                    
                    symbol = extract_symbol_pattern(line)
                    if symbol:
                        if symbol not in symbol_to_level:
                            symbol_order.append(symbol)
                            symbol_to_level[symbol] = len(symbol_order) - 1
                        
                        hierarchy_items.append({
                            'symbol': symbol,
                            'text': line,
                            'level': symbol_to_level[symbol]
                        })
                        print(f"   -> 심볼 '{symbol}' 레벨 {symbol_to_level[symbol]}")
        
        # Check if this row has values without hierarchy
        has_values = False
        value_cells = []
        for col_idx in range(1, len(row)):
            if row[col_idx]:
                cell_value = str(row[col_idx]).strip()
                if cell_value:
                    has_values = True
                    value_cells.append({
                        'col_idx': col_idx,
                        'value': cell_value
                    })
        
        row_analysis.append({
            'row_idx': row_idx,
            'first_cell': first_cell,
            'first_cell_raw': first_cell_raw,  # Keep raw version for debugging
            'hierarchy_items': hierarchy_items,
            'has_hierarchy': len(hierarchy_items) > 0,
            'has_values': has_values,
            'value_cells': value_cells,
            'positions': row_positions,
            'row_data': row
        })
        
        # Debug output
        print(f"행 {row_idx}: 계층={len(hierarchy_items)>0}, 값={has_values}, 내용='{first_cell[:50]}...'")
    
    # Now analyze patterns to identify misplaced values
    reordered_structure = reorder_by_logical_structure(row_analysis, headers)
    
    if reordered_structure:
        return {
            'row_analysis': row_analysis,
            'reordered_structure': reordered_structure,
            'symbol_order': symbol_order,
            'symbol_to_level': symbol_to_level,
            'is_hierarchical': True
        }
    
    # If no hierarchical structure found, return False
    if not symbol_order:
        return False
    
    # Fallback to simple hierarchical structure
    return {
        'row_analysis': row_analysis,
        'reordered_structure': None,
        'symbol_order': symbol_order,
        'symbol_to_level': symbol_to_level,
        'is_hierarchical': True
    }


def reorder_by_logical_structure(row_analysis, headers):
    """
    Reorder rows based on logical structure rather than physical position.
    Specifically handles cases where frequency values appear before their labels.
    """
    reordered = []
    processed = set()
    
    # Debug print
    print(f"\n🔍 분석 중인 행 수: {len(row_analysis)}")
    for idx, row in enumerate(row_analysis):
        print(f"행 {idx}: 계층={row['has_hierarchy']}, 값={row['has_values']}, 내용={row['first_cell'][:30]}...")
    
    i = 0
    while i < len(row_analysis):
        if i in processed:
            i += 1
            continue
            
        current = row_analysis[i]
        
        # Case 1: 값만 있는 행 다음에 번호 패턴 계층이 오는 경우
        if (not current['has_hierarchy'] and current['has_values'] and 
            i + 1 < len(row_analysis)):
            
            next_row = row_analysis[i + 1]
            
            # 다음 행이 번호 패턴을 가진 계층인지 확인
            if next_row['has_hierarchy'] and has_numbered_pattern(next_row):
                print(f"\n✅ 재배치 감지: 값 행 {i} -> 계층 행 {i+1}")
                print(f"   값: {current['row_data']}")
                print(f"   계층: {next_row['first_cell']}")
                
                # 계층을 먼저 추가
                reordered.append({
                    'type': 'hierarchy',
                    'data': next_row,
                    'original_idx': i + 1
                })
                
                # 그 다음 값을 추가
                reordered.append({
                    'type': 'value',
                    'data': current,
                    'original_idx': i,
                    'parent_idx': len(reordered) - 2
                })
                
                processed.add(i)
                processed.add(i + 1)
                
                # 다음 값 행들도 확인
                j = i + 2
                while j < len(row_analysis) and j not in processed:
                    check_row = row_analysis[j]
                    
                    # 또 다른 계층이 나오면 중단
                    if check_row['has_hierarchy']:
                        break
                        
                    # 값만 있는 행이면 현재 계층에 속함
                    if check_row['has_values']:
                        reordered.append({
                            'type': 'value',
                            'data': check_row,
                            'original_idx': j,
                            'parent_idx': len(reordered) - 2
                        })
                        processed.add(j)
                    
                    j += 1
                
                i = j
                continue
        
        # Case 2: 일반 계층 행
        if current['has_hierarchy'] and i not in processed:
            reordered.append({
                'type': 'hierarchy',
                'data': current,
                'original_idx': i
            })
            processed.add(i)
            
            # 뒤따르는 값 행들 처리
            j = i + 1
            while j < len(row_analysis):
                next_row = row_analysis[j]
                
                if next_row['has_hierarchy']:
                    break
                    
                if next_row['has_values'] and j not in processed:
                    reordered.append({
                        'type': 'value',
                        'data': next_row,
                        'original_idx': j,
                        'parent_idx': len(reordered) - 1
                    })
                    processed.add(j)
                
                j += 1
            
            i = j
            continue
        
        # Case 3: 독립적인 값 행
        if current['has_values'] and i not in processed:
            reordered.append({
                'type': 'value',
                'data': current,
                'original_idx': i,
                'parent_idx': None
            })
            processed.add(i)
        
        i += 1
    
    print(f"\n📋 재정렬 결과: {len(reordered)}개 항목")
    for idx, item in enumerate(reordered):
        print(f"  {idx}: {item['type']} - {item['data']['first_cell'][:30]}...")
    
    return reordered


def has_numbered_pattern(row_info):
    """
    Check if a row has numbered pattern like 1), 2), etc.
    """
    if not row_info['hierarchy_items']:
        return False
    
    for item in row_info['hierarchy_items']:
        if item['symbol'] in ['N)', 'N.', 'N-', '(N)', '[N]', 'N:', 'N번']:
            return True
    
    return False


def is_same_or_higher_level(current_row, next_row):
    """
    Check if next row is at same or higher hierarchy level than current.
    """
    # Special markers usually indicate top level
    special_markers = ['→', '▶', '◆', '■']
    
    current_has_special = any(m in current_row['first_cell'] for m in special_markers)
    next_has_special = any(m in next_row['first_cell'] for m in special_markers)
    
    if next_has_special:
        return True
    
    # Compare hierarchy symbols if available
    if current_row['hierarchy_symbol'] and next_row['hierarchy_symbol']:
        # This is simplified - you might want more sophisticated comparison
        return True
    
    return False


def extract_symbol_pattern(line):
    """
    Extract the leading symbol pattern from a line.
    Returns the symbol pattern or None.
    Now includes all possible patterns without predefined hierarchy levels.
    Enhanced to recognize string+number patterns like TP 1, CH 2, etc.
    Also handles special Unicode spaces.
    """
    # Remove all types of Unicode spaces first
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
    
    # Replace all special spaces with regular space
    for space in unicode_spaces:
        line = line.replace(space, ' ')
    
    # Now strip normally
    line = line.strip()
    
    # Debug: check if special spaces were found
    original_line = line
    if any(space in original_line for space in unicode_spaces):
        print(f"⚠️ 특수 공백 발견 및 제거: '{repr(original_line)}' -> '{line}'")
    
    # Comprehensive patterns list (without level assignment)
    patterns = [
        # === NEW: String + Number patterns (높은 우선순위) ===
        # 영문자 + 숫자 (공백 포함/미포함)
        (r'^([A-Z]+)\s*(\d+)\s*$', 'ALPHA_NUM'),  # TP 1, TP1, CH 2, RX3 등 (대문자만, 전체 매칭)
        (r'^([a-z]+)\s*(\d+)\s*$', 'alpha_num'),  # tp 1, tp1, ch 2 등 (소문자만, 전체 매칭)
        (r'^([A-Za-z]+)\s*(\d+)\s*$', 'Alpha_Num'),  # 대소문자 혼합, 전체 매칭
        
        # 한글 + 숫자 (공백 포함/미포함)
        (r'^([가-힣]+)\s*(\d+)\s*$', '한글_숫자'),  # 채널 1, 채널1 등 (전체 매칭)
        
        # 영문자 + 숫자가 라인 시작부분에만 있는 경우 (뒤에 다른 내용 허용)
        (r'^([A-Z]+)\s*(\d+)\s+', 'ALPHA_NUM_PREFIX'),  # TP 1 설명, CH 2 내용 등
        (r'^([a-z]+)\s*(\d+)\s+', 'alpha_num_prefix'),
        (r'^([가-힣]+)\s*(\d+)\s+', '한글_숫자_PREFIX'),
        
        # === 기존 패턴들 ===
        # Arrows and triangles
        (r'^(→)\s*', '→'),
        (r'^(▶)\s*', '▶'),
        (r'^(▷)\s*', '▷'),
        (r'^(◆)\s*', '◆'),
        (r'^(◇)\s*', '◇'),
        (r'^(▲)\s*', '▲'),
        (r'^(△)\s*', '△'),
        (r'^(▼)\s*', '▼'),
        (r'^(▽)\s*', '▽'),
        (r'^(◀)\s*', '◀'),
        (r'^(◁)\s*', '◁'),
        (r'^(⇒)\s*', '⇒'),
        (r'^(⇨)\s*', '⇨'),
        (r'^(➜)\s*', '➜'),
        (r'^(➤)\s*', '➤'),
        (r'^(⟶)\s*', '⟶'),
        
        # Squares and rectangles
        (r'^(■)\s*', '■'),
        (r'^(□)\s*', '□'),
        (r'^(▪)\s*', '▪'),
        (r'^(▫)\s*', '▫'),
        (r'^(◼)\s*', '◼'),
        (r'^(◻)\s*', '◻'),
        (r'^(▣)\s*', '▣'),
        (r'^(▤)\s*', '▤'),
        
        # Circles
        (r'^(●)\s*', '●'),
        (r'^(○)\s*', '○'),
        (r'^(•)\s*', '•'),
        (r'^(◦)\s*', '◦'),
        (r'^(∙)\s*', '∙'),
        (r'^(·)\s*', '·'),
        (r'^(⦁)\s*', '⦁'),
        (r'^(◉)\s*', '◉'),
        (r'^(◎)\s*', '◎'),
        (r'^(⊙)\s*', '⊙'),
        
        # Dashes and lines
        (r'^(-)\s+', '-'),  # Dash with space
        (r'^(–)\s*', '–'),  # En dash
        (r'^(—)\s*', '—'),  # Em dash
        (r'^(―)\s*', '―'),  # Horizontal bar
        (r'^(_)\s*', '_'),  # Underscore
        (r'^(＿)\s*', '＿'),  # Full-width underscore
        
        # Stars and asterisks
        (r'^(\*)\s*', '*'),
        (r'^(★)\s*', '★'),
        (r'^(☆)\s*', '☆'),
        (r'^(✦)\s*', '✦'),
        (r'^(✧)\s*', '✧'),
        (r'^(✪)\s*', '✪'),
        (r'^(✫)\s*', '✫'),
        
        # Check marks and X marks
        (r'^(✓)\s*', '✓'),
        (r'^(✔)\s*', '✔'),
        (r'^(☑)\s*', '☑'),
        (r'^(☐)\s*', '☐'),
        (r'^(✗)\s*', '✗'),
        (r'^(✘)\s*', '✘'),
        (r'^(☒)\s*', '☒'),
        
        # Plus and cross
        (r'^(\+)\s*', '+'),
        (r'^(×)\s*', '×'),
        (r'^(✕)\s*', '✕'),
        (r'^(✖)\s*', '✖'),
        
        # Other symbols
        (r'^(>)\s*', '>'),
        (r'^(<)\s*', '<'),
        (r'^(>>)\s*', '>>'),
        (r'^(<<)\s*', '<<'),
        (r'^(※)\s*', '※'),
        (r'^(♦)\s*', '♦'),
        (r'^(♢)\s*', '♢'),
        (r'^(♠)\s*', '♠'),
        (r'^(♣)\s*', '♣'),
        (r'^(♥)\s*', '♥'),
        (r'^(♡)\s*', '♡'),
        (r'^(§)\s*', '§'),
        (r'^(¶)\s*', '¶'),
        (r'^(†)\s*', '†'),
        (r'^(‡)\s*', '‡'),
        (r'^(‖)\s*', '‖'),
        (r'^(¤)\s*', '¤'),
        (r'^(◈)\s*', '◈'),
        (r'^(◊)\s*', '◊'),
        (r'^(⊚)\s*', '⊚'),
        (r'^(⊛)\s*', '⊛'),
        (r'^(⊕)\s*', '⊕'),
        (r'^(⊖)\s*', '⊖'),
        (r'^(⊗)\s*', '⊗'),
        (r'^(⊘)\s*', '⊘'),
        
        # Numbered patterns
        (r'^(\d+)\)\s*', 'N)'),  # 1), 2), etc.
        (r'^(\d+)\.\s*', 'N.'),  # 1., 2., etc.
        (r'^(\d+)-\s*', 'N-'),  # 1-, 2-, etc.
        (r'^\((\d+)\)\s*', '(N)'),  # (1), (2), etc.
        (r'^\[(\d+)\]\s*', '[N]'),  # [1], [2], etc.
        (r'^(\d+)\s+', 'N '),  # 1 , 2 , etc. (number with space)
        (r'^(\d+):\s*', 'N:'),  # 1:, 2:, etc.
        (r'^(\d+)번\s*', 'N번'),  # 1번, 2번, etc.
        (r'^제(\d+)\s*', '제N'),  # 제1, 제2, etc.
        
        # Letter patterns (Korean)
        (r'^([가-힣])\)\s*', '가)'),  # 가), 나), etc.
        (r'^([가-힣])\.\s*', '가.'),  # 가., 나., etc.
        (r'^\(([가-힣])\)\s*', '(가)'),  # (가), (나), etc.
        (r'^\[([가-힣])\]\s*', '[가]'),  # [가], [나], etc.
        (r'^([ㄱ-ㅎ])\)\s*', 'ㄱ)'),  # ㄱ), ㄴ), etc.
        (r'^([ㄱ-ㅎ])\.\s*', 'ㄱ.'),  # ㄱ., ㄴ., etc.
        
        # Letter patterns (English)
        (r'^([a-z])\)\s*', 'a)'),  # a), b), etc.
        (r'^([A-Z])\)\s*', 'A)'),  # A), B), etc.
        (r'^([a-z])\.\s*', 'a.'),  # a., b., etc.
        (r'^([A-Z])\.\s*', 'A.'),  # A., B., etc.
        (r'^\(([a-z])\)\s*', '(a)'),  # (a), (b), etc.
        (r'^\(([A-Z])\)\s*', '(A)'),  # (A), (B), etc.
        (r'^\[([a-z])\]\s*', '[a]'),  # [a], [b], etc.
        (r'^\[([A-Z])\]\s*', '[A]'),  # [A], [B], etc.
        
        # Roman numerals
        (r'^([ivxlcdm]+)\)\s*', 'i)'),  # i), ii), etc.
        (r'^([IVXLCDM]+)\)\s*', 'I)'),  # I), II), etc.
        (r'^([ivxlcdm]+)\.\s*', 'i.'),  # i., ii., etc.
        (r'^([IVXLCDM]+)\.\s*', 'I.'),  # I., II., etc.
        (r'^\(([ivxlcdm]+)\)\s*', '(i)'),  # (i), (ii), etc.
        (r'^\(([IVXLCDM]+)\)\s*', '(I)'),  # (I), (II), etc.
        
        # Circled numbers and letters
        (r'^(①)\s*', '①'),
        (r'^(②)\s*', '②'),
        (r'^(③)\s*', '③'),
        (r'^(④)\s*', '④'),
        (r'^(⑤)\s*', '⑤'),
        (r'^(⑥)\s*', '⑥'),
        (r'^(⑦)\s*', '⑦'),
        (r'^(⑧)\s*', '⑧'),
        (r'^(⑨)\s*', '⑨'),
        (r'^(⑩)\s*', '⑩'),
        (r'^(⑪)\s*', '⑪'),
        (r'^(⑫)\s*', '⑫'),
        (r'^(⑬)\s*', '⑬'),
        (r'^(⑭)\s*', '⑭'),
        (r'^(⑮)\s*', '⑮'),
        (r'^(⑯)\s*', '⑯'),
        (r'^(⑰)\s*', '⑰'),
        (r'^(⑱)\s*', '⑱'),
        (r'^(⑲)\s*', '⑲'),
        (r'^(⑳)\s*', '⑳'),
        (r'^(ⓐ)\s*', 'ⓐ'),
        (r'^(ⓑ)\s*', 'ⓑ'),
        (r'^(ⓒ)\s*', 'ⓒ'),
        (r'^(ⓓ)\s*', 'ⓓ'),
        (r'^(Ⓐ)\s*', 'Ⓐ'),
        (r'^(Ⓑ)\s*', 'Ⓑ'),
        (r'^(Ⓒ)\s*', 'Ⓒ'),
        (r'^(Ⓓ)\s*', 'Ⓓ'),
        
        # Parenthesized letters
        (r'^(⑴)\s*', '⑴'),
        (r'^(⑵)\s*', '⑵'),
        (r'^(⑶)\s*', '⑶'),
        (r'^(⑷)\s*', '⑷'),
        (r'^(⑸)\s*', '⑸'),
        
        # Special brackets
        (r'^(\{[^}]+\})\s*', '{}'),  # {text} pattern
        (r'^(\[[^]]+\])\s*', '[]'),  # [text] pattern
        (r'^(\([^)]+\))\s*', '()'),  # (text) pattern
        (r'^(《[^》]+》)\s*', '《》'),  # 《text》 pattern
        (r'^(〈[^〉]+〉)\s*', '〈〉'),  # 〈text〉 pattern
        (r'^(【[^】]+】)\s*', '【】'),  # 【text】 pattern
        (r'^(〔[^〕]+〕)\s*', '〔〕'),  # 〔text〕 pattern
        
        # Other special patterns
        (r'^(#)\s*', '#'),
        (r'^(##)\s*', '##'),
        (r'^(###)\s*', '###'),
        (r'^(@)\s*', '@'),
        (r'^(&)\s*', '&'),
        (r'^(%)\s*', '%'),
        (r'^(\$)\s*', '$'),
        (r'^(！)\s*', '！'),
        (r'^(？)\s*', '？'),
        (r'^(：)\s*', '：'),
        (r'^(；)\s*', '；'),
        (r'^(、)\s*', '、'),
        (r'^(。)\s*', '。'),
        (r'^(，)\s*', '，'),
        (r'^(．)\s*', '．'),
        (r'^(・)\s*', '・'),
        (r'^(…)\s*', '…'),
        (r'^(‥)\s*', '‥'),
        (r'^(「[^」]+」)\s*', '「」'),
        (r'^(『[^』]+』)\s*', '『』'),
    ]
    
    # Debug output for string+number patterns
    if re.match(r'^[A-Za-z가-힣]+\s*\d+', line):
        print(f"🔍 문자열+숫자 패턴 감지: '{line}'")
    
    for pattern, symbol_type in patterns:
        match = re.match(pattern, line)
        if match:
            # Special handling for string+number patterns
            if symbol_type in ['ALPHA_NUM', 'alpha_num', 'Alpha_Num', '한글_숫자', 
                              'ALPHA_NUM_PREFIX', 'alpha_num_prefix', '한글_숫자_PREFIX']:
                print(f"✅ 문자열+숫자 패턴 매칭: '{line}' -> {symbol_type}")
            return symbol_type
    
    return None