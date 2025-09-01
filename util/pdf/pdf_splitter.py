import pdfplumber
import re
from transformers import AutoTokenizer
from decimal import Decimal
import hashlib
from collections import Counter, defaultdict
import os
import uuid
import numpy as np
from datetime import datetime
from .pdf_image_extractor import extract_images_from_pdf, insert_images_in_text
from .pdf_table_extractor import extract_tables_as_text
from .pdf_text_processor import extract_page_content_with_tables


class HeaderFooterDetector:
    """PDF 문서의 머리말/꼬리말을 자동으로 탐지하는 클래스"""
    
    def __init__(self, pdf_path):
        self.pdf_path = pdf_path
        self.header_regions = {}  # page_num: margin_ratio
        self.footer_regions = {}  # page_num: margin_ratio
    
    def detect_header_footer_regions(self):
        """텍스트 패턴을 기반으로 머리말/꼬리말 영역 탐지"""
        with pdfplumber.open(self.pdf_path) as pdf:
            total_pages = len(pdf.pages)
            
            # 각 페이지의 상단/하단 3줄씩 수집
            top_lines = [[], [], []]  # 상단 1줄, 2줄, 3줄
            bottom_lines = [[], [], []]  # 하단 1줄, 2줄, 3줄
            page_infos = []
            
            for page_num, page in enumerate(pdf.pages):
                page_height = float(page.height)
                page_width = float(page.width)
                
                page_data = {
                    "page_num": page_num + 1,
                    "height": page_height,
                    "width": page_width
                }
                
                # 페이지의 각 줄을 Y 위치와 함께 추출
                lines_with_y = self._extract_lines_with_positions(page)
                
                if lines_with_y:
                    # 상단 3줄 수집
                    for i in range(min(3, len(lines_with_y))):
                        line_info = lines_with_y[i]
                        top_lines[i].append({
                            "page_num": page_num,
                            "text": line_info["text"],
                            "y_start": line_info["y_start"],
                            "y_end": line_info["y_end"],
                            "avg_y": line_info.get("avg_y", (line_info["y_start"] + line_info["y_end"]) / 2)
                        })
                    
                    # 하단 3줄 수집 (역순)
                    for i in range(min(3, len(lines_with_y))):
                        line_info = lines_with_y[-(i+1)]
                        bottom_lines[i].append({
                            "page_num": page_num,
                            "text": line_info["text"],
                            "y_start": line_info["y_start"],
                            "y_end": line_info["y_end"],
                            "avg_y": line_info.get("avg_y", (line_info["y_start"] + line_info["y_end"]) / 2)
                        })
                
                page_infos.append(page_data)
            
            # 각 줄별로 반복 패턴 분석
            header_separator_line = self._find_separator_line(top_lines, total_pages, is_header=True)
            footer_separator_line = self._find_separator_line(bottom_lines, total_pages, is_header=False)
            
            # 탐지된 구분선으로 영역 설정
            self._set_regions_from_separator_lines(
                pdf, page_infos, header_separator_line, footer_separator_line
            )
    
    def _extract_lines_with_positions(self, page):
        """페이지에서 각 줄을 Y 위치와 함께 추출 - 개선된 버전"""
        try:
            chars = page.chars
            if not chars:
                return []
            
            # 1. 먼저 모든 문자의 평균 높이 계산
            char_heights = [char['height'] for char in chars if 'height' in char]
            avg_char_height = sum(char_heights) / len(char_heights) if char_heights else 12
            
            # 2. 동적 여유값 설정 (평균 문자 높이의 20%)
            line_tolerance = avg_char_height * 0.2
            
            # 3. Y 위치별로 문자 그룹화 (더 유연한 그룹화)
            y_groups = defaultdict(list)
            for char in chars:
                # 근처 Y 그룹 찾기
                y_pos = char['top']
                grouped = False
                
                # 기존 그룹들과 비교하여 가까운 그룹에 추가
                for existing_y in list(y_groups.keys()):
                    if abs(y_pos - existing_y) <= line_tolerance:
                        y_groups[existing_y].append(char)
                        grouped = True
                        break
                
                # 가까운 그룹이 없으면 새 그룹 생성
                if not grouped:
                    y_groups[y_pos].append(char)
            
            # 4. 각 그룹을 실제 Y 위치로 정규화
            normalized_groups = defaultdict(list)
            for y_key, chars_in_group in y_groups.items():
                if chars_in_group:
                    # 그룹 내 평균 Y 위치 계산
                    avg_y = sum(c['top'] for c in chars_in_group) / len(chars_in_group)
                    normalized_groups[avg_y] = chars_in_group
            
            # 5. 각 줄을 정리
            lines = []
            for y_pos in sorted(normalized_groups.keys()):
                line_chars = sorted(normalized_groups[y_pos], key=lambda c: c['x0'])
                
                # 줄 텍스트 생성
                line_text = ""
                last_x1 = None
                for char in line_chars:
                    if last_x1 is not None and char['x0'] - last_x1 > char['width'] * 0.3:
                        line_text += " "
                    line_text += char['text']
                    last_x1 = char['x1']
                
                if line_text.strip():
                    lines.append({
                        "text": line_text.strip(),
                        "y_start": min(c['top'] for c in line_chars),
                        "y_end": max(c['bottom'] for c in line_chars),
                        "avg_y": y_pos,
                        "char_count": len(line_chars)
                    })
            
            return lines
            
        except Exception as e:
            print(f"Error extracting lines: {e}")
            return []
    
    def _find_separator_line(self, lines_by_position, total_pages, is_header=True):
        """각 줄별로 반복 패턴을 찾아 구분선 탐지 - 개선된 버전"""
        threshold = 0.4  # 40%로 낮춤 (기존 50%)
        separator_candidates = []
        
        # 각 줄 위치별로 분석
        for line_num, line_list in enumerate(lines_by_position):
            if not line_list:
                continue
            
            # 텍스트 유사성 그룹화 (완전 일치 대신 유사성 기반)
            text_groups = defaultdict(list)
            
            for item in line_list:
                text = item["text"]
                matched = False
                
                # 기존 그룹과 유사성 비교
                for group_text in list(text_groups.keys()):
                    similarity = self._calculate_text_similarity(text, group_text)
                    if similarity > 0.85:  # 85% 이상 유사하면 같은 그룹
                        text_groups[group_text].append(item)
                        matched = True
                        break
                
                if not matched:
                    text_groups[text].append(item)
            
            # 가장 많이 반복되는 텍스트 그룹 찾기
            for group_text, items in text_groups.items():
                percentage = len(items) / total_pages
                if percentage >= threshold:
                    # Y 위치 변동성 계산
                    y_positions = []
                    for item in items:
                        y_positions.append({
                            "page_num": item["page_num"],
                            "y_start": item["y_start"],
                            "y_end": item["y_end"],
                            "avg_y": item.get("avg_y", (item["y_start"] + item["y_end"]) / 2)
                        })
                    
                    # Y 위치 표준편차 계산
                    avg_ys = [pos["avg_y"] for pos in y_positions]
                    if len(avg_ys) > 1:
                        mean_y = sum(avg_ys) / len(avg_ys)
                        variance = sum((y - mean_y) ** 2 for y in avg_ys) / len(avg_ys)
                        std_dev = variance ** 0.5
                        
                        # 표준편차가 작으면 (일관된 위치) 더 높은 점수
                        consistency_score = 1 / (1 + std_dev)
                    else:
                        consistency_score = 1.0
                    
                    separator_candidates.append({
                        "line_num": line_num + 1,
                        "text": group_text,
                        "count": len(items),
                        "percentage": percentage * 100,
                        "y_positions": y_positions,
                        "consistency_score": consistency_score,
                        "total_score": percentage * consistency_score
                    })
        
        # 가장 높은 점수의 구분선 선택
        if separator_candidates:
            # total_score 기준으로 정렬
            separator_candidates.sort(key=lambda x: x["total_score"], reverse=True)
            best_candidate = separator_candidates[0]
            
            # 디버그 출력
            print(f"{'Header' if is_header else 'Footer'} separator candidates:")
            for cand in separator_candidates[:3]:  # 상위 3개만 출력
                print(f"  Line {cand['line_num']}: '{cand['text'][:50]}...' "
                      f"({cand['percentage']:.1f}%, consistency: {cand['consistency_score']:.2f})")
            
            return best_candidate
        
        return None
    
    def _calculate_text_similarity(self, text1, text2):
        """두 텍스트의 유사도 계산 (0~1)"""
        # 간단한 문자 기반 유사도 (더 정교한 알고리즘으로 대체 가능)
        if not text1 or not text2:
            return 0.0
        
        # 정규화
        text1 = text1.strip().lower()
        text2 = text2.strip().lower()
        
        if text1 == text2:
            return 1.0
        
        # 레벤슈타인 거리 기반 유사도
        distance = self._levenshtein_distance(text1, text2)
        max_len = max(len(text1), len(text2))
        
        if max_len == 0:
            return 1.0
        
        similarity = 1 - (distance / max_len)
        return max(0.0, similarity)
    
    def _levenshtein_distance(self, s1, s2):
        """레벤슈타인 거리 계산"""
        if len(s1) < len(s2):
            return self._levenshtein_distance(s2, s1)
        
        if len(s2) == 0:
            return len(s1)
        
        previous_row = range(len(s2) + 1)
        for i, c1 in enumerate(s1):
            current_row = [i + 1]
            for j, c2 in enumerate(s2):
                insertions = previous_row[j + 1] + 1
                deletions = current_row[j] + 1
                substitutions = previous_row[j] + (c1 != c2)
                current_row.append(min(insertions, deletions, substitutions))
            previous_row = current_row
        
        return previous_row[-1]
    
    def _set_regions_from_separator_lines(self, pdf, page_infos, header_separator, footer_separator):
        """탐지된 구분선을 기준으로 제거할 영역 설정 - 개선된 버전"""
        # 머리말 영역 설정
        if header_separator:
            for y_info in header_separator["y_positions"]:
                page_num = y_info["page_num"]
                if page_num < len(pdf.pages):
                    page_height = float(pdf.pages[page_num].height)
                    
                    # 동적 여유값 계산 (페이지 높이의 1% 또는 10포인트 중 큰 값)
                    dynamic_margin = max(page_height * 0.01, 10)
                    
                    # 구분선 포함하여 바깥쪽 제거
                    cut_y_from_top = y_info["y_end"] + dynamic_margin
                    margin_ratio = cut_y_from_top / page_height
                    
                    # 최대 30% 제한
                    self.header_regions[page_num] = min(margin_ratio, 0.3)
                    
                    if page_num < len(page_infos):
                        page_infos[page_num]["header_y_end"] = y_info["y_end"]
                        page_infos[page_num]["header_cut_y"] = cut_y_from_top
                        page_infos[page_num]["header_ratio"] = self.header_regions[page_num]
                        page_infos[page_num]["header_line_num"] = header_separator["line_num"]
                        page_infos[page_num]["header_margin"] = dynamic_margin
        
        # 꼬리말 영역 설정
        if footer_separator:
            for y_info in footer_separator["y_positions"]:
                page_num = y_info["page_num"]
                if page_num < len(pdf.pages):
                    page_height = float(pdf.pages[page_num].height)
                    
                    # 동적 여유값 계산
                    dynamic_margin = max(page_height * 0.01, 10)
                    
                    # 구분선 포함하여 바깥쪽 제거
                    cut_y_from_top = y_info["y_start"] - dynamic_margin
                    margin_ratio = (page_height - cut_y_from_top) / page_height
                    
                    # 최대 30% 제한
                    self.footer_regions[page_num] = min(margin_ratio, 0.3)
                    
                    if page_num < len(page_infos):
                        page_infos[page_num]["footer_y_start"] = y_info["y_start"]
                        page_infos[page_num]["footer_cut_y"] = cut_y_from_top
                        page_infos[page_num]["footer_ratio"] = self.footer_regions[page_num]
                        page_infos[page_num]["footer_line_num"] = footer_separator["line_num"]
                        page_infos[page_num]["footer_margin"] = dynamic_margin
    
    def _find_separated_texts(self, page):
        """페이지에서 본문과 분리된 머리말/꼬리말 텍스트 찾기"""
        result = {"header": None, "footer": None}
        
        try:
            chars = page.chars
            if not chars:
                return result
            
            page_height = float(page.height)
            
            # Y 위치별로 문자 그룹화 (pdfplumber 좌표계: 위에서부터)
            y_groups = defaultdict(list)
            for char in chars:
                # pdfplumber에서 char['top']은 이미 위에서부터의 거리
                y_key = round(char['top'], 3)
                y_groups[y_key].append(char)
            
            # Y 위치 정렬 (위에서 아래로)
            sorted_y_positions = sorted(y_groups.keys())
            if not sorted_y_positions:
                return result
            
            # 텍스트 블록 찾기 (연속된 Y 위치를 하나의 블록으로)
            text_blocks = []
            current_block = []
            last_y = None
            
            for y_pos in sorted_y_positions:
                if last_y is None or y_pos - last_y < 15:  # 15포인트 이내면 같은 블록
                    current_block.append(y_pos)
                else:
                    if current_block:
                        text_blocks.append(current_block)
                    current_block = [y_pos]
                last_y = y_pos
            
            if current_block:
                text_blocks.append(current_block)
            
            # 머리말 찾기: 첫 번째 블록이 충분히 분리되어 있는지 확인
            if len(text_blocks) >= 2:
                first_block = text_blocks[0]
                second_block = text_blocks[1]
                
                # 첫 블록의 끝과 두 번째 블록의 시작 사이 간격
                gap = min(second_block) - max(first_block)
                
                if gap > 30:  # 30포인트 이상 떨어져 있으면 머리말로 간주
                    # 첫 블록의 텍스트 추출
                    header_chars = []
                    for y in first_block:
                        header_chars.extend(y_groups[y])
                    
                    header_chars.sort(key=lambda c: (c['top'], c['x0']))
                    header_text = self._chars_to_text(header_chars)
                    
                    if header_text.strip():
                        result["header"] = {
                            "text": header_text.strip(),
                            "y_start": min(c['top'] for c in header_chars),  # 위에서부터
                            "y_end": max(c['bottom'] for c in header_chars)  # 위에서부터
                        }
            
            # 꼬리말 찾기: 마지막 블록이 충분히 분리되어 있는지 확인
            if len(text_blocks) >= 2:
                last_block = text_blocks[-1]
                second_last_block = text_blocks[-2]
                
                # 마지막 블록과 그 이전 블록 사이 간격
                gap = min(last_block) - max(second_last_block)
                
                if gap > 30:  # 30포인트 이상 떨어져 있으면 꼬리말로 간주
                    # 마지막 블록의 텍스트 추출
                    footer_chars = []
                    for y in last_block:
                        footer_chars.extend(y_groups[y])
                    
                    footer_chars.sort(key=lambda c: (c['top'], c['x0']))
                    footer_text = self._chars_to_text(footer_chars)
                    
                    if footer_text.strip():
                        result["footer"] = {
                            "text": footer_text.strip(),
                            "y_start": min(c['top'] for c in footer_chars),  # 위에서부터
                            "y_end": max(c['bottom'] for c in footer_chars)  # 위에서부터
                        }
            
            return result
            
        except Exception as e:
            return result
    
    def _chars_to_text(self, chars):
        """문자 리스트를 텍스트로 변환"""
        if not chars:
            return ""
        
        # Y 위치별로 그룹화하여 줄 단위로 처리 (더 정밀한 소수점)
        lines = defaultdict(list)
        for char in chars:
            y_key = round(char['top'], 3)  # 소수점 3자리
            lines[y_key].append(char)
        
        # 각 줄을 텍스트로 변환
        text_lines = []
        for y_pos in sorted(lines.keys()):
            line_chars = sorted(lines[y_pos], key=lambda c: c['x0'])
            line_text = ""
            last_x1 = None
            
            for char in line_chars:
                # 단어 사이 공백 추가
                if last_x1 is not None and char['x0'] - last_x1 > char['width'] * 0.3:
                    line_text += " "
                line_text += char['text']
                last_x1 = char['x1']
            
            text_lines.append(line_text)
        
        return "\n".join(text_lines)
    
    def get_margin_ratios(self):
        """탐지된 영역을 margin ratio로 변환"""
        self.detect_header_footer_regions()
        
        # 평균값 계산 (더 정밀한 소수점)
        avg_top_margin = 0.0
        avg_bottom_margin = 0.0
        
        if self.header_regions:
            # numpy를 사용하지 않고 정밀한 평균 계산
            total = sum(self.header_regions.values())
            avg_top_margin = total / len(self.header_regions)
        
        if self.footer_regions:
            total = sum(self.footer_regions.values())
            avg_bottom_margin = total / len(self.footer_regions)
        
        return avg_top_margin, avg_bottom_margin


def auto_detect_margins(pdf_path):
    """PDF의 머리말/꼬리말 영역을 자동으로 탐지하여 margin ratio 반환"""
    detector = HeaderFooterDetector(pdf_path)
    top_margin, bottom_margin = detector.get_margin_ratios()
    
    print(f"🔍 자동 탐지된 margin - 상단: {top_margin*100:.3f}%, 하단: {bottom_margin*100:.3f}%")
    
    return top_margin, bottom_margin


def clean_text_by_fixed_margins_with_tables(pdf_path, top_margin_ratio=0.15, bottom_margin_ratio=0.1, 
                                           image_map=None, extract_tables_as_text_flag=True,
                                           auto_detect_header_footer=True):
    """
    Clean text by removing margins and optionally insert image tags and text tables.
    Enhanced to extract text and tables separately, then combine them in order.
    Now with automatic header/footer detection option.
    """
    # 자동 머리말/꼬리말 탐지
    if auto_detect_header_footer:
        detected_top, detected_bottom = auto_detect_margins(pdf_path)
        # 자동 탐지된 값이 있으면 사용, 없으면 기본값 사용
        if detected_top > 0:
            top_margin_ratio = detected_top
        if detected_bottom > 0:
            bottom_margin_ratio = detected_bottom
    
    top_margin_ratio = float(top_margin_ratio)
    bottom_margin_ratio = float(bottom_margin_ratio)
    
    cleaned_pages = []
    
    with pdfplumber.open(pdf_path) as pdf:
        for page_num, page in enumerate(pdf.pages, start=1):
            page_height = float(page.height)
            
            # PDF 좌표계: bottom은 아래에서부터, top은 위에서부터
            # bottom_margin_ratio가 0.1이면 아래 10%를 제거
            # top_margin_ratio가 0.1이면 위 10%를 제거
            
            # pdfplumber의 bbox는 (x0, y0, x1, y1) 형식
            # y0는 위에서부터의 거리, y1은 아래까지의 거리
            y0_from_top = page_height * top_margin_ratio      # 위에서부터 제거할 거리
            y1_from_top = page_height * (1 - bottom_margin_ratio)  # 위에서부터 보존할 끝 거리

            try:
                # pdfplumber의 within_bbox는 위에서부터의 좌표를 사용
                cropped = page.within_bbox((0, y0_from_top, float(page.width), y1_from_top))
                
                if extract_tables_as_text_flag:
                    # Extract text with table regions marked
                    page_content = extract_page_content_with_tables(cropped, page_num)
                else:
                    # Original text extraction
                    page_content = cropped.extract_text(x_tolerance=1, y_tolerance=1) or ""
                
                if not page_content or len(page_content.strip()) == 0:
                    print(f"⚠️ Fallback on page {page_num}: cropped text is empty")
                    page_content = "[CROPPING FAILED]"
                    
            except Exception as e:
                print(f"❌ Exception on page {page_num}: {e}")
                page_content = page.extract_text() or ""

            # Insert images if available
            if image_map and page_num in image_map:
                page_content = insert_images_in_text(page_content.strip(), page_num, image_map)
            
            cleaned_pages.append(page_content.strip())
    
    return cleaned_pages


def clean_text_by_fixed_margins(pdf_path, top_margin_ratio=0.15, bottom_margin_ratio=0.1, 
                               image_map=None, auto_detect_header_footer=True):
    """
    Original clean text function for backward compatibility.
    """
    return clean_text_by_fixed_margins_with_tables(
        pdf_path, top_margin_ratio, bottom_margin_ratio, 
        image_map, extract_tables_as_text_flag=False,
        auto_detect_header_footer=auto_detect_header_footer
    )


def split_pdf_by_token_window(pdf_path, top_margin_ratio=0, bottom_margin_ratio=0,
                              window_size=700, overlap=100,
                              model_name="./models/KURE-v1", doc_id=None,
                              extract_text_tables=True, auto_detect_header_footer=True):
    """
    Splits PDF content into token-based chunks using the BGE/KURE tokenizer.
    Includes estimated start_page for each chunk, extracts images, and text tables.
    Now with automatic header/footer detection option.
    """
    if doc_id is None:
        doc_id = str(uuid.uuid4())
    
    # Extract images first
    image_map = extract_images_from_pdf(pdf_path, doc_id)
    
    # Use modified function that includes text tables
    page_texts = clean_text_by_fixed_margins_with_tables(
        pdf_path, top_margin_ratio, bottom_margin_ratio, 
        image_map, extract_text_tables,
        auto_detect_header_footer=auto_detect_header_footer
    )
    
    tokenizer = AutoTokenizer.from_pretrained(model_name, local_files_only=True)

    # Encode each page separately to map tokens to pages
    page_token_boundaries = []
    all_tokens = []
    for i, text in enumerate(page_texts):
        tokens = tokenizer.encode(text, add_special_tokens=False)
        page_token_boundaries.append((len(all_tokens), len(all_tokens) + len(tokens), i + 1))  # (start_idx, end_idx, page_number)
        all_tokens.extend(tokens)

    chunks = []
    start = 0
    idx = 1
    total_tokens = len(all_tokens)

    while start < total_tokens:
        end = min(start + window_size, total_tokens)
        token_chunk = all_tokens[start:end]
        text_chunk = tokenizer.decode(token_chunk, skip_special_tokens=True)

        # Estimate start_page by finding the first page whose token span overlaps this chunk
        start_page = next((page_num for s_idx, e_idx, page_num in page_token_boundaries if s_idx <= start < e_idx), 1)

        chunks.append({
            "title": f"Chunk {idx}",
            "content": text_chunk,
            "start_token": start,
            "start_page": start_page
        })

        idx += 1
        start += window_size - overlap

    return chunks


def split_pdf_by_pages(pdf_path, top_margin_ratio=0, bottom_margin_ratio=0, 
                       doc_id=None, extract_text_tables=True, auto_detect_header_footer=True):
    """
    Split PDF by pages with image extraction and text tables.
    Now with automatic header/footer detection option.
    """
    if doc_id is None:
        doc_id = str(uuid.uuid4())
    
    # Extract images first
    image_map = extract_images_from_pdf(pdf_path, doc_id)
    
    # Use modified function that includes text tables
    page_texts = clean_text_by_fixed_margins_with_tables(
        pdf_path, top_margin_ratio, bottom_margin_ratio, 
        image_map, extract_text_tables,
        auto_detect_header_footer=auto_detect_header_footer
    )
    
    pages = []
    for i, text in enumerate(page_texts, start=1):
        pages.append({
            "content": text.strip(),
            "page_number": i
        })
    return pages


def split_pdf_by_section_headings(pdf_path, pattern=None, top_margin_ratio=0.1, 
                                  bottom_margin_ratio=0.1, doc_id=None,
                                  extract_text_tables=True, auto_detect_header_footer=True,
                                  document_title=None):
    """
    Split PDF by section headings with image extraction and text tables.
    Now with automatic header/footer detection option.
    """
    if doc_id is None:
        doc_id = str(uuid.uuid4())
    
    # Extract images first
    image_map = extract_images_from_pdf(pdf_path, doc_id)

    ############################################
    # 1) Heuristic function to merge lines
    ############################################
    def heuristic_join_lines(lines):
        """
        Merges wrapped lines intelligently, preserving real paragraph breaks
        (blank lines or bullet points), but avoiding excessive newlines.
        Preserves table structure by not merging lines within tables.
        """

        bullet_or_heading_pattern = (
            r'^\s*('
            r'[-*•⦁●∙·‣▪►☐・]'            # Bullet symbols including ・
            r'|[0-9]+\.'                  # Numbered list: 1. 2.
            r'|\([0-9]+\)'               # (1), (2), ...
            r'|\[[^\]]+\]'              # [텍스트], [확인], etc.
            r')\s*'
        )

        result = []
        current_paragraph = []
        in_table = False

        for raw_line in lines:
            line = raw_line.strip()

            # Check if we're entering a table
            if re.match(r'^\[표\s+\d+\s+-\s+페이지\s+\d+\s+시작\]$', line):
                # Flush current paragraph
                if current_paragraph:
                    result.append(" ".join(current_paragraph))
                    current_paragraph = []
                in_table = True
                result.append(raw_line)  # Keep table start marker
                continue
            
            # Check if we're exiting a table
            if re.match(r'^\[표\s+\d+\s+-\s+페이지\s+\d+\s+끝\]$', line):
                in_table = False
                result.append(raw_line)  # Keep table end marker
                continue
            
            # If we're in a table, preserve all formatting
            if in_table:
                result.append(raw_line)  # Keep original line with all formatting
                continue

            # Regular text processing (outside of tables)
            if any(b in line for b in ['⦁', '•', '-', '*']):
                print(f"🔍 Checking for bullet: {repr(line)}")

            # ✅ Match bullet pattern exactly
            if re.match(bullet_or_heading_pattern, line):
                print(f"✅ Bullet matched: {repr(line)}")  # debug success
                if current_paragraph:
                    result.append(" ".join(current_paragraph))
                    current_paragraph = []
                result.append(line)
            elif not line:
                # Blank line = new paragraph
                if current_paragraph:
                    result.append(" ".join(current_paragraph))
                    current_paragraph = []
                result.append("")
            else:
                current_paragraph.append(line)

        # Flush any remaining content
        if current_paragraph:
            result.append(" ".join(current_paragraph))

        # Join all lines
        return "\n".join(result)


    ############################################
    # 2) Default pattern: Updated to include 제 N 장 with flexible spaces
    ############################################
    if pattern is None:
        # Matches "제 N 장", "첨부 N" with flexible spaces, and numeric headings like "1.2.3 Some text"
        pattern = (
            r'(?m)^'
            r'(?:제\s*\d{1,3}\s*장)'  # "제 N 장" with flexible spaces
            r'|'
            r'(?:첨부\s*\d{1,3})'     # "첨부 N" with flexible spaces
            r'|'
            r'(?:\d{1,2}(?:\.\d{1,2}){1,3})'
            r'(?:\.?)\s+'
            r'(?!Radio Navigation and Landing Aids)'
            r'[^\s\d].+'
        )

    ############################################
    # 3) Extract lines from PDF (with margins, images, and text tables)
    ############################################
    page_texts = clean_text_by_fixed_margins_with_tables(
        pdf_path, top_margin_ratio, bottom_margin_ratio, 
        image_map, extract_text_tables,
        auto_detect_header_footer=auto_detect_header_footer
    )

    # Remove table content from section matching
    def remove_table_content(text):
        """Remove content between table markers"""
        # Pattern to match everything between table start and end markers
        table_pattern = r'\[표\s+\d+\s+-\s+페이지\s+\d+\s+시작\].*?\[표\s+\d+\s+-\s+페이지\s+\d+\s+끝\]'
        # Remove table content but keep the rest
        cleaned_text = re.sub(table_pattern, '', text, flags=re.DOTALL)
        return cleaned_text

    # Collect all matches excluding table content
    matches = []
    korean_chapter_count = 0  # Track count of "제 N 장" matches
    
    for page_idx, page_text in enumerate(page_texts):
        # Remove table content before matching
        cleaned_page_text = remove_table_content(page_text)
        
        # Use re.MULTILINE so ^ matches line starts within text
        for match in re.finditer(pattern, cleaned_page_text, flags=re.MULTILINE):
            # Check if this is a Korean chapter
            if re.match(r'제\s*\d{1,3}\s*장', match.group().strip()):
                korean_chapter_count += 1
                # Skip the first occurrence of "제 1 장"
                if korean_chapter_count == 1 and re.match(r'제\s*1\s*장', match.group().strip()):
                    print(f"⏭️ Skipping first occurrence of 제 1 장 on page {page_idx + 1}")
                    continue
            
            # Find the actual position in the original text (with tables)
            match_text = match.group()
            actual_position = page_text.find(match_text)
            if actual_position != -1:
                # Create a new match object with the correct position
                matches.append({
                    "match": match,
                    "page_idx": page_idx,
                    "text": page_text,  # Use original text with tables
                    "actual_start": actual_position,
                    "match_text": match_text
                })

    # Sort matches by their absolute position
    matches.sort(key=lambda m: (m["page_idx"], m["actual_start"]))
    
    # Debug: Print all found matches
    print(f"\n📋 Total matches found: {len(matches)}")
    for idx, m in enumerate(matches):
        print(f"  {idx}: {m['match_text']} (page {m['page_idx'] + 1})")

    # Count occurrences of each section title
    all_section_titles = []
    for item in matches:
        section_raw = re.sub(r'\s+', ' ', item["match_text"].strip())
        sec_match = re.match(r'(\d{1,2}(?:\.\d{1,2}){0,3})', section_raw)
        if sec_match:
            all_section_titles.append(sec_match.group(1))
        else:
            # For Korean chapters
            kor_match = re.match(r'제\s*(\d{1,3})\s*장', section_raw)
            if kor_match:
                all_section_titles.append(f"제 {kor_match.group(1)} 장")
            else:
                # For appendices
                appendix_match = re.match(r'첨부\s*(\d{1,3})', section_raw)
                if appendix_match:
                    all_section_titles.append(f"첨부 {appendix_match.group(1)}")

    section_title_counts = Counter(all_section_titles)

    ############################################
    # 4) Prepare for section building
    ############################################
    sections = []
    section_hierarchy = {}
    seen_section_numbers = set()
    collect = False
    last_valid_number = None
    merged_out_of_order = set()
    merged_content_cache = set()
    expecting_next_subsection = False  # Flag to track if we're expecting N.1 after 제 N 장
    expected_chapter_num = None  # Track which chapter number we're expecting subsections for
    in_appendix_mode = False  # Track if we've entered appendix mode
    last_appendix_num = 0  # Track last appendix number
    current_chapter_sections = set()  # Track all sections in current chapter

    def parse_number(n):
        return [int(p) for p in n.split(".") if p.isdigit()]

    def is_strictly_next(prev, current, current_chapter=None, current_chapter_sections=None):
        """Check if current section number follows the expected sequence"""
        # If we just processed a Korean chapter, any subsection of that chapter is valid
        if current_chapter is not None and len(current) >= 1 and current[0] == current_chapter:
            # Check if it's a valid subsection that we haven't seen before
            current_str = '.'.join(map(str, current))
            if current_str in current_chapter_sections:
                print(f"⚠️ Section {current_str} already exists in chapter {current_chapter}")
                return False
            return True
        
        if not prev:
            return True  # Accept any valid section number when starting

        # Case: same depth → must increment last digit
        if len(current) == len(prev):
            return current[:-1] == prev[:-1] and current[-1] == prev[-1] + 1

        # Case: one level deeper → must start with 1 (child section)
        if len(current) == len(prev) + 1:
            return current[:-1] == prev and current[-1] == 1

        # Case: back to higher level
        for i in range(min(len(prev), len(current))):
            if current[i] == prev[i]:
                continue
            if current[i] == prev[i] + 1:
                return all(c == 1 for c in current[i + 1:])
            return False

        return False

    def is_out_of_order(current, current_chapter=None, current_chapter_sections=None):
        """Check if the current section is out of order within the current chapter"""
        if current_chapter is None or len(current) < 1:
            return False
        
        # Must be in the current chapter
        if current[0] != current_chapter:
            return True
        
        # For sections like 5.20, if we see 5.1 or 5, it's out of order
        current_str = '.'.join(map(str, current))
        
        # Check all existing sections in current chapter
        for existing in current_chapter_sections:
            existing_parts = parse_number(existing)
            
            # If current has fewer parts than existing, check if it's a parent that should have come earlier
            if len(current) < len(existing_parts):
                # Check if current is a parent of existing
                if existing_parts[:len(current)] == current:
                    print(f"⚠️ Section {current_str} is out of order - should come before {existing}")
                    return True
            
            # If same depth, check if current should have come earlier
            elif len(current) == len(existing_parts):
                # Compare at same level
                if current[:-1] == existing_parts[:-1] and current[-1] < existing_parts[-1]:
                    print(f"⚠️ Section {current_str} is out of order - should come before {existing}")
                    return True
        
        return False

    def merge_out_of_order_section(section_title_raw, section_content, section_number):
        """Merge out-of-order section into previous section"""
        content_hash = hash_text(section_content)
        if sections and content_hash not in merged_content_cache:
            print(f"📎 Merging out-of-order section {section_number} into previous")
            sections[-1]["content"] += "\n\n" + section_title_raw + "\n" + section_content
            merged_content_cache.add(content_hash)
            merged_out_of_order.add(section_number)
            return True
        else:
            print(f"⏭️ Duplicate out-of-order content skipped: {section_title_raw}")
            return False

    def hash_text(text):
        """Hash section content to detect duplicates reliably."""
        return hashlib.sha256(text.strip().encode("utf-8")).hexdigest()

    ############################################
    # 5) Build sections from matches
    ############################################
    for idx, item in enumerate(matches):
        match = item["match"]
        page_idx = item["page_idx"]
        page_text = item["text"]
        actual_start = item["actual_start"]

        # The raw heading text
        section_title_raw = re.sub(r'\s+', ' ', match.group().strip())

        # Parse lines from current heading to next
        # Use actual_start instead of match.start() for correct positioning
        lines = []

        # Next heading info
        next_page_idx = matches[idx + 1]["page_idx"] if idx + 1 < len(matches) else len(page_texts)
        next_actual_start = matches[idx + 1]["actual_start"] if idx + 1 < len(matches) else None
        next_page_text = page_texts[next_page_idx] if next_page_idx < len(page_texts) else ""

        # Calculate the end position of the current match
        current_match_end = actual_start + len(item["match_text"])

        # Handle content range between this heading and the next heading
        if next_page_idx == page_idx and next_actual_start is not None:
            # Both headings are on the same page — slice between them
            lines = page_text[current_match_end:next_actual_start].splitlines()
        else:
            # 1. From heading to end of current page
            lines.extend(page_text[current_match_end:].splitlines())

            # 2. Full in-between pages
            for i in range(page_idx + 1, next_page_idx):
                lines.extend(page_texts[i].splitlines())

            # 3. Start of next page up to next heading
            if next_actual_start is not None and next_page_idx < len(page_texts):
                lines.extend(next_page_text[:next_actual_start].splitlines())

        # Convert lines → final content
        section_content = heuristic_join_lines(lines)

        # Check if heading is Korean "제 N 장"
        kor_match = re.match(r'제\s*(\d{1,3})\s*장', section_title_raw)
        if kor_match:
            is_korean_chapter = True
            is_appendix = False
            chapter_num = int(kor_match.group(1))
            section_number = f"제 {chapter_num} 장"  # Normalize format
            section_number_list = [chapter_num]  # Use chapter number for hierarchy
            
            # If this is 제 1 장, start collecting
            if chapter_num == 1:
                collect = True
                print(f"✅ Starting collection with {section_number}")
            
            # For any 제 N 장, expect N.1 next
            if collect:
                expecting_next_subsection = True
                expected_chapter_num = chapter_num
                last_valid_number = None  # Reset for new chapter sequence
                in_appendix_mode = False  # Reset appendix mode
                current_chapter_sections = set()  # Reset chapter sections
                print(f"✅ Found {section_number}, expecting {chapter_num}.1 next")
        else:
            # Check if heading is "첨부 N"
            appendix_match = re.match(r'첨부\s*(\d{1,3})', section_title_raw)
            if appendix_match:
                is_korean_chapter = False
                is_appendix = True
                appendix_num = int(appendix_match.group(1))
                section_number = f"첨부 {appendix_num}"  # Normalize format
                section_number_list = [999, appendix_num]  # Use special prefix for appendix
                
                # Only process if we're collecting and have passed chapter 6
                if collect and expected_chapter_num and expected_chapter_num >= 6:
                    in_appendix_mode = True
                    last_appendix_num = appendix_num
                    expecting_next_subsection = False  # No subsections expected after appendix
                    print(f"✅ Found {section_number} (appendix mode activated)")
                else:
                    print(f"⏭️ Skipping {section_number} - not in chapter 6+ or not collecting")
                    continue
            else:
                # numeric heading
                is_korean_chapter = False
                is_appendix = False
                sec_num_match = re.match(r'(\d{1,2}(?:\.\d{1,2}){0,3})', section_title_raw)
                if not sec_num_match:
                    # skip if not a valid heading
                    continue
                section_number = sec_num_match.group(1)
                section_number_list = parse_number(section_number)
            
            # Special handling after 제 N 장
            if expecting_next_subsection and expected_chapter_num is not None:
                # We're expecting N.1 after 제 N 장
                if len(section_number_list) >= 2 and section_number_list[0] == expected_chapter_num and section_number_list[1] == 1:
                    expecting_next_subsection = False
                    last_valid_number = section_number_list
                    current_chapter_sections.add(section_number)
                    print(f"✅ Found expected {section_number} after 제 {expected_chapter_num} 장")
                else:
                    # Check if this is a valid subsection of current chapter
                    if len(section_number_list) >= 1 and section_number_list[0] == expected_chapter_num:
                        # It's a subsection of the current chapter, check if it's in order
                        if is_out_of_order(section_number_list, expected_chapter_num, current_chapter_sections):
                            # Out of order section - merge with previous
                            if merge_out_of_order_section(section_title_raw, section_content, section_number):
                                continue
                        else:
                            expecting_next_subsection = False
                            last_valid_number = section_number_list
                            current_chapter_sections.add(section_number)
                            print(f"✅ Found subsection {section_number} of 제 {expected_chapter_num} 장")
                    else:
                        print(f"⚠️ Expected {expected_chapter_num}.x subsection but found {section_number}, skipping")
                        continue
            
            # Check if section is out of order within current chapter
            if expected_chapter_num is not None and len(section_number_list) >= 1:
                if section_number_list[0] == expected_chapter_num:
                    if is_out_of_order(section_number_list, expected_chapter_num, current_chapter_sections):
                        if merge_out_of_order_section(section_title_raw, section_content, section_number):
                            continue
            
            # Skip 3.x sections with >2 levels as before
            if section_number_list[0] == 3 and len(section_number_list) > 2:
                content_hash = hash_text(section_content)
                if sections and content_hash not in merged_content_cache:
                    print(f"📎 Merging skipped 3.x section into previous: {section_title_raw}")
                    sections[-1]["content"] += "\n\n" + section_content
                    merged_content_cache.add(content_hash)
                else:
                    print(f"⏭️ Duplicate 3.x content skipped: {section_title_raw}")
                continue

        # Only process if we've started collecting
        if not collect:
            continue

        # For numeric sections and appendices, check ordering (non-out-of-order sections)
        if not is_korean_chapter and last_valid_number is not None and not is_appendix:
            # Get current chapter number if we're in a chapter
            current_chapter_num = None
            if expected_chapter_num is not None:
                current_chapter_num = expected_chapter_num
            
            if not is_strictly_next(last_valid_number, section_number_list, current_chapter_num, current_chapter_sections):
                # Handle out-of-order sections
                if section_title_counts.get(section_number, 0) > 1:
                    print(f"⏭️ Out-of-order heading {section_number} appears multiple times, skipping early merge.")
                    continue

                if section_number in seen_section_numbers:
                    print(f"⏭️ Known section {section_number} already processed, skipping merge.")
                    continue

                if section_number in merged_out_of_order:
                    print(f"⏭️ Already merged out-of-order heading {section_number}, skipping repeat.")
                    continue

                print(f"📎 Merging out-of-order heading {section_number} (page {page_idx + 1}) into previous section")
                merged_text = section_title_raw + "\n" + section_content
                if sections:
                    merged_hash = hash_text(merged_text)
                    if merged_hash not in merged_content_cache:
                        sections[-1]["content"] += "\n\n" + merged_text
                        merged_out_of_order.add(section_number)
                        merged_content_cache.add(merged_hash)
                        seen_section_numbers.add(section_number)
                continue
        
        # For appendices, check sequence
        if is_appendix and in_appendix_mode:
            if appendix_num != last_appendix_num + 1 and last_appendix_num > 0:
                print(f"⚠️ Appendix out of order: expected 첨부 {last_appendix_num + 1} but found 첨부 {appendix_num}")
                # But still process it
            last_appendix_num = appendix_num

        # Mark as seen and update last valid number
        seen_section_numbers.add(section_number)
        if not is_korean_chapter and not is_appendix:
            last_valid_number = section_number_list
            current_chapter_sections.add(section_number)

        # Build hierarchy
        if is_korean_chapter:
            depth = 0  # Korean chapters are top level
            section_hierarchy[0] = (section_number, section_title_raw)
            # Clear lower levels when starting new chapter
            for d in list(section_hierarchy):
                if d > 0:
                    del section_hierarchy[d]
        elif is_appendix:
            depth = 0  # Appendices are also top level
            section_hierarchy[0] = (section_number, section_title_raw)
            # Clear lower levels when starting new appendix
            for d in list(section_hierarchy):
                if d > 0:
                    del section_hierarchy[d]
        else:
            depth = section_number.count(".") + 1
            section_hierarchy[depth] = (section_number, section_title_raw)
            for d in list(section_hierarchy):
                if d > depth:
                    del section_hierarchy[d]
        
        display_title = section_title_raw
        if depth > 1:
            parent = section_hierarchy.get(depth - 1)
            display_title = parent[1] if parent else section_title_raw

        # Add final section
        start_page = page_idx + 1
        if len(section_content.strip()) < 1:
            # Keep headings with depth 0, 1 or 2 even if they're empty (including appendices)
            if not is_korean_chapter and not is_appendix and len(section_number_list) >= 3:
                print(f"⏭️ Skipping empty deep-level section {section_number}")
                continue
            else:
                print(f"🪧 Keeping empty heading: {section_number}")
        
        # Build hierarchical tree for this section
        section_tree = []
        
        # Add document title if available
        if document_title:
            section_tree.append(f"📄 {document_title}")
        
        # Add all parent sections in hierarchy
        for level in sorted(section_hierarchy.keys()):
            if level <= depth:
                _, title = section_hierarchy[level]
                indent = "  " * level
                if level == depth:
                    # Current section - use filled arrow
                    section_tree.append(f"{indent}▶ {title}")
                else:
                    # Parent section - use hollow arrow
                    section_tree.append(f"{indent}▷ {title}")
        
        # Create the hierarchical tree header
        tree_header = "\n".join(section_tree)
        
        # Prepend the tree to the section content
        section_content = tree_header + "\n" + "─" * 50 + "\n\n" + section_title_raw + "\n" + section_content.strip()
        
        print(f"✅ Added section: {section_number} (page {start_page})")
        sections.append({
            "title": display_title,
            "section_id": section_number,
            "content": section_content.strip(),
            "start_page": start_page,
            "section_hierarchy": dict(section_hierarchy),
            "document_title": document_title
        })

    return sections


# Backward compatibility aliases
clean_text_by_fixed_margins_with_html_tables = clean_text_by_fixed_margins_with_tables
extract_html_tables = extract_text_tables = True