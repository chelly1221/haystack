import pdfplumber
import re
from transformers import AutoTokenizer
from decimal import Decimal

def split_pdf_by_token_window(pdf_path, top_margin_ratio=0, bottom_margin_ratio=0,
                              window_size=700, overlap=100,
                              model_name="./models/KURE-v1"):
    """
    Splits PDF content into token-based chunks using the BGE/KURE tokenizer.
    Includes estimated start_page for each chunk.
    """
    tokenizer = AutoTokenizer.from_pretrained(model_name, local_files_only=True)
    page_texts = clean_text_by_fixed_margins(pdf_path, top_margin_ratio, bottom_margin_ratio)

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

def split_pdf_by_pages(pdf_path, top_margin_ratio=0, bottom_margin_ratio=0):
    page_texts = clean_text_by_fixed_margins(pdf_path, top_margin_ratio, bottom_margin_ratio)
    pages = []
    for i, text in enumerate(page_texts, start=1):
        pages.append({
            "content": text.strip(),
            "page_number": i
        })
    return pages

def split_pdf_by_section_headings(pdf_path, pattern=None,
                                  top_margin_ratio=0.1, bottom_margin_ratio=0.1):
    import re
    import pdfplumber

    ############################################
    # 1) Heuristic function to merge lines
    ############################################
    def heuristic_join_lines(lines):
        """
        Merges lines that look like wrapped text,
        preserves blank lines and bullet lines as real breaks.
        """
        bullet_or_heading_pattern = r'^(?:[*\-]|[0-9]+\.)\s+'
        final_lines = []
        paragraph_open = False

        for raw_line in lines:
            line = raw_line.strip()
            if not line:
                # blank => new paragraph
                final_lines.append("")
                paragraph_open = False
                continue

            # bullet or short heading line?
            if re.match(bullet_or_heading_pattern, line):
                # Force a paragraph break before it
                final_lines.append("")     # old paragraph ends
                final_lines.append(line)   # keep bullet line alone
                paragraph_open = False
            else:
                # normal line (likely a wrap)
                if not paragraph_open:
                    # start new paragraph
                    final_lines.append(line)
                    paragraph_open = True
                else:
                    # join with space
                    final_lines[-1] += " " + line

        # Convert final_lines → paragraphs separated by double newlines
        paragraphs = []
        for ln in final_lines:
            if ln == "":
                # paragraph break
                paragraphs.append("")
            else:
                # a line in the same paragraph
                if paragraphs and paragraphs[-1] != "":
                    paragraphs[-1] += "\n" + ln
                else:
                    paragraphs.append(ln)

        return "\n\n".join(paragraphs).strip()

    ############################################
    # 2) Default pattern: anchor headings to line starts
    ############################################
    if pattern is None:
        # Matches headings that start with e.g. "1.2.3 Some text" or "제 2 장 ..."
        pattern = (
            r'(?:^|\n)\s*'  # line start or newline
            r'(?:\d{1,2}(?:\.\d{1,2}){0,3}\.?[\s]+[^\s\d].+)'  # numeric heading
            r'|(?:^|\n)\s*'
            r'(?:제\s+\d{1,3}\s+장\s*[^\n]*)'                 # "제 N 장" heading
        )

    ############################################
    # 3) Extract lines from PDF (with margins)
    ############################################
    from . import clean_text_by_fixed_margins  # or inline your margin function
    page_texts = clean_text_by_fixed_margins(pdf_path, top_margin_ratio, bottom_margin_ratio)

    # Collect all matches
    matches = []
    for page_idx, page_text in enumerate(page_texts):
        # Use re.MULTILINE so ^ matches line starts within text
        for match in re.finditer(pattern, page_text, flags=re.MULTILINE):
            matches.append({
                "match": match,
                "page_idx": page_idx,
                "text": page_text,
            })

    # Sort matches by their absolute position
    # (helpful if the pattern picks up multiple lines on a single page)
    matches.sort(key=lambda m: (m["page_idx"], m["match"].start()))

    ############################################
    # 4) Prepare for section building
    ############################################
    sections = []
    section_hierarchy = {}
    seen_section_numbers = set()
    collect = False
    last_valid_number = None
    merged_out_of_order = set()

    def parse_number(n):
        return [int(p) for p in n.split(".") if p.isdigit()]

    def is_strictly_next(prev, current):
        if not prev:
            return current == [1, 1, 1]

        if len(current) == len(prev) and current[:-1] == prev[:-1]:
            return current[-1] == prev[-1] + 1

        for level in range(len(prev)):
            if level >= len(current):
                return False
            if current[level] == prev[level]:
                continue
            if current[level] == prev[level] + 1:
                return all(c == 1 for c in current[level + 1:])
            return False
        return False

    ############################################
    # 5) Build sections from matches
    ############################################
    for idx, item in enumerate(matches):
        match = item["match"]
        page_idx = item["page_idx"]
        page_text = item["text"]

        # The raw heading text
        section_title_raw = re.sub(r'\s+', ' ', match.group().strip())

        # For the slice from the heading to the next heading
        next_start = matches[idx + 1]["match"].start() if (idx + 1 < len(matches)) else len(page_text)
        lines = []

        # Next heading info
        next_page_idx = matches[idx + 1]["page_idx"] if idx + 1 < len(matches) else len(page_texts)
        next_match_start = matches[idx + 1]["match"].start() if idx + 1 < len(matches) else None
        next_page_text = page_texts[next_page_idx] if next_page_idx < len(page_texts) else ""

        # 1) Gather lines from current heading → end of this page
        lines.extend(page_text[match.end():].splitlines())

        # 2) Full pages in between
        for i in range(page_idx + 1, next_page_idx):
            lines.extend(page_texts[i].splitlines())

        # 3) Partial next page up to next heading
        if next_page_idx > page_idx and next_match_start is not None:
            lines.extend(next_page_text[:next_match_start].splitlines())

        # Convert lines → final content
        section_content = heuristic_join_lines(lines)

        # Check if heading is Korean "제 N 장"
        kor_match = re.match(r'제\s+\d{1,3}\s+장', section_title_raw)
        if kor_match:
            is_korean_chapter = True
            section_number = kor_match.group(0).strip()
            section_number_list = [int(s) for s in re.findall(r'\d+', section_number)]
        else:
            # numeric heading
            is_korean_chapter = False
            sec_num_match = re.match(r'(\d{1,2}(?:\.\d{1,2}){0,3})', section_title_raw)
            if not sec_num_match:
                # skip if not a valid heading
                continue
            section_number = sec_num_match.group(1)
            section_number_list = parse_number(section_number)
            # skip 3.x with >2 levels if desired
            if section_number_list[0] == 3 and len(section_number_list) > 2:
                if sections:
                    sections[-1]["content"] += "\n\n" + section_content
                continue

        # Duplicate heading?
        if section_number in seen_section_numbers:
            print(f"⚠️ Duplicate section {section_number} on page {page_idx + 1}, skipping.")
            continue

        # Start collecting only after "1.1.1"
        if not collect:
            if section_number == "1.1.1":
                collect = True
                last_valid_number = section_number_list
            else:
                continue

        # If out-of-order, merge into previous
        if section_number != "1.1.1":
            if not is_korean_chapter and not is_strictly_next(last_valid_number, section_number_list):
                if section_number in merged_out_of_order:
                    print(f"⏭️ Already merged out-of-order heading {section_number}, skipping again.")
                    continue
                print(f"📎 Merging out-of-order heading {section_number} (page {page_idx + 1}) into previous section")
                merged_text = section_title_raw + "\n" + section_content
                if sections:
                    if merged_text not in sections[-1]["content"]:
                        sections[-1]["content"] += "\n\n" + merged_text
                        merged_out_of_order.add(section_number)
                continue

        # Mark as seen
        seen_section_numbers.add(section_number)
        last_valid_number = section_number_list

        # Build hierarchy
        depth = section_number.count(".") + 1
        section_hierarchy[depth] = (section_number, section_title_raw)
        for d in list(section_hierarchy):
            if d > depth:
                del section_hierarchy[d]
        display_title = section_title_raw
        if depth > 1:
            parent = section_hierarchy.get(depth - 1)
            display_title = parent[1] if parent else section_title_raw

        # add final
        start_page = page_idx + 1
        if len(section_content.strip()) < 1:
            continue

        print(f"✅ Added section: {section_number} (page {start_page})")
        sections.append({
            "title": display_title,
            "section_id": section_number,
            "content": section_content.strip(),
            "start_page": start_page
        })

    return sections

def clean_text_by_fixed_margins(pdf_path, top_margin_ratio=0.15, bottom_margin_ratio=0.1):
    top_margin_ratio = float(top_margin_ratio)
    bottom_margin_ratio = float(bottom_margin_ratio)

    cleaned_pages = []
    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            page_height = float(page.height)
            y0 = page_height * bottom_margin_ratio       # cut bottom
            y1 = page_height * (1 - top_margin_ratio)    # cut top

            try:
                cropped = page.within_bbox((0, y0, float(page.width), y1))
                text = cropped.extract_text(x_tolerance=1, y_tolerance=1)
                if text is None or len(text.strip()) == 0:
                    print(f"⚠️ Fallback on page {page.page_number}: cropped text is empty")
                    text = "[CROPPING FAILED]"
            except Exception as e:
                print(f"❌ Exception on page {page.page_number}: {e}")
                text = page.extract_text() or ""

            cleaned_pages.append(text.strip())
    return cleaned_pages
