import zipfile, os, uuid, shutil
import xml.etree.ElementTree as ET
from typing import List
from lxml import etree as ET
from pathlib import Path

# 공통 네임스페이스
NAMESPACES = {
    'hp': 'http://www.hancom.co.kr/hwpml/2011/paragraph',
    'hc': 'http://www.hancom.co.kr/hwpml/2011/core'
}

def extract_images_from_hwpx(doc_id: str, zip_ref: zipfile.ZipFile) -> dict:
    image_dir = os.path.join("image_store", doc_id)
    os.makedirs(image_dir, exist_ok=True)

    image_map = {}  # binaryItemIDRef → URL

    for name in zip_ref.namelist():
        if name.startswith("BinData/") and '.' in name:
            img_id = os.path.splitext(os.path.basename(name))[0]
            ext = os.path.splitext(name)[1].lstrip('.').lower()

            if ext not in ["jpg", "jpeg", "bmp", "png", "gif", "wmf", "emf"]:
                continue  # 허용된 확장자만 처리

            dst_filename = f"{img_id}.{ext}"
            dst_path = os.path.join(image_dir, dst_filename)

            with zip_ref.open(name) as source, open(dst_path, 'wb') as target:
                shutil.copyfileobj(source, target)

            image_url = f"http://192.168.1.101:8001/images/{doc_id}/{dst_filename}"
            image_map[img_id] = image_url

    return image_map

def convert_paragraph_to_text(paragraph, image_map):
    parts = []
    for run in paragraph.findall(".//hp:run", NAMESPACES):
        pic = run.find("hp:pic", NAMESPACES)
        if pic is not None:
            img_tag = pic.find(".//hc:img", NAMESPACES)
            if img_tag is not None:
                ref_id = img_tag.attrib.get("binaryItemIDRef")
                if ref_id and ref_id in image_map:
                    parts.append(f'<img src="{image_map[ref_id]}" style="max-width:100%;">')
            continue

        text_node = run.find("hp:t", NAMESPACES)
        if text_node is not None:
            parts.append(text_node.text or "")
    return "".join(parts)

def remove_header_footer_sections(temp_dir: str):
    for dirpath, _, files in os.walk(temp_dir):
        for fname in sorted(files):
            if "section" in fname and fname.endswith(".xml"):
                full_path = os.path.join(dirpath, fname)
                tree = ET.parse(full_path)
                root = tree.getroot()

                removed = False
                for tag in ["header", "footer"]:
                    for node in root.findall(f".//{{http://www.hancom.co.kr/hwpml/2011/section}}{tag}"):
                        parent = node.getparent()
                        if parent is not None:
                            parent.remove(node)
                            removed = True

                if removed:
                    tree.write(full_path, encoding="utf-8", xml_declaration=True)

def parse_hwpx_content_with_page(path: str, file_id: str) -> List[tuple[str, int]]:
    temp_dir = f"/tmp/hwpx_{uuid.uuid4().hex}"
    os.makedirs(temp_dir, exist_ok=True)

    with zipfile.ZipFile(path, 'r') as zip_ref:
        zip_ref.extractall(temp_dir)
        remove_header_footer_sections(temp_dir)
        image_map = extract_images_from_hwpx(file_id, zip_ref)
    ns = {
        'hp': 'http://www.hancom.co.kr/hwpml/2011/paragraph',
        'hs': 'http://www.hancom.co.kr/hwpml/2011/section',
        'hc': 'http://www.hancom.co.kr/hwpml/2011/core'
    }

    # 2. binaryItemIDRef(rId123) → 실제 이미지 파일명 매핑
    bin_rel_map = {}
    rels_path = os.path.join(temp_dir, "_rels", "content.hpf.rels")
    if os.path.exists(rels_path):
        rel_tree = ET.parse(rels_path)
        for rel in rel_tree.findall(".//{http://schemas.openxmlformats.org/package/2006/relationships}Relationship"):
            r_id = rel.attrib.get("Id")
            target = rel.attrib.get("Target")
            if r_id and target:
                bin_name = os.path.basename(target)
                bin_rel_map[r_id] = bin_name

    # 3. 저장 폴더 생성
    output_dir = f"./image_store/{file_id}"
    os.makedirs(output_dir, exist_ok=True)

    def is_inside_header_or_footer(element):
        while element is not None:
            tag = ET.QName(element.tag).localname.lower()
            if tag in ("header", "footer", "ctrl"):
                return True
            element = element.getparent()
        return False
    
    page_number = 1
    pagewise_text = []
    image_counter = 1

    for dirpath, _, files in os.walk(temp_dir):
        for fname in sorted(files):
            if "section" in fname and fname.endswith(".xml"):
                full_path = os.path.join(dirpath, fname)
                tree = ET.parse(full_path)
                root = tree.getroot()

                for node in root.iter():
                    if ET.QName(node.tag).localname == "secDef":
                        page_number += 1

                for para in root.findall(".//hp:p", namespaces=ns):
                    is_header_footer = any(
                        ET.QName(ancestor.tag).localname.lower() in ["header", "footer", "ctrl"]
                        for ancestor in para.iterancestors()
                    )
                    if is_header_footer:
                        continue

                    if para.attrib.get("pageBreak") == "1":
                        page_number += 1

                    para_text = convert_paragraph_to_text(para, image_map)
                    if para_text.strip():
                        pagewise_text.append((para_text.strip(), page_number))


    return pagewise_text

def split_hwpx_by_pages(path: str, file_id: str) -> List[dict]:
    pagewise_text = parse_hwpx_content_with_page(path, file_id)
    if not pagewise_text:
        return []

    pages = {}
    for text, page_number in pagewise_text:
        if page_number not in pages:
            pages[page_number] = []
        pages[page_number].append(text)

    chunks = []
    for page_number in sorted(pages.keys()):
        full_page_text = "\n".join(pages[page_number]).strip()
        chunks.append({
            "title": f"Page {page_number}",
            "content": full_page_text,
            "page_number": page_number
        })

    return chunks
