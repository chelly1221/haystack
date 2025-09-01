import os
import fitz  # PyMuPDF
import io
import hashlib
from PIL import Image


def generate_short_id(doc_id: str, page_num: int, img_index: int) -> str:
    """
    Generate a short, unique ID for an image.
    Uses first 8 characters of MD5 hash for brevity.
    """
    content = f"{doc_id}_{page_num}_{img_index}_{os.urandom(8).hex()}"
    hash_obj = hashlib.md5(content.encode())
    return hash_obj.hexdigest()[:8]


def extract_images_from_pdf(pdf_path: str, doc_id: str) -> dict:
    """
    Extract images from PDF and save them to disk.
    Returns a mapping of image index to URL.
    
    Args:
        pdf_path: Path to the PDF file
        doc_id: Document identifier
    """
    # Create image directory
    image_dir = os.path.join("image_store")
    os.makedirs(image_dir, exist_ok=True)
    
    image_map = {}  # page_num -> [(image_index, url), ...]
    
    # Open PDF with PyMuPDF for image extraction
    pdf_document = fitz.open(pdf_path)
    
    for page_num in range(len(pdf_document)):
        page = pdf_document[page_num]
        
        # Get list of actual images (not text or other objects)
        image_list = page.get_images()
        
        page_images = []
        img_counter = 0
        skipped_counter = 0
        
        for img_index, img in enumerate(image_list):
            saved = False
            xref = img[0]
            
            print(f"\n🔍 Processing image {img_index + 1} on page {page_num + 1}")
            
            # Method 1: Try extract_image first (most reliable for actual images)
            try:
                base_image = pdf_document.extract_image(xref)
                image_bytes = base_image["image"]
                
                # Try to open with PIL
                image = Image.open(io.BytesIO(image_bytes))
                print(f"  Image mode: {image.mode}, size: {image.size}")
                
                # Validate before processing
                if not is_valid_image(image):
                    skipped_counter += 1
                    continue
                
                # Convert to RGB if necessary
                if image.mode == "RGBA":
                    # Create white background
                    background = Image.new("RGB", image.size, (255, 255, 255))
                    background.paste(image, mask=image.split()[3])
                    image = background
                elif image.mode == "P":
                    image = image.convert("RGBA").convert("RGB")
                elif image.mode == "CMYK":
                    image = image.convert("RGB")
                elif image.mode == "L":
                    # Keep grayscale as is
                    pass
                elif image.mode not in ["RGB", "L"]:
                    image = image.convert("RGB")
                
                # Validate again after conversion
                if not is_valid_image(image):
                    skipped_counter += 1
                    continue
                
                # Generate short unique ID for the image (8 characters)
                short_id = generate_short_id(doc_id, page_num + 1, img_counter + 1)
                dst_filename = f"{short_id}.png"
                dst_path = os.path.join(image_dir, dst_filename)
                
                image.save(dst_path, "PNG", optimize=True)
                saved = True
                print(f"  ✅ Saved as {dst_filename}")
                
            except Exception as e:
                print(f"  ⚠️ Method 1 failed: {str(e)[:100]}")
            
            # Method 2: Try with Pixmap for problematic images
            if not saved:
                try:
                    pix = fitz.Pixmap(pdf_document, xref)
                    print(f"  Pixmap: size={pix.width}x{pix.height}, n={pix.n}, alpha={pix.alpha}")
                    
                    # Handle different color spaces
                    if pix.n == 1:  # Grayscale
                        # Keep as grayscale
                        pass
                    elif pix.n == 2:  # Grayscale + Alpha
                        # Create RGB with white background
                        pix2 = fitz.Pixmap(fitz.csRGB, pix, alpha=False)
                        pix2.set_rect(pix2.irect, (255, 255, 255))
                        pix2.copy_pixmap(pix, alpha=True)
                        pix = pix2
                    elif pix.n == 4:  # CMYK
                        pix2 = fitz.Pixmap(fitz.csRGB, pix)
                        pix = pix2
                    elif pix.n == 5:  # CMYK + Alpha
                        # Convert to RGB first
                        pix2 = fitz.Pixmap(fitz.csRGB, pix)
                        # Then remove alpha
                        if pix2.alpha:
                            pix3 = fitz.Pixmap(pix2, 0)
                            pix2 = pix3
                        pix = pix2
                    
                    # Remove alpha if still present
                    if pix.alpha:
                        pix2 = fitz.Pixmap(pix, 0)
                        pix = pix2
                    
                    # Convert to PIL for validation
                    img_data = pix.tobytes("png")
                    image = Image.open(io.BytesIO(img_data))
                    
                    # Validate the image
                    if not is_valid_image(image):
                        skipped_counter += 1
                        pix = None
                        continue
                    
                    # Generate short unique ID for the image (8 characters)
                    short_id = generate_short_id(doc_id, page_num + 1, img_counter + 1)
                    dst_filename = f"{short_id}.png"
                    dst_path = os.path.join(image_dir, dst_filename)
                    
                    pix.save(dst_path)
                    saved = True
                    print(f"  ✅ Method 2 saved as {dst_filename}")
                    
                    pix = None
                    
                except Exception as e:
                    print(f"  ❌ Method 2 failed: {str(e)[:100]}")
            
            # If successfully saved, add to map with short URL
            if saved:
                # Create short URL: http://192.168.10.101:8001/images/해시8자리
                image_url = f"http://192.168.10.101:8001/images/{short_id}"
                page_images.append((img_counter, image_url))
                img_counter += 1
        
        if skipped_counter > 0:
            print(f"\n📊 Page {page_num + 1}: Saved {img_counter} images, skipped {skipped_counter} invalid images")
        
        if page_images:
            image_map[page_num + 1] = page_images
    
    pdf_document.close()
    return image_map


def is_valid_image(image):
    """
    Check if the image contains meaningful content (not just white/single color)
    """
    # Convert to RGB if needed for consistent analysis
    if image.mode not in ["RGB", "L"]:
        test_image = image.convert("RGB")
    else:
        test_image = image
    
    # Check image size
    if image.size[0] < 10 or image.size[1] < 10:
        print(f"  ❌ Image too small: {image.size}")
        return False
    
    # Get image statistics
    if test_image.mode == "L":
        # Grayscale image
        histogram = test_image.histogram()
        # Check if all pixels are the same value
        non_zero_bins = sum(1 for count in histogram if count > 0)
        if non_zero_bins <= 1:
            print(f"  ❌ Grayscale image is single color")
            return False
        
        # Check if image is mostly white or black
        total_pixels = image.size[0] * image.size[1]
        white_pixels = histogram[255] if len(histogram) > 255 else 0
        black_pixels = histogram[0]
        
        if white_pixels / total_pixels > 0.99:
            print(f"  ❌ Image is mostly white (99%+)")
            return False
        if black_pixels / total_pixels > 0.99:
            print(f"  ❌ Image is mostly black (99%+)")
            return False
            
    else:
        # RGB image
        # Get bounding box of non-white/non-black content
        extrema = test_image.getextrema()
        
        # Check if all channels have the same value (grayscale in RGB)
        if extrema[0] == extrema[1] == extrema[2]:
            if extrema[0][0] == extrema[0][1]:  # Single color
                print(f"  ❌ RGB image is single color: {extrema}")
                return False
        
        # Check for mostly white images
        # Sample some pixels to check diversity
        width, height = test_image.size
        sample_pixels = []
        sample_size = min(100, width * height // 10)  # Sample up to 100 pixels
        
        for _ in range(sample_size):
            x = hash(str(_)) % width
            y = hash(str(_ * 2)) % height
            sample_pixels.append(test_image.getpixel((x, y)))
        
        # Check color diversity
        unique_colors = len(set(sample_pixels))
        if unique_colors <= 2:
            print(f"  ❌ Image has very low color diversity: {unique_colors} unique colors in sample")
            return False
        
        # Check if mostly white
        white_count = sum(1 for p in sample_pixels if all(c > 250 for c in p[:3]))
        if white_count / len(sample_pixels) > 0.95:
            print(f"  ❌ Image is mostly white (95%+ white pixels in sample)")
            return False
    
    print(f"  ✅ Image validation passed")
    return True


def insert_images_in_text(text: str, page_num: int, image_map: dict) -> str:
    """
    Insert image URLs directly into the text at appropriate positions.
    """
    if page_num not in image_map:
        return text
    
    # For simplicity, append image URLs at the end of the page text
    # In a more sophisticated implementation, you might want to detect image positions
    result = text
    for img_index, img_url in image_map[page_num]:
        result += f'\n{img_url}\n'
    
    return result