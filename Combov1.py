#!/usr/bin/env python3
"""
Marvel AI - Hospital Billing Audit Suite
Combined System for Document Analysis & Rate Validation
"""
import streamlit as st
import os
import json
import re
import time
import requests
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from typing import Optional, List, Dict, Tuple, Any, Union
import tempfile
import fitz  # PyMuPDF
import io

# ----------------------------
# CONFIG
# ----------------------------
# MARVEL_AI_API_KEY = os.getenv("MARVEL_AI_API_KEY") or "AIzaSyBcuMDgQUlh5HARk9lp82GxAVyUgkfd6ZY"
MARVEL_AI_API_KEY = st.secrets.get("MARVEL_AI_API_KEY", os.getenv("MARVEL_AI_API_KEY", "AIzaSyBcuMDgQUlh5HARk9lp82GxAVyUgkfd6ZY"))

UPLOAD_URL = (
    "https://generativelanguage.googleapis.com/upload/v1beta/files?key=" +
    MARVEL_AI_API_KEY
)

MARVEL_AI_URL = (
    "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash-lite:generateContent?key=" +
    MARVEL_AI_API_KEY
)

# STATIC RATE CARD PATH
STATIC_RATE_CARD_PATH = r"Data\MRD\CGHS corporate rate.xlsx"

# ----------------------------
# Doctor alias dictionary
# ----------------------------
DOCTOR_ALIAS = {
    "dr ksb": "Dr. Kalyanasundarabharathi V C",
    "dr k.s.b": "Dr. Kalyanasundarabharathi V C",
    "kalyanasundarabharathi": "Dr. Kalyanasundarabharathi V C",
    "dr kalyanasundarabharathi": "Dr. Kalyanasundarabharathi V C",
    "dr k t ravindran": "Dr. KT Ravindran",
    "dr ravindran k t": "Dr. KT Ravindran",
    "dr ravindran": "Dr. KT Ravindran",
    "dr senthil kumar r": "Dr. Senthilkumar R",
    "dr senthilkumar r": "Dr. Senthilkumar R",
    "dr senthil": "Dr. Senthilkumar R",
    "dr siva sowmiya r": "Dr. Siva Sowmiya R",
    "dr sivasowmiya": "Dr. Siva Sowmiya R",
    "dr balakrishnan nm": "Dr. Balakrishnan N.M.",
    "dr balakrishnan n.m": "Dr. Balakrishnan N.M.",
    "dr balasubramaniam c": "Dr. Balasubramaniam C"
}

# ============================================================================
# COMMON FUNCTIONS (Used by both modules)
# ============================================================================
def upload_file_pdf(path: str, max_retries: int = 3) -> str:
    """Upload PDF and return file URI"""
    with open(path, "rb") as f:
        pdf_bytes = f.read()

    for attempt in range(1, max_retries + 1):
        try:
            resp = requests.post(
                UPLOAD_URL,
                headers={"Content-Type": "application/pdf"},
                data=pdf_bytes,
                timeout=120
            )
        except Exception as e:
            time.sleep(2 * attempt)
            continue

        if resp.status_code == 200:
            file_data = resp.json()
            file_uri = file_data.get("file", {}).get("uri")
            return file_uri
        else:
            time.sleep(2 * attempt)

    raise Exception("Upload failed after retries")

def extract_json_from_text(text: str) -> Optional[dict]:
    """Extract JSON from model response"""
    if not text:
        return None

    # Try fenced JSON block
    m = re.search(r"```(?:json)?\s*(\{[\s\S]*\})\s*```", text, re.IGNORECASE)
    if m:
        candidate = m.group(1)
        try:
            return json.loads(candidate)
        except:
            pass

    # Greedy JSON extraction
    start = text.find("{")
    if start != -1:
        depth = 0
        candidate = None
        for i in range(start, len(text)):
            if text[i] == "{":
                depth += 1
            elif text[i] == "}":
                depth -= 1
                if depth == 0:
                    candidate = text[start:i+1]
                    break
        if candidate:
            try:
                return json.loads(candidate)
            except:
                pass

    return None

# ============================================================================
# MODULE 1: DOCUMENT ANALYSIS (Medical Record vs Final Bill)
# ============================================================================
def extract_bill_items_from_pdf(pdf_path: str) -> List[Dict]:
    """
    Extract bill items from hospital PDF bill with proper total handling
    """
    bill_items = []
    doc = fitz.open(pdf_path)
    
    # Category mapping
    category_mapping = {
        'BED CHARGES-WARD': 'BED_CHARGES',
        'DIET CHARGES': 'DIET_CHARGES', 
        'DRUG CHARGES': 'DRUG_CHARGES',
        'NURSING SERVICE-WARD': 'NURSING_SERVICE',
        'PROFESSIONAL CHARGES': 'PROFESSIONAL_CHARGES',
        'TREATMENT': 'TREATMENT',
        'X RAY CHARGES': 'XRAY_CHARGES',
        'BED CHARGES-ICU': 'ICU_CHARGES',
        'DRESSING CHARGES': 'DRESSING_CHARGES',
        'HISTOPATHOLOGY': 'HISTOPATHOLOGY',
        'NURSING SERVICE-ICU': 'NURSING_SERVICE_ICU',
        'OPERATION THEATRE': 'OPERATION_THEATRE',
        'OT CONSUMABLES': 'OT_CONSUMABLES',
        'CLINICAL PATHOLOGY': 'CLINICAL_PATHOLOGY',
        'MICROBIOLOGY': 'MICROBIOLOGY',
        'ULTRASOUND': 'ULTRASOUND'
    }
    
    current_category = None
    current_entity = None
    category_items = []  # Temporary storage for current category items
    
    for page_num in range(len(doc)):
        page = doc[page_num]
        text = page.get_text()
        lines = text.split('\n')
        
        i = 0
        while i < len(lines):
            line = lines[i].strip()
            
            # Check for category headers
            for category_key, category_value in category_mapping.items():
                if line.startswith(category_key) or f") {category_key}" in line:
                    # If we have items from previous category, add them to bill_items
                    if category_items:
                        bill_items.extend(category_items)
                        category_items = []
                    
                    current_category = category_value
                    current_entity = category_key
                    break
            
            # Pattern for bill items: date code description amount
            date_pattern = r'\d{2}-\d{2}-\d{4}'
            
            if re.match(date_pattern, line):
                # This might be a bill item line
                date_part = line
                
                # Look ahead to get the complete item
                item_parts = [date_part]
                j = i + 1
                while j < len(lines) and j < i + 5:  # Look ahead up to 5 lines
                    next_line = lines[j].strip()
                    if (re.match(date_pattern, next_line) or 
                        next_line.startswith('Run Date') or
                        next_line.startswith('***') or
                        any(next_line.startswith(f"{k})") for k in range(1, 20)) or
                        next_line.startswith('Concession Details') or
                        next_line.startswith('Total Bill Amount')):
                        break
                    if next_line and not next_line.startswith('Patient Name'):
                        item_parts.append(next_line)
                    j += 1
                
                # Join the parts to form complete item text
                complete_text = ' '.join(item_parts)
                
                # Extract amounts - look for multiple currency patterns
                amount_pattern = r'([\d,]+\.\d{2})'
                amount_matches = re.findall(amount_pattern, complete_text)
                
                if amount_matches:
                    # Check if this is the last line with total (has multiple amounts)
                    if len(amount_matches) > 1:
                        # This is the last line with individual amount + total
                        # Use the first amount (individual amount), ignore the total
                        amount_str = amount_matches[0].replace(',', '')
                    else:
                        # Regular line with single amount
                        amount_str = amount_matches[0].replace(',', '')
                    
                    try:
                        amount = float(amount_str)
                        
                        # Create the billed text (everything except the total amount if present)
                        if len(amount_matches) > 1:
                            # Remove the total amount from the text, keep only individual amount
                            billed_text = complete_text.rsplit(amount_matches[1], 1)[0].strip()
                        else:
                            billed_text = complete_text
                        
                        bill_item = {
                            "charge_date": date_part.split()[0] if ' ' in date_part else date_part,
                            "billed_text": billed_text,
                            "billed_entity": current_entity or "UNCATEGORIZED",
                            "billed_amount": amount,
                            "bill_page": page_num + 1,
                            "category": current_category or "UNCATEGORIZED"
                        }
                        
                        category_items.append(bill_item)
                        
                    except ValueError:
                        pass
            
            # Check for concession and summary section
            if line.startswith('Concession Details') or line.startswith('Total Bill Amount'):
                # Add any remaining category items
                if category_items:
                    bill_items.extend(category_items)
                    category_items = []
                break
            
            i += 1
        
        # Add any remaining category items at page end
        if category_items:
            bill_items.extend(category_items)
            category_items = []
    
    doc.close()
    return bill_items

def extract_concession_details(pdf_path: str) -> Dict[str, Any]:
    """
    Extract concession and payment details from the bill
    """
    concession_data = {}
    doc = fitz.open(pdf_path)
    
    concession_patterns = {
        'total_bill_amount': r'Total Bill Amount\s*:\s*([\d,]+\.\d{2})',
        'less_concession': r'Less Concession\s*:\s*([\d,]+\.\d{2})',
        'net_amount': r'Net Amount\s*:\s*([\d,]+\.\d{2})',
        'advance_adjusted': r'Advance Adjusted\s*:\s*([\d,]+\.\d{2})',
        'account_to_insurance': r'A/C to VIDAL.*?:\s*([\d,]+\.\d{2})',
        'as_per_mou_concession': r'AS PER MOU.*?CONCESSION.*?:\s*([\d,]+\.\d{2})',
        'as_per_package_concession': r'AS PER PACKAGE.*?CONCESSION.*?:\s*([\d,]+\.\d{2})'
    }
    
    advance_details = []
    
    for page_num in range(len(doc)):
        page = doc[page_num]
        text = page.get_text()
        
        # Extract concession amounts
        for key, pattern in concession_patterns.items():
            match = re.search(pattern, text, re.IGNORECASE | re.DOTALL)
            if match:
                amount_str = match.group(1).replace(',', '')
                try:
                    concession_data[key] = float(amount_str)
                except ValueError:
                    pass
        
        # Extract advance details
        advance_pattern = r'(\d{2}-\d{2}-\d{4})\s+(IRA\d+)\s+([\d,]+\.\d{2})'
        advance_matches = re.findall(advance_pattern, text)
        
        for date, ref_no, amount in advance_matches:
            advance_details.append({
                "date": date,
                "reference": ref_no,
                "amount": float(amount.replace(',', ''))
            })
    
    concession_data['advance_details'] = advance_details
    doc.close()
    return concession_data

def parse_hospital_bill(pdf_path: str) -> Dict[str, Any]:
    """
    Main function to parse hospital bill and return structured data
    """
    bill_items = extract_bill_items_from_pdf(pdf_path)
    concession_details = extract_concession_details(pdf_path)
    
    result = {
        "bill_items": bill_items,
        "total_items": len(bill_items),
        "concession_details": concession_details,
        "summary": {
            "total_amount": sum(item["billed_amount"] for item in bill_items),
            "categories": {}
        }
    }
    
    # Calculate category-wise summary
    for item in bill_items:
        category = item["category"]
        if category not in result["summary"]["categories"]:
            result["summary"]["categories"][category] = 0
        result["summary"]["categories"][category] += item["billed_amount"]
    
    return result

def process_chunk_with_marvel_ai(medical_uri: str, bill_uri: str, chunk_items: List[Dict], 
                                chunk_num: int, total_chunks: int, max_retries: int = 3) -> List[Dict]:
    """Process a chunk of bill items using Marvel AI for documentation validation"""
    
    for attempt in range(max_retries):
        try:
            # Prepare chunk data for prompt
            chunk_data = []
            for item in chunk_items:
                chunk_data.append({
                    "charge_date": item.get("charge_date", ""),
                    "billed_entity": item.get("billed_entity", ""),
                    "billed_amount": item.get("billed_amount", 0),
                    "category": item.get("category", ""),
                    "billed_text": item.get("billed_text", "")
                })
            
            chunk_json = json.dumps(chunk_data, indent=2)
            alias_json = json.dumps(DOCTOR_ALIAS, indent=2)
            
            prompt = f"""
            You are Marvel AI Hospital Billing Auditor. Process CHUNK {chunk_num} of {total_chunks}.

            MEDICAL_RECORD: {medical_uri}
            FINAL_BILL: {bill_uri}

            DOCTOR NAME MAPPING:
            {alias_json}

            BILL ITEMS TO AUDIT (Chunk {chunk_num}/{total_chunks} - {len(chunk_items)} items):
            {chunk_json}

            TASK: For each bill item above, find evidence in medical records and create audit entry.

            OUTPUT FORMAT - JSON ONLY:
            {{
                "audits": [
                    {{
                        "category": "BED_CHARGES",
                        "charge_date": "18-09-2025",
                        "billed_text": "10481 4117 4,000.00",
                        "billed_entity": "BED CHARGES-WARD",
                        "billed_amount": 4000,
                        "bill_page": 1,
                        "report_entity": "Ward admission record - Room 4117",
                        "report_page": 8,
                        "report_date": "18-09-2025",
                        "matching_confidence": "HIGH",
                        "audit_outcome": "MATCH",
                        "remarks": "Ward bed occupancy documented in nursing notes"
                    }}
                ]
            }}

            AUDIT OUTCOMES:
            - MATCH: Service properly documented and billed correctly
            - AMOUNT_MISMATCH: Service documented but billed amount differs from records
            - UNSUPPORTED_BILLING: Billed but no documentation found in medical records
            - POTENTIAL_MISSING_CHARGE: Documented in records but not billed

            Return JSON with exactly {len(chunk_items)} audit entries.
            """
            
            payload = {
                "contents": [
                    {
                        "role": "user",
                        "parts": [
                            {"text": prompt},
                            {"fileData": {"mimeType": "application/pdf", "fileUri": medical_uri}},
                            {"fileData": {"mimeType": "application/pdf", "fileUri": bill_uri}}
                        ]
                    }
                ],
                "generationConfig": {
                    "maxOutputTokens": 3000,
                    "temperature": 0.1
                }
            }

            resp = requests.post(
                MARVEL_AI_URL,
                headers={"Content-Type": "application/json"},
                data=json.dumps(payload),
                timeout=180
            )

            if resp.status_code == 429:  # Rate limit
                wait_time = (attempt + 1) * 10
                time.sleep(wait_time)
                continue
                
            if resp.status_code != 200:
                if attempt < max_retries - 1:
                    time.sleep(5)
                    continue
                else:
                    raise Exception(f"API request failed: {resp.status_code}")

            data = resp.json()
            text = data["candidates"][0]["content"]["parts"][0]["text"]
            
            # Parse the response
            parsed = extract_json_from_text(text)
            
            if parsed and "audits" in parsed:
                audits = parsed["audits"]
                return audits
            else:
                if attempt < max_retries - 1:
                    time.sleep(3)
                    continue
                else:
                    raise Exception("Failed to parse audit results")
                    
        except Exception as e:
            if attempt < max_retries - 1:
                wait_time = (attempt + 1) * 5
                time.sleep(wait_time)
                continue
            else:
                raise e
    
    raise Exception(f"Failed after {max_retries} attempts")

def run_document_analysis_audit(medical_file, bill_file):
    """Run the document analysis audit (Medical Record vs Final Bill)"""
    # Save uploaded files temporarily
    with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as tmp_med:
        tmp_med.write(medical_file.getvalue())
        medical_path = tmp_med.name

    with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as tmp_bill:
        tmp_bill.write(bill_file.getvalue())
        bill_path = tmp_bill.name

    try:
        # Upload files
        with st.spinner("Uploading documents to secure cloud..."):
            med_uri = upload_file_pdf(medical_path)
            bill_uri = upload_file_pdf(bill_path)

        # Step 1: Extract ALL bill items using enhanced PyMuPDF
        st.info("Step 1: Extracting bill items using advanced parsing...")
        bill_analysis = parse_hospital_bill(bill_path)
        all_bill_items = bill_analysis["bill_items"]
        concession_details = bill_analysis["concession_details"]
        
        if not all_bill_items:
            st.warning("No bill items were extracted from the document")
            return {"audits": []}, [], {}

        st.success(f"Successfully extracted {len(all_bill_items)} bill items!")
        
        # Step 2: Group by category for processing
        categorized_items = {}
        for item in all_bill_items:
            category = item.get("category", "OTHER_CHARGES")
            if category not in categorized_items:
                categorized_items[category] = []
            categorized_items[category].append(item)

        # Step 3: Process all categories with unified progress bar
        all_audits = []
        total_categories = len(categorized_items)
        current_category_num = 0
        
        # Create unified progress bar
        st.info("Step 2: Processing all categories with Marvel AI...")
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        # Process each category
        for category, items in categorized_items.items():
            current_category_num += 1
            progress = (current_category_num - 1) / total_categories
            progress_bar.progress(progress)
            
            # Update status with current category
            category_display = category.replace('_', ' ').title()
            status_text.text(f"Processing {category_display} ({len(items)} items)...")
            
            # Split category items into chunks
            chunk_size = 10
            category_chunks = [items[i:i + chunk_size] for i in range(0, len(items), chunk_size)]
            total_chunks = len(category_chunks)
            
            for chunk_idx, chunk_items in enumerate(category_chunks):
                try:
                    chunk_audits = process_chunk_with_marvel_ai(
                        med_uri, bill_uri, chunk_items, chunk_idx + 1, total_chunks, max_retries=3
                    )
                    all_audits.extend(chunk_audits)
                    
                except Exception as e:
                    # Graceful error handling - continue with next chunk
                    continue
                
                # Small delay to avoid rate limits
                time.sleep(2)
        
        # Complete the progress bar
        progress_bar.progress(1.0)
        status_text.text("All categories processed successfully!")
        
        # Create final result
        final_result = {"audits": all_audits}
        
        return final_result, all_bill_items, concession_details

    except Exception as e:
        st.error(f"System encountered an issue: {str(e)}")
        return {"audits": []}, [], {}
    finally:
        # Cleanup temp files
        try:
            os.unlink(medical_path)
            os.unlink(bill_path)
        except:
            pass

# ============================================================================
# MODULE 2: RATE VALIDATION (Final Bill vs Excel Rate Card)
# ============================================================================
def extract_bill_info_with_ai(bill_uri: str) -> Dict[str, Any]:
    """
    Extract ALL information from bill using AI in ONE call
    Returns: header_info, line_items, concession_details
    """
    prompt = """
    You are a hospital billing expert. Extract ALL information from this hospital bill.
    
    EXTRACT THE FOLLOWING IN JSON FORMAT:

    1. HEADER INFORMATION (from top of bill):
    - patient_name: Full name of patient
    - mrd_id: MRD ID number
    - bill_no: Bill number
    - bill_date: Bill date (DD-MM-YYYY)
    - company: Company/Insurance name (e.g., "SOUTHERN RAILWAY", "ECHS REGIONAL CENTRE", etc.)
    - admitting_doctor: Admitting doctor name
    - treating_doctor: Treating doctor name
    - admit_date: Admission date (DD-MM-YYYY)
    - discharge_date: Discharge date (DD-MM-YYYY)
    - ward_type: Ward type (e.g., "PRIVATE WARD")
    - umid: UMID if available

    2. LINE ITEMS (ALL services billed):
    Extract EVERY line item/service charge. For EACH line item include:
    - charge_date: Date of service (DD-MM-YYYY)
    - service_code: Service code/number (4-6 digit code like 18213, 20702, etc.)
    - service_description: Description of service (what was done)
    - base_amount: Base unit amount (e.g., 350.00, 8217.00)
    - quantity: Number of units/occurrences (e.g., 3, 1) - extract from patterns like (350.00*3) or (8217*1)
    - billed_amount: Total amount billed (base_amount * quantity)
    - category: Category of service (e.g., "PROFESSIONAL CHARGES", "DRUG CHARGES", "BED CHARGES", etc.)
    - billed_entity: Entity name from bill section

    3. CONCESSION & PAYMENT DETAILS (from bottom of bill):
    - total_bill_amount: Total bill amount
    - less_concession: Total concession amount
    - net_amount: Net amount payable
    - advance_adjusted: Advance amount adjusted
    - as_per_mou_concession: MOU concession if mentioned
    - as_per_package_concession: Package concession if mentioned

    OUTPUT FORMAT - STRICT JSON:
    {
        "header_info": {
            "patient_name": "KRISHNAMOORTHY",
            "mrd_id": "201401260108",
            "bill_no": "IR2510450",
            "bill_date": "10-10-2025",
            "company": "SOUTHERN RAILWAY",
            "admitting_doctor": "Ranjith R",
            "treating_doctor": "Ranjith R",
            "admit_date": "07-10-2025",
            "discharge_date": "10-10-2025",
            "ward_type": "PRIVATE WARD",
            "umid": "15647261073Z"
        },
        "line_items": [
            {
                "charge_date": "09-10-2025",
                "service_code": "CN002",
                "service_description": "DR RANJITH R MD,DM(cardiologist)",
                "base_amount": 35000.00,
                "quantity": 3,
                "billed_amount": 105000.00,
                "category": "PROFESSIONAL CHARGES",
                "billed_entity": "PROFESSIONAL CHARGES"
            },
            {
                "charge_date": "09-10-2025",
                "service_code": "16997",
                "service_description": "ADMINISTRATIVE CHARGES",
                "base_amount": 8217.00,
                "quantity": 1,
                "billed_amount": 8217.00,
                "category": "GENERAL",
                "billed_entity": "GENERAL"
            }
        ],
        "concession_details": {
            "total_bill_amount": 50000.00,
            "less_concession": 10000.00,
            "net_amount": 40000.00,
            "advance_adjusted": 5000.00,
            "as_per_mou_concession": 8000.00,
            "as_per_package_concession": 2000.00
        }
    }

    IMPORTANT RULES:
    1. Extract ALL line items - don't miss any
    2. Look for multiplication patterns like (11500.00*1), (35000*3), (350.00*3), (8217*1)
    3. service_code MUST be extracted - look for 4-6 digit numbers or alphanumeric codes
    4. If service code absolutely not found, use "NOT_FOUND"
    5. Company field is CRITICAL - extract exactly as shown in bill
    6. All amounts as numbers (float)
    7. Dates in DD-MM-YYYY format
    8. Return ONLY valid JSON, no other text
    9. For quantity: If pattern like (35000*3) found, extract 3 as quantity
    10. For base_amount: If pattern like (35000*3) found, extract 35000 as base_amount
    11. billed_amount should be base_amount * quantity
    """

    max_retries = 3
    for attempt in range(max_retries):
        try:
            payload = {
                "contents": [
                    {
                        "role": "user",
                        "parts": [
                            {"text": prompt},
                            {"fileData": {"mimeType": "application/pdf", "fileUri": bill_uri}}
                        ]
                    }
                ],
                "generationConfig": {
                    "maxOutputTokens": 4000,
                    "temperature": 0.1
                }
            }

            resp = requests.post(
                MARVEL_AI_URL,
                headers={"Content-Type": "application/json"},
                data=json.dumps(payload),
                timeout=180
            )

            if resp.status_code == 429:  # Rate limit
                wait_time = (attempt + 1) * 10
                time.sleep(wait_time)
                continue
                
            if resp.status_code != 200:
                if attempt < max_retries - 1:
                    time.sleep(5)
                    continue
                else:
                    raise Exception(f"API request failed: {resp.status_code}")

            data = resp.json()
            text = data["candidates"][0]["content"]["parts"][0]["text"]
            
            # Parse JSON response
            parsed = extract_json_from_text(text)
            
            if parsed:
                # Validate structure
                if "header_info" in parsed and "line_items" in parsed:
                    # Ensure line_items is a list
                    if isinstance(parsed["line_items"], list):
                        return parsed
                    else:
                        parsed["line_items"] = []
                        return parsed
                else:
                    if attempt < max_retries - 1:
                        time.sleep(3)
                        continue
            
        except Exception as e:
            if attempt < max_retries - 1:
                time.sleep(5)
                continue
            else:
                print(f"Error extracting bill info: {str(e)}")
    
    # Return empty structure if all retries fail
    return {
        "header_info": {},
        "line_items": [],
        "concession_details": {}
    }

class RateValidator:
    """Validates service rates against static rate sheet"""
    
    def __init__(self):
        self.rate_sheet = self._load_static_rate_sheet()
    
    def _load_static_rate_sheet(self) -> Dict[str, Dict]:
        """Load static rate data from predefined path"""
        try:
            # Check if file exists
            if not os.path.exists(STATIC_RATE_CARD_PATH):
                print(f"Rate card not found at: {STATIC_RATE_CARD_PATH}")
                return {'STANDARD': {}, 'CGHS': {}}
            
            # Try reading as Excel first, then CSV
            try:
                df = pd.read_excel(STATIC_RATE_CARD_PATH)
            except:
                try:
                    df = pd.read_csv(STATIC_RATE_CARD_PATH)
                except Exception as e:
                    print(f"Error reading rate card: {e}")
                    return {'STANDARD': {}, 'CGHS': {}}
            
            # Create lookup dictionaries
            standard_lookup = {}  # {service_code: data}
            cghs_lookup = {}      # {service_code: data}
            
            for _, row in df.iterrows():
                # Standard rates
                if pd.notna(row.get('Service Code')):
                    code = str(row['Service Code']).strip()
                    if code and code != 'nan' and code != 'None':
                        standard_lookup[code] = {
                            'service_name': str(row.get('Service Name', '')).strip(),
                            'rate': float(row['Rate']) if pd.notna(row.get('Rate')) else 0.0
                        }
                
                # CGHS rates
                if pd.notna(row.get('CGHS CODE')):
                    code = str(row['CGHS CODE']).strip()
                    if code and code != 'nan' and code != 'None':
                        cghs_lookup[code] = {
                            'service_name': str(row.get('CGHS SERVICE NAME', '')).strip(),
                            'rate': float(row['CGHS RATE']) if pd.notna(row.get('CGHS RATE')) else 0.0
                        }
            
            return {
                'STANDARD': standard_lookup,
                'CGHS': cghs_lookup
            }
        except Exception as e:
            print(f"Error loading rate sheet: {e}")
            return {'STANDARD': {}, 'CGHS': {}}
    
    def determine_rate_scheme(self, company_name: str) -> str:
        """
        Determine which rate scheme to use based on company
        Returns: 'CGHS' or 'STANDARD'
        """
        if not company_name:
            return 'STANDARD'
        
        company_upper = str(company_name).upper()
        
        # CGHS/Railway/ECHS companies
        cghs_keywords = [
            'SOUTHERN RAILWAY', 'RAILWAY', 'ECHS', 
            'CGHS', 'CENTRAL GOVERNMENT', 'DEFENCE', 'GOVERNMENT',
            'RAILWAYS', 'EX-SERVICEMEN', 'EXSERVICEMEN'
        ]
        
        for keyword in cghs_keywords:
            if keyword in company_upper:
                return 'CGHS'
        
        return 'STANDARD'
    
    def validate_rate(self, service_code: str, base_amount: float, quantity: int, 
                     billed_amount: float, rate_scheme: str) -> Dict:
        """
        Validate service code and rate with quantity consideration
        """
        result = {
            'service_code': service_code,
            'base_amount': base_amount,
            'quantity': quantity,
            'billed_amount': billed_amount,
            'rate_scheme': rate_scheme,
            'validation_status': 'PENDING',
            'matched_status': 'NOT_MATCHED',  # Default
            'approved_rate': None,
            'service_name': None,
            'match_found': False,
            'remarks': '',
            'rate_difference': 0,
            'unit_price_mismatch': False
        }
        
        if not service_code or service_code == 'NOT_FOUND':
            result['validation_status'] = 'SERVICE_CODE_NOT_FOUND'
            result['matched_status'] = 'NOT_MATCHED'
            result['remarks'] = 'Service code not found in bill line item'
            return result
        
        # Check if billed amount matches base_amount * quantity
        calculated_amount = base_amount * quantity
        if abs(calculated_amount - billed_amount) > 0.01:
            result['remarks'] = f'Billed amount mismatch: {base_amount} * {quantity} = {calculated_amount}, but billed {billed_amount}'
        
        # Get appropriate rate lookup
        lookup = self.rate_sheet.get(rate_scheme, {})
        
        # Check if service code exists (exact match first)
        if service_code not in lookup:
            # Try case-insensitive search
            service_code_upper = service_code.upper()
            matching_keys = [k for k in lookup.keys() if str(k).upper() == service_code_upper]
            
            if matching_keys:
                # Use the matching key
                actual_code = matching_keys[0]
                service_data = lookup[actual_code]
                result['service_code'] = actual_code
            else:
                result['validation_status'] = 'SERVICE_NOT_IN_RATE_SHEET'
                result['matched_status'] = 'NOT_MATCHED'
                result['remarks'] = f'Service code {service_code} not found in {rate_scheme} rate sheet'
                return result
        else:
            service_data = lookup[service_code]
        
        # Get approved rate
        approved_rate = service_data.get('rate', 0.0)
        
        # Calculate expected total for quantity
        expected_total = approved_rate * quantity
        result['approved_rate'] = approved_rate
        result['expected_total'] = expected_total
        
        result.update({
            'service_name': service_data.get('service_name', ''),
            'match_found': True,
            'rate_difference': billed_amount - expected_total
        })
        
        # Check unit price
        unit_price_difference = base_amount - approved_rate
        if abs(unit_price_difference) > 0.01:
            result['unit_price_mismatch'] = True
        
        # Exact match validation (with float precision tolerance)
        if abs(billed_amount - expected_total) < 0.01:
            result['validation_status'] = 'RATE_COMPLIANT'
            result['matched_status'] = 'MATCHED'
            result['remarks'] = f'Rate matches exactly for {quantity} units'
        else:
            result['validation_status'] = 'RATE_NON_COMPLIANT'
            result['matched_status'] = 'NOT_MATCHED'
            if billed_amount > expected_total:
                result['remarks'] = f'Overcharge: ₹{billed_amount - expected_total:.2f} for {quantity} units'
            else:
                result['remarks'] = f'Undercharge: ₹{expected_total - billed_amount:.2f} for {quantity} units'
            
            if result['unit_price_mismatch']:
                result['remarks'] += f' | Unit price mismatch: ₹{base_amount} vs ₹{approved_rate}'
        
        return result
    
    def get_rate_sheet_summary(self) -> Dict:
        """Get summary of loaded rate sheet"""
        standard_count = len(self.rate_sheet.get('STANDARD', {}))
        cghs_count = len(self.rate_sheet.get('CGHS', {}))
        
        return {
            'standard_services': standard_count,
            'cghs_services': cghs_count,
            'total_services': standard_count + cghs_count
        }

def perform_rate_validation_audit(line_items: List[Dict], 
                                header_info: Dict,
                                rate_validator: RateValidator) -> pd.DataFrame:
    """
    Perform rate validation audit and return DataFrame with required columns
    """
    audit_results = []
    
    company = header_info.get('company', '')
    rate_scheme = rate_validator.determine_rate_scheme(company)
    
    for item in line_items:
        service_code = item.get('service_code', '')
        service_description = item.get('service_description', '')
        base_amount = item.get('base_amount', 0)
        quantity = item.get('quantity', 1)
        billed_amount = item.get('billed_amount', 0)
        charge_date = item.get('charge_date', '')
        
        # Perform rate validation
        validation = rate_validator.validate_rate(
            service_code=service_code,
            base_amount=base_amount,
            quantity=quantity,
            billed_amount=billed_amount,
            rate_scheme=rate_scheme
        )
        
        # Create audit row with ALL required columns
        audit_row = {
            'charge_date': charge_date,
            'service_code': service_code,
            'service_description': service_description,
            'quantity': quantity,
            'base_amount': base_amount,
            'billed_amount': billed_amount,
            'approved_rate': validation.get('approved_rate'),
            'expected_total': validation.get('expected_total', 0),
            'rate_difference': validation.get('rate_difference', 0),
            'matched_status': validation.get('matched_status', 'NOT_MATCHED'),
            'remarks': validation.get('remarks', ''),
            'validation_status': validation.get('validation_status', 'PENDING'),
            'category': item.get('category', ''),
            'billed_entity': item.get('billed_entity', ''),
            'company': company,
            'rate_scheme': rate_scheme,
            'service_name': validation.get('service_name', ''),
            'match_found': validation.get('match_found', False),
            'unit_price_mismatch': validation.get('unit_price_mismatch', False)
        }
        
        # Set audit_outcome for money leakage analysis
        status = validation.get('validation_status', '')
        if status == 'RATE_COMPLIANT':
            audit_row['audit_outcome'] = 'MATCH'
        elif status == 'RATE_NON_COMPLIANT':
            audit_row['audit_outcome'] = 'AMOUNT_MISMATCH'
        elif status == 'SERVICE_NOT_IN_RATE_SHEET':
            audit_row['audit_outcome'] = 'UNSUPPORTED_BILLING'
        elif status == 'SERVICE_CODE_NOT_FOUND':
            audit_row['audit_outcome'] = 'UNSUPPORTED_BILLING'
        else:
            audit_row['audit_outcome'] = 'UNSUPPORTED_BILLING'
        
        audit_results.append(audit_row)
    
    # Convert to DataFrame
    df = pd.DataFrame(audit_results)
    
    # Reorder columns as requested with matched_status column
    column_order = [
        'charge_date', 'service_code', 'service_description', 'quantity',
        'base_amount', 'billed_amount', 'approved_rate', 'expected_total',
        'rate_difference', 'matched_status', 'remarks'
    ]
    
    # Add remaining columns
    remaining_cols = [col for col in df.columns if col not in column_order]
    df = df[column_order + remaining_cols]
    
    return df

def run_rate_validation_audit(bill_file):
    """Run the rate validation audit (Final Bill vs Excel Rate Card)"""
    # Save uploaded file temporarily
    with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as tmp_bill:
        tmp_bill.write(bill_file.getvalue())
        bill_path = tmp_bill.name

    try:
        # Upload file to Marvel AI
        with st.spinner("Uploading bill to Marvel AI..."):
            bill_uri = upload_file_pdf(bill_path)

        # Step 1: Extract ALL information from bill using AI
        st.info("Step 1: Extracting bill information using AI...")
        
        progress_bar = st.progress(0)
        status_text = st.empty()
        status_text.text("Extracting header information, line items, and concession details...")
        
        # Use AI for extraction
        bill_info = extract_bill_info_with_ai(bill_uri)
        
        progress_bar.progress(1.0)
        
        # Get extracted data
        header_info = bill_info.get("header_info", {})
        line_items = bill_info.get("line_items", [])
        concession_details = bill_info.get("concession_details", {})
        
        if not line_items:
            st.warning("No line items were extracted from the bill")
            return line_items, concession_details, {}, header_info, pd.DataFrame()
        
        st.success(f"✅ Successfully extracted {len(line_items)} line items!")
        
        # Step 2: Perform rate validation with static rate card
        st.info("Step 2: Performing rate validation...")
        
        # Initialize rate validator with static path
        rate_validator = RateValidator()
        
        # Show rate sheet summary
        rate_summary = rate_validator.get_rate_sheet_summary()
        st.write(f"**Rate Sheet Summary:**")
        st.write(f"- Standard Services: {rate_summary['standard_services']}")
        st.write(f"- CGHS Services: {rate_summary['cghs_services']}")
        st.write(f"- Total Services: {rate_summary['total_services']}")
        
        # Determine rate scheme
        company = header_info.get('company', '')
        rate_scheme = rate_validator.determine_rate_scheme(company)
        st.write(f"**Detected Rate Scheme:** {rate_scheme} (Based on Company: {company})")
        
        # Perform rate validation audit
        rate_progress = st.progress(0)
        status_text.text(f"Validating rates for {len(line_items)} items...")
        
        # Create the SINGLE TABLE with all required columns including matched_status
        rate_audit_df = perform_rate_validation_audit(
            line_items, header_info, rate_validator
        )
        
        rate_progress.progress(1.0)
        st.success(f"✅ Rate validation completed for {len(line_items)} items!")
        
        # Calculate rate compliance metrics
        if not rate_audit_df.empty:
            total_items = len(rate_audit_df)
            rate_compliant = len(rate_audit_df[rate_audit_df['validation_status'] == 'RATE_COMPLIANT'])
            rate_non_compliant = len(rate_audit_df[rate_audit_df['validation_status'] == 'RATE_NON_COMPLIANT'])
            not_in_sheet = len(rate_audit_df[rate_audit_df['validation_status'] == 'SERVICE_NOT_IN_RATE_SHEET'])
            code_not_found = len(rate_audit_df[rate_audit_df['validation_status'] == 'SERVICE_CODE_NOT_FOUND'])
            
            total_overcharge = rate_audit_df[rate_audit_df['rate_difference'] > 0]['rate_difference'].sum()
            total_undercharge = abs(rate_audit_df[rate_audit_df['rate_difference'] < 0]['rate_difference'].sum())
            total_billed = rate_audit_df['billed_amount'].sum()
            total_approved = rate_audit_df['expected_total'].fillna(0).sum()
            
            compliance_rate = (rate_compliant / total_items * 100) if total_items > 0 else 0
            
            rate_compliance_metrics = {
                'total_items': total_items,
                'rate_compliant': rate_compliant,
                'rate_non_compliant': rate_non_compliant,
                'not_in_rate_sheet': not_in_sheet,
                'service_code_not_found': code_not_found,
                'compliance_rate': compliance_rate,
                'total_overcharge': total_overcharge,
                'total_undercharge': total_undercharge,
                'total_billed_amount': total_billed,
                'total_approved_amount': total_approved,
                'total_variance': total_billed - total_approved,
                'matched_count': len(rate_audit_df[rate_audit_df['matched_status'] == 'MATCHED']),
                'not_matched_count': len(rate_audit_df[rate_audit_df['matched_status'] == 'NOT_MATCHED'])
            }
        
        return line_items, concession_details, rate_compliance_metrics, header_info, rate_audit_df

    except Exception as e:
        st.error(f"❌ System encountered an issue: {str(e)}")
        return [], {}, {}, {}, pd.DataFrame()
    finally:
        # Cleanup temp file
        try:
            os.unlink(bill_path)
        except:
            pass

# ============================================================================
# MONEY LEAKAGE ANALYSIS (Common for both modules)
# ============================================================================
def calculate_money_leakage(audit_df: pd.DataFrame) -> Dict:
    """Calculate money leakage metrics from audit results"""
    
    leakage_analysis = {
        "total_billed_amount": audit_df['billed_amount'].sum(),
        "total_leakage_amount": 0,
        "leakage_by_category": {},
        "leakage_by_type": {
            "unsupported_billing": 0,
            "amount_mismatch": 0,
            "potential_missing_charges": 0
        },
        "recommendations": [],
        "priority_issues": []
    }
    
    # Calculate leakage by audit outcome
    for outcome in ['UNSUPPORTED_BILLING', 'AMOUNT_MISMATCH', 'POTENTIAL_MISSING_CHARGE']:
        if 'audit_outcome' in audit_df.columns:
            outcome_data = audit_df[audit_df['audit_outcome'] == outcome]
            total_amount = outcome_data['billed_amount'].sum()
            leakage_analysis["leakage_by_type"][outcome.lower()] = total_amount
            
            # Add to total leakage
            if outcome != 'POTENTIAL_MISSING_CHARGE':  # Missing charges are revenue loss, not overbilling
                leakage_analysis["total_leakage_amount"] += total_amount
    
    # Calculate leakage by category
    if 'category' in audit_df.columns:
        for category in audit_df['category'].unique():
            category_data = audit_df[audit_df['category'] == category]
            category_leakage = category_data[
                category_data['audit_outcome'].isin(['UNSUPPORTED_BILLING', 'AMOUNT_MISMATCH'])
            ]['billed_amount'].sum()
            
            if category_leakage > 0:
                leakage_analysis["leakage_by_category"][category] = category_leakage
    
    # Generate recommendations
    leakage_analysis = generate_recommendations(leakage_analysis, audit_df)
    
    return leakage_analysis

def generate_recommendations(leakage_analysis: Dict, audit_df: pd.DataFrame) -> Dict:
    """Generate actionable recommendations based on leakage analysis"""
    
    recommendations = []
    priority_issues = []
    
    # Analyze by leakage type
    unsupported_amount = leakage_analysis["leakage_by_type"]["unsupported_billing"]
    mismatch_amount = leakage_analysis["leakage_by_type"]["amount_mismatch"]
    missing_amount = leakage_analysis["leakage_by_type"]["potential_missing_charges"]
    
    # Priority 1: Unsupported billing (direct revenue loss risk)
    if unsupported_amount > 0:
        priority_issues.append({
            "type": "HIGH_RISK",
            "title": "Unsupported Billing Identified",
            "description": f"₹{unsupported_amount:,.2f} billed without proper documentation",
            "impact": "High financial and compliance risk",
            "action": "Immediate documentation review required"
        })
        recommendations.append({
            "priority": "HIGH",
            "action": "Review and validate all unsupported charges",
            "category": "Compliance",
            "timeline": "Immediate"
        })
    
    # Priority 2: Amount mismatches
    if mismatch_amount > 0:
        priority_issues.append({
            "type": "MEDIUM_RISK", 
            "title": "Billing Amount Discrepancies",
            "description": f"₹{mismatch_amount:,.2f} in amount mismatches found",
            "impact": "Potential revenue leakage",
            "action": "Verify billing rates and calculations"
        })
        recommendations.append({
            "priority": "MEDIUM",
            "action": "Standardize billing rates across departments",
            "category": "Revenue Integrity",
            "timeline": "1 week"
        })
    
    # Priority 3: Missing charges (revenue recovery opportunity)
    if missing_amount > 0:
        priority_issues.append({
            "type": "REVENUE_OPPORTUNITY",
            "title": "Potential Revenue Recovery",
            "description": f"₹{missing_amount:,.2f} in services documented but not billed",
            "impact": "Revenue leakage opportunity",
            "action": "Implement charge capture process"
        })
        recommendations.append({
            "priority": "HIGH",
            "action": "Establish real-time charge capture system",
            "category": "Revenue Cycle",
            "timeline": "2 weeks"
        })
    
    # Category-specific recommendations
    for category, amount in leakage_analysis["leakage_by_category"].items():
        if amount > 10000:  # Significant leakage in category
            recommendations.append({
                "priority": "MEDIUM",
                "action": f"Review {category.replace('_', ' ').title()} billing processes",
                "category": "Process Improvement",
                "timeline": "2 weeks"
            })
    
    # Add general best practices
    recommendations.extend([
        {
            "priority": "LOW",
            "action": "Implement automated billing validation checks",
            "category": "Technology",
            "timeline": "1 month"
        },
        {
            "priority": "MEDIUM", 
            "action": "Train staff on proper documentation requirements",
            "category": "Training",
            "timeline": "3 weeks"
        }
    ])
    
    leakage_analysis["recommendations"] = recommendations
    leakage_analysis["priority_issues"] = priority_issues
    
    return leakage_analysis

def create_leakage_charts(leakage_analysis: Dict, audit_df: pd.DataFrame):
    """Create visualization charts for money leakage analysis"""
    
    charts = {}
    
    # Chart 1: Leakage by Type (Pie chart)
    leakage_types = ['Unsupported Billing', 'Amount Mismatch', 'Missing Charges']
    leakage_amounts = [
        leakage_analysis["leakage_by_type"]["unsupported_billing"],
        leakage_analysis["leakage_by_type"]["amount_mismatch"], 
        leakage_analysis["leakage_by_type"]["potential_missing_charges"]
    ]
    
    # Filter out zero values
    filtered_types = []
    filtered_amounts = []
    for lt, la in zip(leakage_types, leakage_amounts):
        if la > 0:
            filtered_types.append(lt)
            filtered_amounts.append(la)
    
    if filtered_amounts:
        fig_pie = px.pie(
            values=filtered_amounts,
            names=filtered_types,
            title="Money Leakage Distribution by Type",
            color_discrete_sequence=['#FF6B6B', '#FFA726', '#42A5F5']
        )
        fig_pie.update_traces(textposition='inside', textinfo='percent+label')
        charts['leakage_by_type'] = fig_pie
    
    # Chart 2: Leakage by Category (Bar chart)
    if leakage_analysis["leakage_by_category"]:
        categories = list(leakage_analysis["leakage_by_category"].keys())
        amounts = list(leakage_analysis["leakage_by_category"].values())
        
        fig_bar = px.bar(
            x=categories,
            y=amounts,
            title="Money Leakage by Category (₹)",
            labels={'x': 'Category', 'y': 'Amount (₹)'},
            color=amounts,
            color_continuous_scale='Reds'
        )
        fig_bar.update_layout(xaxis_tickangle=-45)
        charts['leakage_by_category'] = fig_bar
    
    # Chart 3: Audit Outcomes Overview
    if 'audit_outcome' in audit_df.columns:
        outcome_counts = audit_df['audit_outcome'].value_counts()
        fig_outcomes = px.bar(
            x=outcome_counts.index,
            y=outcome_counts.values,
            title="Audit Outcomes Distribution",
            labels={'x': 'Outcome', 'y': 'Number of Items'},
            color=outcome_counts.index,
            color_discrete_map={
                'MATCH': '#4CAF50',
                'UNSUPPORTED_BILLING': '#F44336', 
                'AMOUNT_MISMATCH': '#FF9800',
                'POTENTIAL_MISSING_CHARGE': '#2196F3'
            }
        )
        charts['audit_outcomes'] = fig_outcomes
    
    # Chart 4: Matched vs Not Matched Status (for rate validation)
    if 'matched_status' in audit_df.columns:
        matched_counts = audit_df['matched_status'].value_counts()
        fig_matched = px.pie(
            values=matched_counts.values,
            names=matched_counts.index,
            title="Rate Card Match Status",
            color_discrete_sequence=['#4CAF50', '#F44336', '#FF9800']
        )
        fig_matched.update_traces(textposition='inside', textinfo='percent+label')
        charts['matched_status'] = fig_matched
    
    return charts

# ============================================================================
# MAIN STREAMLIT DASHBOARD
# ============================================================================
def main():
    st.set_page_config(
        page_title="Marvel AI - Hospital Billing Audit Suite",
        page_icon="⚡",
        layout="wide",
        initial_sidebar_state="collapsed"
    )

    # Custom CSS for professional dashboard
    st.markdown("""
    <style>
    .marvel-header {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 2rem;
        border-radius: 10px;
        color: white;
        margin-bottom: 1rem;
    }
    .marvel-title {
        font-size: 2.5rem;
        font-weight: bold;
        margin-bottom: 0.5rem;
    }
    .marvel-subtitle {
        font-size: 1.2rem;
        opacity: 0.9;
    }
    .module-card {
        background: white;
        padding: 1.5rem;
        border-radius: 10px;
        border: 1px solid #e0e0e0;
        margin: 0.5rem 0;
        transition: all 0.3s ease;
    }
    .module-card:hover {
        box-shadow: 0 4px 12px rgba(0,0,0,0.1);
        transform: translateY(-2px);
    }
    .module-active {
        border-left: 6px solid #667eea;
        background: #f8f9ff;
    }
    .leakage-card {
        background: linear-gradient(135deg, #ff6b6b 0%, #ee5a52 100%);
        padding: 1.5rem;
        border-radius: 10px;
        color: white;
        margin: 0.5rem 0;
    }
    .concession-card {
        background: linear-gradient(135deg, #4CAF50 0%, #45a049 100%);
        padding: 1.5rem;
        border-radius: 10px;
        color: white;
        margin: 0.5rem 0;
    }
    .rate-card {
        background: linear-gradient(135deg, #2196F3 0%, #1976D2 100%);
        padding: 1.5rem;
        border-radius: 10px;
        color: white;
        margin: 0.5rem 0;
    }
    .matched-badge {
        background-color: #4CAF50;
        color: white;
        padding: 2px 8px;
        border-radius: 12px;
        font-size: 0.8em;
        font-weight: bold;
    }
    .not-matched-badge {
        background-color: #F44336;
        color: white;
        padding: 2px 8px;
        border-radius: 12px;
        font-size: 0.8em;
        font-weight: bold;
    }
    </style>
    """, unsafe_allow_html=True)

    # Marvel AI Branding
    st.markdown("""
    <div class="marvel-header">
        <div class="marvel-title">Marvel AI - Hospital Billing Audit Suite</div>
        <div class="marvel-subtitle">Comprehensive Money Leakage Detection System</div>
    </div>
    """, unsafe_allow_html=True)

    # Module Selection
    st.markdown("### 🎯 Select Audit Module")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown('<div class="module-card module-active">', unsafe_allow_html=True)
        st.markdown("#### 📄 **Document Analysis**")
        st.markdown("**Medical Record vs Final Bill**")
        st.markdown("- Validate billing against medical documentation")
        st.markdown("- Detect unsupported/missing charges")
        st.markdown("- Ensure service documentation compliance")
        st.markdown("</div>", unsafe_allow_html=True)
        
        doc_analysis_selected = st.checkbox("Run Document Analysis", key="doc_analysis")
    
    with col2:
        st.markdown('<div class="module-card">', unsafe_allow_html=True)
        st.markdown("#### 📊 **Rate Validation**")
        st.markdown("**Final Bill vs Excel Rate Card**")
        st.markdown("- Validate rates against approved rate sheet")
        st.markdown("- Detect overcharges/undercharges")
        st.markdown("- Calculate money leakage amounts")
        st.markdown("</div>", unsafe_allow_html=True)
        
        rate_validation_selected = st.checkbox("Run Rate Validation", key="rate_validation")

    # Common File Upload Section
    st.markdown("---")
    st.header("📁 Upload Required Documents")
    
    # File uploaders
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Medical Records")
        medical_file = st.file_uploader(
            "Upload Patient Medical Record PDF", 
            type=["pdf"],
            key="medical",
            help="Required for Document Analysis"
        )
        if medical_file:
            st.success(f"✅ Medical record uploaded: {medical_file.name}")

    with col2:
        st.subheader("Final Bill")
        bill_file = st.file_uploader(
            "Upload Hospital Final Bill PDF", 
            type=["pdf"],
            key="bill",
            help="Required for both Document Analysis and Rate Validation"
        )
        if bill_file:
            st.success(f"✅ Final bill uploaded: {bill_file.name}")

    # Static Rate Card Information (for Rate Validation)
    if rate_validation_selected:
        st.info(f"""
        **📊 Static Rate Card Configuration:**
        - **Path:** {STATIC_RATE_CARD_PATH}
        - **Status:** {'✅ Loaded' if os.path.exists(STATIC_RATE_CARD_PATH) else '❌ Not Found'}
        - **Type:** Excel/CSV file with Service Codes and Rates
        """)

    # Run Audit Buttons
    st.markdown("---")
    
    # Document Analysis Button
    if doc_analysis_selected and medical_file and bill_file:
        if st.button("🚀 Start Document Analysis Audit", type="primary", use_container_width=True):
            # Clear previous results
            for key in ['doc_audit_complete', 'rate_audit_complete', 
                       'doc_audit_result', 'rate_audit_df',
                       'doc_bill_items', 'rate_line_items',
                       'doc_concession_details', 'rate_concession_details',
                       'doc_leakage_analysis', 'rate_leakage_analysis',
                       'doc_leakage_charts', 'rate_leakage_charts']:
                if key in st.session_state:
                    del st.session_state[key]
            
            try:
                # Run Document Analysis Audit
                with st.spinner("Running Document Analysis..."):
                    audit_result, bill_items, concession_details = run_document_analysis_audit(medical_file, bill_file)
                
                # Store in session state
                st.session_state.doc_audit_result = audit_result
                st.session_state.doc_bill_items = bill_items
                st.session_state.doc_concession_details = concession_details
                st.session_state.doc_audit_complete = True
                
                # Calculate money leakage for document analysis
                if audit_result and audit_result["audits"]:
                    audit_df = pd.DataFrame(audit_result["audits"])
                    st.session_state.doc_leakage_analysis = calculate_money_leakage(audit_df)
                    st.session_state.doc_leakage_charts = create_leakage_charts(
                        st.session_state.doc_leakage_analysis, audit_df
                    )
                
                st.success("✅ Document Analysis completed successfully!")
                
            except Exception as e:
                st.error(f"❌ Document Analysis failed: {str(e)}")
    
    # Rate Validation Button
    if rate_validation_selected and bill_file:
        if st.button("🚀 Start Rate Validation Audit", type="primary", use_container_width=True):
            try:
                # Run Rate Validation Audit
                with st.spinner("Running Rate Validation..."):
                    line_items, concession_details, rate_compliance_metrics, header_info, audit_df = run_rate_validation_audit(bill_file)
                
                # Store in session state
                st.session_state.rate_line_items = line_items
                st.session_state.rate_concession_details = concession_details
                st.session_state.rate_compliance_metrics = rate_compliance_metrics
                st.session_state.header_info = header_info
                st.session_state.rate_audit_df = audit_df
                st.session_state.rate_audit_complete = True
                
                # Calculate money leakage for rate validation
                if audit_df is not None and not audit_df.empty:
                    st.session_state.rate_leakage_analysis = calculate_money_leakage(audit_df)
                    st.session_state.rate_leakage_charts = create_leakage_charts(
                        st.session_state.rate_leakage_analysis, audit_df
                    )
                
                st.success("✅ Rate Validation completed successfully!")
                
            except Exception as e:
                st.error(f"❌ Rate Validation failed: {str(e)}")
    
    # Display Results
    st.markdown("---")
    
    # Document Analysis Results
    if st.session_state.get('doc_audit_complete', False):
        st.header("📄 Document Analysis Results")
        
        audit_result = st.session_state.doc_audit_result
        audits = audit_result.get('audits', [])
        
        if audits:
            df = pd.DataFrame(audits)
            
            # Summary statistics
            st.subheader("Quick Overview")
            col1, col2, col3, col4, col5 = st.columns(5)
            
            total_audits = len(df)
            matches = len(df[df['audit_outcome'] == 'MATCH'])
            mismatches = len(df[df['audit_outcome'] == 'AMOUNT_MISMATCH'])
            missing = len(df[df['audit_outcome'] == 'POTENTIAL_MISSING_CHARGE'])
            unsupported = len(df[df['audit_outcome'] == 'UNSUPPORTED_BILLING'])
            
            with col1:
                st.metric("Total Items", total_audits)
            with col2:
                st.metric("Matches", matches)
            with col3:
                st.metric("Mismatches", mismatches)
            with col4:
                st.metric("Missing", missing)
            with col5:
                st.metric("Unsupported", unsupported)
            
            # Detailed results
            st.subheader("Detailed Audit Results")
            st.dataframe(
                df,
                use_container_width=True,
                hide_index=True,
                height=400
            )
        
        # Money Leakage Analysis for Document Analysis
        if st.session_state.get('doc_leakage_analysis'):
            st.subheader("💰 Document Analysis - Money Leakage")
            
            leakage_analysis = st.session_state.doc_leakage_analysis
            charts = st.session_state.doc_leakage_charts
            
            # Leakage Overview Card
            st.markdown(f"""
            <div class="leakage-card">
                <h3>Total Identified Leakage: ₹{leakage_analysis['total_leakage_amount']:,.2f}</h3>
                <p>Out of ₹{leakage_analysis['total_billed_amount']:,.2f} total billed amount</p>
            </div>
            """, unsafe_allow_html=True)
            
            # Charts
            if charts:
                col1, col2 = st.columns(2)
                with col1:
                    if 'leakage_by_type' in charts:
                        st.plotly_chart(charts['leakage_by_type'], use_container_width=True)
                with col2:
                    if 'audit_outcomes' in charts:
                        st.plotly_chart(charts['audit_outcomes'], use_container_width=True)
            
            # Recommendations
            if leakage_analysis.get('recommendations'):
                st.subheader("📋 Actionable Recommendations")
                for rec in leakage_analysis['recommendations']:
                    priority_color = {
                        'HIGH': '#dc3545',
                        'MEDIUM': '#ffc107',
                        'LOW': '#28a745'
                    }.get(rec['priority'], '#007bff')
                    
                    st.markdown(f"""
                    <div style="background: #f8f9fa; padding: 1rem; border-radius: 8px; border-left: 4px solid {priority_color}; margin: 0.5rem 0;">
                        <strong style="color: {priority_color}">{rec['priority']} PRIORITY</strong><br>
                        {rec['action']}<br>
                        <small>Category: {rec['category']} | Timeline: {rec['timeline']}</small>
                    </div>
                    """, unsafe_allow_html=True)
        
        # Concession Validation for Document Analysis
        if st.session_state.get('doc_concession_details'):
            st.subheader("💳 Concession Validation")
            
            concession = st.session_state.doc_concession_details
            
            if concession:
                # Safely format concession amounts
                def format_amount(amount):
                    if amount is None:
                        return "0.00"
                    try:
                        return f"₹{float(amount):,.2f}"
                    except:
                        return "N/A"
                
                # Concession Overview
                total_bill = concession.get('total_bill_amount', 0)
                net_amount = concession.get('net_amount', 0)
                total_concession = concession.get('less_concession', 0)
                
                st.markdown(f"""
                <div class="concession-card">
                    <h3>Financial Summary</h3>
                    <p><strong>Total Bill Amount:</strong> {format_amount(total_bill)}</p>
                    <p><strong>Net Amount Payable:</strong> {format_amount(net_amount)}</p>
                    <p><strong>Total Concession:</strong> {format_amount(total_concession)}</p>
                </div>
                """, unsafe_allow_html=True)
    
    # Rate Validation Results
    if st.session_state.get('rate_audit_complete', False):
        st.header("📊 Rate Validation Results")
        
        # Create tabs for different sections
        tab1, tab2, tab3, tab4 = st.tabs([
            "Rate Compliance", 
            "Money Leakage", 
            "Concession", 
            "Download"
        ])
        
        with tab1:
            # RATE COMPLIANCE SUMMARY
            if st.session_state.get('rate_compliance_metrics'):
                rate_metrics = st.session_state.rate_compliance_metrics
                
                # Compliance Card
                compliance_rate = rate_metrics['compliance_rate']
                if compliance_rate >= 90:
                    status = "Excellent"
                elif compliance_rate >= 70:
                    status = "Moderate"
                else:
                    status = "Poor"
                
                st.markdown(f"""
                <div class="rate-card">
                    <h3>Rate Compliance: {compliance_rate:.1f}% - {status}</h3>
                    <p><strong>Total Items:</strong> {rate_metrics['total_items']}</p>
                    <p><strong>Matched:</strong> {rate_metrics.get('matched_count', 0)}</p>
                    <p><strong>Not Matched:</strong> {rate_metrics.get('not_matched_count', 0)}</p>
                </div>
                """, unsafe_allow_html=True)
                
                # Financial Impact Metrics
                st.subheader("💰 Financial Impact")
                col1, col2, col3, col4 = st.columns(4)
                with col1:
                    st.metric("Total Billed", f"₹{rate_metrics['total_billed_amount']:,.2f}")
                with col2:
                    st.metric("Total Approved", f"₹{rate_metrics['total_approved_amount']:,.2f}")
                with col3:
                    st.metric("Total Overcharge", f"₹{rate_metrics['total_overcharge']:,.2f}")
                with col4:
                    st.metric("Total Undercharge", f"₹{rate_metrics['total_undercharge']:,.2f}")
                
                # Detailed table
                if st.session_state.get('rate_audit_df') is not None:
                    st.subheader("📋 Detailed Rate Validation")
                    
                    display_df = st.session_state.rate_audit_df.copy()
                    
                    # Format currency columns
                    currency_cols = ['base_amount', 'billed_amount', 'approved_rate', 'expected_total', 'rate_difference']
                    for col in currency_cols:
                        if col in display_df.columns:
                            display_df[col] = display_df[col].apply(
                                lambda x: f"₹{x:,.2f}" if pd.notna(x) and isinstance(x, (int, float)) else "N/A"
                            )
                    
                    # Select columns to display
                    display_cols = ['charge_date', 'service_code', 'service_description', 
                                  'quantity', 'base_amount', 'billed_amount', 'approved_rate',
                                  'expected_total', 'rate_difference', 'matched_status', 'remarks']
                    
                    # Filter to available columns
                    available_cols = [col for col in display_cols if col in display_df.columns]
                    
                    st.dataframe(
                        display_df[available_cols],
                        width='stretch',
                        hide_index=True,
                        height=400
                    )
        
        with tab2:
            # MONEY LEAKAGE ANALYSIS for Rate Validation
            if st.session_state.get('rate_leakage_analysis'):
                leakage_analysis = st.session_state.rate_leakage_analysis
                charts = st.session_state.rate_leakage_charts
                
                # Leakage Overview Card
                total_leakage = leakage_analysis.get('total_leakage_amount', 0)
                total_billed = leakage_analysis.get('total_billed_amount', 0)
                
                st.markdown(f"""
                <div class="leakage-card">
                    <h3>Total Identified Leakage: ₹{total_leakage:,.2f}</h3>
                    <p>Out of ₹{total_billed:,.2f} total billed amount</p>
                    <p><strong>Leakage Percentage:</strong> {(total_leakage/total_billed*100 if total_billed > 0 else 0):.1f}%</p>
                </div>
                """, unsafe_allow_html=True)
                
                # Charts Section
                if charts:
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        if 'leakage_by_type' in charts:
                            st.plotly_chart(charts['leakage_by_type'], use_container_width=True)
                        elif 'matched_status' in charts:
                            st.plotly_chart(charts['matched_status'], use_container_width=True)
                    
                    with col2:
                        if 'leakage_by_category' in charts:
                            st.plotly_chart(charts['leakage_by_category'], use_container_width=True)
                        elif 'audit_outcomes' in charts:
                            st.plotly_chart(charts['audit_outcomes'], use_container_width=True)
                
                # Recommendations
                if leakage_analysis.get('recommendations'):
                    st.subheader("📋 Actionable Recommendations")
                    for rec in leakage_analysis['recommendations']:
                        priority_color = {
                            'HIGH': '#dc3545',
                            'MEDIUM': '#ffc107',
                            'LOW': '#28a745'
                        }.get(rec['priority'], '#007bff')
                        
                        st.markdown(f"""
                        <div style="background: #f8f9fa; padding: 1rem; border-radius: 8px; border-left: 4px solid {priority_color}; margin: 0.5rem 0;">
                            <strong style="color: {priority_color}">{rec['priority']} PRIORITY</strong><br>
                            {rec['action']}<br>
                            <small>Category: {rec['category']} | Timeline: {rec['timeline']}</small>
                        </div>
                        """, unsafe_allow_html=True)
        
        with tab3:
            # CONCESSION VALIDATION for Rate Validation
            if st.session_state.get('rate_concession_details'):
                concession = st.session_state.rate_concession_details
                
                if concession:
                    # Safely format concession amounts
                    def format_amount(amount):
                        if amount is None:
                            return "0.00"
                        try:
                            return f"₹{float(amount):,.2f}"
                        except:
                            return "N/A"
                    
                    # Concession Overview Card
                    total_bill = concession.get('total_bill_amount', 0)
                    net_amount = concession.get('net_amount', 0)
                    total_concession = concession.get('less_concession', 0)
                    
                    st.markdown(f"""
                    <div class="concession-card">
                        <h3>Financial Summary</h3>
                        <p><strong>Total Bill Amount:</strong> {format_amount(total_bill)}</p>
                        <p><strong>Net Amount Payable:</strong> {format_amount(net_amount)}</p>
                        <p><strong>Total Concession:</strong> {format_amount(total_concession)}</p>
                    </div>
                    """, unsafe_allow_html=True)
        
        with tab4:
            # DOWNLOAD REPORTS
            st.header("📥 Download Reports")
            
            col1, col2, col3 = st.columns(3)
            
            with col1:
                if st.session_state.get('rate_audit_df') is not None and not st.session_state.rate_audit_df.empty:
                    csv = st.session_state.rate_audit_df.to_csv(index=False)
                    st.download_button(
                        label="Download Rate Audit CSV",
                        data=csv,
                        file_name="rate_audit_report.csv",
                        mime="text/csv",
                        use_container_width=True
                    )
            
            with col2:
                if st.session_state.get('rate_leakage_analysis'):
                    leakage_json = json.dumps(st.session_state.rate_leakage_analysis, indent=2)
                    st.download_button(
                        label="Download Leakage Analysis",
                        data=leakage_json,
                        file_name="rate_leakage_analysis.json",
                        mime="application/json",
                        use_container_width=True
                    )
            
            with col3:
                if st.session_state.get('rate_concession_details'):
                    concession_json = json.dumps(st.session_state.rate_concession_details, indent=2)
                    st.download_button(
                        label="Download Concession Details",
                        data=concession_json,
                        file_name="rate_concession_details.json",
                        mime="application/json",
                        use_container_width=True
                    )

# Initialize session state
if 'doc_audit_complete' not in st.session_state:
    st.session_state.doc_audit_complete = False
if 'rate_audit_complete' not in st.session_state:
    st.session_state.rate_audit_complete = False
if 'doc_audit_result' not in st.session_state:
    st.session_state.doc_audit_result = None
if 'rate_audit_df' not in st.session_state:
    st.session_state.rate_audit_df = None
if 'doc_bill_items' not in st.session_state:
    st.session_state.doc_bill_items = None
if 'rate_line_items' not in st.session_state:
    st.session_state.rate_line_items = None
if 'doc_concession_details' not in st.session_state:
    st.session_state.doc_concession_details = None
if 'rate_concession_details' not in st.session_state:
    st.session_state.rate_concession_details = None
if 'doc_leakage_analysis' not in st.session_state:
    st.session_state.doc_leakage_analysis = None
if 'rate_leakage_analysis' not in st.session_state:
    st.session_state.rate_leakage_analysis = None
if 'doc_leakage_charts' not in st.session_state:
    st.session_state.doc_leakage_charts = None
if 'rate_leakage_charts' not in st.session_state:
    st.session_state.rate_leakage_charts = None

if __name__ == "__main__":
    main()