import pandas as pd
import os
import re
import tkinter as tk
from tkinter import filedialog, messagebox
import threading
import traceback

def get_matching_columns(df, possible_names):
    """Find columns in dataframe that match any of the provided possible names"""
    possible_names = [name.strip() for name in possible_names.split('Or')]
    matching_cols = []
    for name in possible_names:
        matches = [col for col in df.columns if col.strip().lower() == name.lower()]
        matching_cols.extend(matches)
    return matching_cols

def get_single_matching_column(df, possible_names):
    """Find the first column that matches any of the provided possible names"""
    possible_names = [name.strip() for name in possible_names.split('Or')]
    for name in possible_names:
        matching_cols = [col for col in df.columns if col.strip().lower() == name.lower()]
        if matching_cols:
            return matching_cols[0]
    return None

def extract_pincode_from_text(text):
    """Extract 6-digit pincode from text with improved pattern matching and logging"""
    if pd.isna(text) or text is None:
        return ''
    
    text = str(text) # Ensure we are working with string
    
    # Standard 6-digit pincode
    match = re.search(r'\b\d{6}\b', text)
    if match:
        pincode = match.group(0)
        return pincode
    
    # Try format with spaces or hyphens (e.g., 110 001 or 110-001)
    match = re.search(r'\b\d{3}[\s-]?\d{3}\b', text)
    if match:
        pincode = match.group(0).replace(' ', '').replace('-', '')
        return pincode
    
    return ''


def clean_pincode(pincode):
    """Clean and standardize pincode format"""
    if pd.isna(pincode) or pincode is None:
        return ''
    
    pincode_str = str(pincode).strip()
    digits_only = re.sub(r'\D', '', pincode_str)
    
    if len(digits_only) == 6:
        return digits_only
    
    return ''

def load_pin_database(pin_file_path, log_callback):
    """Load the pincode database with flexible column handling and error handling"""
    try:
        log_callback(f"Loading PIN database from: {pin_file_path}")
        # Try reading the specified sheet
        try:
            pin_df = pd.read_excel(pin_file_path, sheet_name="TBLPINCITY", dtype=str)
            log_callback("Successfully loaded TBLPINCITY sheet from PIN.xlsx")
        except Exception as e:
            # If that fails, try the first sheet
            log_callback(f"TBLPINCITY sheet not found: {str(e)}")
            log_callback("Trying to read the first sheet instead...")
            try:
                xls = pd.ExcelFile(pin_file_path)
                pin_df = pd.read_excel(pin_file_path, sheet_name=xls.sheet_names[0], dtype=str)
                log_callback(f"Using sheet: {xls.sheet_names[0]}")
            except Exception as e2:
                log_callback(f"Error loading PIN database: {str(e2)}")
                log_callback(traceback.format_exc())
                return pd.DataFrame(columns=['Pincode', 'City'])

        # Display column information for debugging
        log_callback(f"Found columns in PIN file: {', '.join(pin_df.columns.tolist())}")

        # Identify the pincode and city columns
        pincode_col = None
        city_col = None

        # First try exact matches
        for col in pin_df.columns:
            if col.strip().upper() == 'PINCODE':
                pincode_col = col
            elif col.strip().upper() == 'CITY':
                city_col = col

        # If not found, try partial matches
        if not pincode_col:
            for col in pin_df.columns:
                if 'PIN' in col.upper():
                    pincode_col = col
                    log_callback(f"Using '{col}' as pincode column")
                    break

        if not city_col:
            for col in pin_df.columns:
                if 'CITY' in col.upper():
                    city_col = col
                    log_callback(f"Using '{col}' as city column")
                    break

        # If still not found, use the first two columns
        if not pincode_col or not city_col:
            if len(pin_df.columns) >= 2:
                pincode_col = pin_df.columns[0]
                city_col = pin_df.columns[1]
                log_callback(f"Using first column '{pincode_col}' as pincode and second column '{city_col}' as city")
            else:
                log_callback("Error: Could not identify pincode and city columns in PIN file.  Check PIN.xlsx file structure.")
                return pd.DataFrame(columns=['Pincode', 'City'])

        # Check if the identified columns actually exist
        if pincode_col not in pin_df.columns or city_col not in pin_df.columns:
            log_callback(f"Error: Pincode column '{pincode_col}' or City column '{city_col}' not found in PIN.xlsx.  Check column names.")
            return pd.DataFrame(columns=['Pincode', 'City'])
        
        # Create a standardized dataframe
        standard_df = pd.DataFrame({
            'Pincode': pin_df[pincode_col].astype(str).apply(clean_pincode),
            'City': pin_df[city_col].astype(str).str.strip().str.upper()
        })

        # Remove rows with invalid pincodes
        valid_df = standard_df[standard_df['Pincode'].str.len() == 6].copy() # Ensure we are working with a copy.
        
        log_callback(f"Loaded {len(valid_df)} valid pincodes from PIN database")
        return valid_df

    except Exception as e:
        log_callback(f"Error loading PIN database: {str(e)}")
        log_callback(traceback.format_exc())
        return pd.DataFrame(columns=['Pincode', 'City'])



def merge_customer_files(base_directory, log_callback):
    """Merge customer files, process pincodes and cities, and handle errors robustly."""
    try:
        # Define file paths
        sender_details_file = os.path.join(base_directory, 'Sender Address.xlsx')
        input_directory = os.path.join(base_directory, 'Input')
        output_directory = os.path.join(base_directory, 'Output')
        pin_file = os.path.join(base_directory, 'PIN.xlsx')

        # Display information about the process
        log_callback(f"Base directory: {base_directory}")
        log_callback(f"Looking for PIN file at: {pin_file}")
        log_callback(f"Looking for sender details at: {sender_details_file}")
        log_callback(f"Reading input files from: {input_directory}")
        log_callback(f"Output will be saved to: {output_directory}")

        # Define column mappings
        column_mappings = {
            'SL': 'SL Or sr Or srno Or SR. NO. Or sr. no.',
            'Barcode': 'Barcode Or Barcodes Or awb Or QR Post Or POD Or pod Or Bar code  Or Bar code',
            'REF': 'REF Or reference Or code Or Reference No. Or Ref.No. Or Notice Ref. No. Or ref_no Or Ref. No.',
            'AddrePincode': 'AddrePincode Or CustAddrePincode Or CustAddrePincode Or Pincode Or Pin code Or Pin Or Pin.Code Or PIN CODE_DPM Or PIN_CODE',
            'AddreName': 'Name Or CustomerName Or name borower Or Customer Name Or CUSTOMER FULL NAME',
            'AddreCity': 'AddreCity Or CustAddreCity Or City Or district Or Dist_ Or Or CUSTADR_CITY Or DISTRICT Or DISTRICT_DPM'
        }

        address_columns = 'add 1 Or add 2 Or add 3 Or add_1 Or add_2 Or add_3 Or add1 Or add2 Or add3 Or CustAddreADD1 Or CustAddre_ADD2 Or CustAddre_ADD3 Or  State Or Customeradd1 Or Customeradd2 Or Customeradd3 Or Customeradd4 Or address1 Or address2 Or address3 Or address4 Or Address Or Customer Address Or Add_1 Or add Or ADD1 Or ADD2 Or CUSTOMER ADDRESS 1 Or CUSTOMER ADDRESS 2 Or add_1 Or CUSTOMER_ADDRESS Or  ADDRESS '

        output_columns = [
            'SL', 'Barcode', 'REF', 'SenderCity', 'SenderPincode', 'SenderName', 'SenderADD1',
            'SenderADD2', 'SenderADD3', 'AddreCity', 'AddrePincode', 'AddreName', 'AddreADD1',
            'Addre_ADD2', 'Addre_ADD3', 'ADDREMAIL', 'ADDRMOBILE', 'SENDERMOBILE', 'Weight',
            'InsVal', 'PrPdAmount', 'PrPdType', 'FMLisenceId', 'FMSomNo', 'Input File Name', 'Sheet Name'
        ]

        # Load PIN database
        log_callback("Loading PIN database...")
        pin_df = load_pin_database(pin_file, log_callback)
        if pin_df.empty:
            log_callback("Error: PIN database not loaded.  Processing stopped.")
            return

        # Load sender details
        try:
            sender_details = pd.read_excel(sender_details_file)
            log_callback(f"Successfully loaded sender details with {len(sender_details)} records")
        except Exception as e:
            log_callback(f"Error loading sender details: {str(e)}")
            log_callback(traceback.format_exc())
            sender_details = pd.DataFrame(columns=['File Name Contain', 'SenderCity', 'SenderPincode', 'SenderName', 'SenderADD1', 'SenderADD2', 'SenderADD3'])

        merged_dataframes = []
        processed_count = 0
        error_count = 0

        # Process input files
        if not os.path.exists(input_directory):
            log_callback(f"Input directory not found: {input_directory}")
            return

        input_files = [f for f in os.listdir(input_directory) if f.endswith(('.txt', '.csv', '.xlsx', '.xls'))]
        log_callback(f"Found {len(input_files)} input files to process")

        for filename in input_files:
            file_path = os.path.join(input_directory, filename)
            log_callback(f"\nProcessing file: {filename}")

            try:
                # Read input file
                if filename.endswith(('.xlsx', '.xls')):
                    try:
                        current_df = pd.read_excel(file_path, dtype=str)
                        sheet_name = pd.ExcelFile(file_path).sheet_names[0]
                    except Exception as e:
                        log_callback(f"Error reading Excel file {filename}: {str(e)}")
                        log_callback(traceback.format_exc())
                        error_count += 1
                        continue # Skip to the next file
                else:
                    try:
                        current_df = pd.read_csv(file_path, sep='\t', dtype=str, encoding='utf-8')
                        sheet_name = ''
                    except Exception as e:
                        log_callback(f"Error reading CSV/TXT file {filename}: {str(e)}")
                        log_callback(traceback.format_exc())
                        error_count += 1
                        continue  # Skip to the next file

                log_callback(f"Successfully read file with {len(current_df)} rows and {len(current_df.columns)} columns")
                log_callback(f"Columns found: {', '.join(current_df.columns.tolist()[:5])}{'...' if len(current_df.columns) > 5 else ''}")

                # Find matching sender details
                base_filename = filename.split('-')[0].split('.')[0].strip()
                sender_row = pd.DataFrame()
                if not sender_details.empty:
                    sender_row = sender_details[sender_details['File Name Contain'].str.contains(base_filename, na=False, case=False)]

                if not sender_row.empty:
                    log_callback(f"Found matching sender details for '{base_filename}'")

                    # Map columns according to mapping dictionary
                    actual_mapping = {}
                    for target_col, possible_names in column_mappings.items():
                        source_col = get_single_matching_column(current_df, possible_names)
                        if source_col:
                            actual_mapping[source_col] = target_col
                            log_callback(f"Mapped '{source_col}' to '{target_col}'")

                    if actual_mapping:
                        try:
                            current_df.rename(columns=actual_mapping, inplace=True)
                        except KeyError as e:
                            log_callback(f"Error: Column not found during renaming: {e}")
                            log_callback(traceback.format_exc())
                            error_count += 1
                            continue # Skip to next file.
                    
                    # Process address columns
                    address_cols = get_matching_columns(current_df, address_columns)
                    if address_cols:
                        log_callback(f"Found address columns: {', '.join(address_cols)}")
                        current_df['AddreADD1'] = current_df[address_cols].fillna('').apply(
                            lambda x: ', '.join(filter(None, x)), axis=1
                        ).str.strip() #remove leading/trailing spaces

                    # Add sender information
                    if not sender_row.empty: # Check if sender_row is not empty
                        sender_info = sender_row.iloc[0]
                        current_df['SenderCity'] = sender_info.get('SenderCity', '')
                        current_df['SenderPincode'] = sender_info.get('SenderPincode', '')
                        current_df['SenderName'] = sender_info.get('SenderName', '')
                        current_df['SenderADD1'] = sender_info.get('SenderADD1', '')
                        current_df['SenderADD2'] = sender_info.get('SenderADD2', '')
                        current_df['SenderADD3'] = sender_info.get('SenderADD3', '')
                    else:
                        log_callback(f"No sender details found for {filename}")
                        current_df['SenderCity'] = ''
                        current_df['SenderPincode'] = ''
                        current_df['SenderName'] = ''
                        current_df['SenderADD1'] = ''
                        current_df['SenderADD2'] = ''
                        current_df['SenderADD3'] = ''

                    # Generate sequential SL numbers
                    current_df['SL'] = range(1, len(current_df) + 1)

                    # Ensure all output columns exist
                    for col in output_columns:
                        if col not in current_df.columns:
                            current_df[col] = ''

                    # Add file metadata
                    current_df['Input File Name'] = filename
                    current_df['Sheet Name'] = sheet_name

                    # Select only the required columns
                    current_df = current_df[output_columns].copy() #make a copy
                    merged_dataframes.append(current_df)
                    processed_count += 1
                    log_callback(f"Successfully processed: {filename}")

            except Exception as e:
                log_callback(f"Error processing {filename}: {str(e)}")
                log_callback(traceback.format_exc())
                error_count += 1

        # Combine all processed dataframes
        if merged_dataframes:
            final_df = pd.concat(merged_dataframes, ignore_index=True)
            
            # Process pincodes and cities *after* merging
            process_pincodes_and_cities(final_df, pin_df, log_callback)
            
            os.makedirs(output_directory, exist_ok=True)
            output_file = os.path.join(output_directory, f'Output-Post_File_{pd.Timestamp.now().strftime("%d%m%Y")}.xlsx')
            
            log_callback(f"\nSaving output file with {len(final_df)} total records...")
            final_df.to_excel(output_file, index=False)
            log_callback(f"Output saved to: {output_file}")
            log_callback(f"\nProcessing summary:")
            log_callback(f"Total files processed successfully: {processed_count}")
            log_callback(f"Total files with errors: {error_count}")
        else:
            log_callback("\nNo files were successfully processed.")

    except Exception as e:
        log_callback(f"Unexpected error: {str(e)}")
        log_callback(traceback.format_exc())

def process_pincodes_and_cities(df, pin_df, log_callback):
    """
    Extracts pincodes and assigns cities to the given DataFrame.

    Args:
        df: The DataFrame to process.
        pin_df: The DataFrame containing the pincode-city mapping.
        log_callback: A function to use for logging.
    """
    log_callback("Processing pincodes and cities for the merged dataframe...")

    # Process pincodes
    if 'AddrePincode' in df.columns:
        log_callback("Processing pincodes...")
        df['AddrePincode'] = df['AddrePincode'].fillna('').astype(str).apply(clean_pincode)

        # Extract pincodes from address fields if pincode is missing
        pincode_found_count = 0
        for i, row in df.iterrows():
            if not row['AddrePincode']:
                for col in ['AddreADD1', 'Addre_ADD2', 'Addre_ADD3']:
                    if col in df.columns:
                        address_text = row.get(col, '')
                        pin = extract_pincode_from_text(address_text)
                        if pin:
                            df.at[i, 'AddrePincode'] = pin
                            pincode_found_count += 1
                            break
        log_callback(f"Extracted {pincode_found_count} pincodes from address fields")
    else:
        log_callback("No AddrePincode column found in the input data")
        df['AddrePincode'] = ''

    # Process cities
    if 'AddreCity' in df.columns:
        log_callback("Processing cities...")
        df['AddreCity'] = df['AddreCity'].fillna('').astype(str).str.strip()

        # Look up city from pincode if city is missing
        city_found_count = 0
        for i, row in df.iterrows():
            if (not row['AddreCity'] or row['AddreCity'] == '') and row['AddrePincode']:
                match = pin_df[pin_df['Pincode'] == row['AddrePincode']]
                if not match.empty:
                    df.at[i, 'AddreCity'] = match.iloc[0]['City']
                    city_found_count += 1
                else:
                    log_callback(f"Pincode {row['AddrePincode']} not found in PIN database.")
        log_callback(f"Mapped {city_found_count} cities from pincodes")
    else:
        log_callback("No AddreCity column found in the input data")
        df['AddreCity'] = ''



def run_gui():
    root = tk.Tk()
    root.title("Post File Merger 3.0 - Enhanced")
    root.geometry("900x600")
    root.configure(bg="#f0f8ff")

    def browse_folder():
        folder = filedialog.askdirectory()
        if folder:
            path_entry.delete(0, tk.END)
            path_entry.insert(0, folder)

    def start_merge():
        # Clear the log
        log_text.delete(1.0, tk.END)
        
        base_path = path_entry.get()
        if not os.path.isdir(base_path):
            messagebox.showerror("Error", "Invalid base directory path.")
            return

        # Disable buttons during processing
        browse_button.config(state=tk.DISABLED)
        start_button.config(state=tk.DISABLED)
        
        def log_message(msg):  # Changed name to log_message
            log_text.insert(tk.END, msg + "\n")
            log_text.see(tk.END)
            root.update_idletasks()

        def thread_complete():
            # Re-enable buttons
            browse_button.config(state=tk.NORMAL)
            start_button.config(state=tk.NORMAL)
            log_message("\nProcessing complete!") # Changed here too

        # Start processing in a separate thread
        thread = threading.Thread(target=lambda: [merge_customer_files(base_path, log_message), thread_complete()], daemon=True) # And here
        thread.start()

    # Create main frame
    main_frame = tk.Frame(root, bg="#f0f8ff")
    main_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)

    # Create header frame
    header_frame = tk.Frame(main_frame, bg="#f0f8ff")
    header_frame.pack(fill=tk.X, pady=5)
    
    # Add title label
    title_label = tk.Label(header_frame, text="Post File Merger - Enhanced Pincode Edition", 
                          bg="#f0f8ff", fg="#000080", font=('Arial', 14, 'bold'))
    title_label.pack(side=tk.LEFT, padx=5)

    # Create input frame
    input_frame = tk.Frame(main_frame, bg="#f0f8ff")
    input_frame.pack(fill=tk.X, pady=5)
    
    # Directory selection
    tk.Label(input_frame, text="Base Directory:", bg="#f0f8ff", fg="#000080", font=('Arial', 10, 'bold')).pack(side=tk.LEFT, padx=5)
    path_entry = tk.Entry(input_frame, width=70, font=('Courier', 10))
    path_entry.pack(side=tk.LEFT, padx=5, expand=True, fill=tk.X)
    browse_button = tk.Button(input_frame, text="Browse", command=browse_folder, bg="#4682b4", fg="white", font=('Arial', 9, 'bold'))
    browse_button.pack(side=tk.LEFT, padx=5)
    start_button = tk.Button(input_frame, text="Start Processing", command=start_merge, bg="#32cd32", fg="white", font=('Arial', 9, 'bold'))
    start_button.pack(side=tk.LEFT, padx=5)

    # Create status frame
    status_frame = tk.Frame(main_frame, bg="#f0f8ff")
    status_frame.pack(fill=tk.X, pady=5)
    
    # Create log frame with scrollbars
    log_frame = tk.Frame(main_frame, bg="#f0f8ff")
    log_frame.pack(fill=tk.BOTH, expand=True, pady=5)
    
    # Add scrollbars
    scrollbar_y = tk.Scrollbar(log_frame)
    scrollbar_y.pack(side=tk.RIGHT, fill=tk.Y)
    scrollbar_x = tk.Scrollbar(log_frame, orient=tk.HORIZONTAL)
    scrollbar_x.pack(side=tk.BOTTOM, fill=tk.X)
    
    # Add log text widget
    log_text = tk.Text(log_frame, width=110, height=30, bg="#ffffff", fg="#000000", 
                      font=('Consolas', 9), wrap=tk.NONE,
                      xscrollcommand=scrollbar_x.set, yscrollcommand=scrollbar_y.set)
    log_text.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
    
    # Configure scrollbars
    scrollbar_y.config(command=log_text.yview)
    scrollbar_x.config(command=log_text.xview)
    
    # Add initial instructions to log
    log_text.insert(tk.END, "Welcome to Post File Merger - Enhanced PincodeEdition\n")
    log_text.insert(tk.END, "---------------------------------------------------\n")
    log_text.insert(tk.END, "1. Select the base directory containing:\n")
    log_text.insert(tk.END,"   - 'PIN.xlsx' with pincode and city data\n")
    log_text.insert(tk.END, "   - 'Sender Address.xlsx' with sender information\n")
    log_text.insert(tk.END, "   - 'Input' folder with files to process\n")
    log_text.insert(tk.END, "   - 'Output' folder will be created if it doesn't exist\n")
    log_text.insert(tk.END, "2. Click 'Start Processing' to begin\n\n")
    log_text.insert(tk.END, "The program will:\n")
    log_text.insert(tk.END, "- Extract and clean pincodes from input files\n")
    log_text.insert(tk.END, "- Match cities to pincodes using the PIN database\n")
    log_text.insert(tk.END, "- Add sender information based on the filename\n")
    log_text.insert(tk.END, "- Generate consolidated output file\n\n")

    root.mainloop()

if __name__ == "__main__":
    run_gui()

