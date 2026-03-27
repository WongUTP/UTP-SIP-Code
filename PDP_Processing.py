import pandas as pd
import os
import shutil
from datetime import datetime
import numpy as np

# ===== CONFIGURATION =====
folder_path = r"\\SHARED_FOLDER_PATH\Outlook PDP Autosave"
base_file_path = os.path.join(folder_path, "BASE FILE.xlsx")
backup_folder = os.path.join(folder_path, "Backups")

columns_to_read = [
    'MODULE_GROUP', 
    'LotName', 
    'Lot_Package', 
    'Lot_MESHoldReason', 
    'Issue_Causing_Equipment', 
    'Issue_CreationDate_lt', 
    'Issue_Description', 
    'Root Cause Containment', 
    'Root Cause Corrective', 
    'Root Cause Verification'
]

unique_id_column = 'LotName'

####################################--------MOLDING DEFECT LIST------------#########################################################
filters = {
    'MODULE_GROUP': ['QFP'],
    'Lot_MESHoldReason': [
        'Chipped Casing_1', 'Compound Overflow/Bleeding', 'Crack Casing', 
        'Delamination On Die Pad', 'Incomplete Mould', 'Incomplete Shot', 
        'Irregular Surface', 'Mold Flashes', 'Mold Flashes On Back HeatSink', 
        'Mold Flashes On Lead', 'Mould Void', 'Porous Surface', 'Sag Down Wire', 
        'Sagging Wire', 'Sagging Wire In Z Direction', 'Sagging Wire_1', 
        'Wire Shorted To Wire', 'Wire Sweep'
    ]
}
###################################################################################################################################

exclude_lotnames = ['ABC123', 'DEF456', 'GHI789']
MAX_BACKUPS = 5

# =========================

def get_available_files(folder):
    """Get all Excel files in folder (excluding BASE FILE)"""
    files = [f for f in os.listdir(folder) 
             if f.endswith('.xlsx') 
             and not f.startswith('~$')
             and f != 'BASE FILE.xlsx']
    
    # Sort by creation time (newest first)
    files_with_time = [(f, os.path.getctime(os.path.join(folder, f))) for f in files]
    files_with_time.sort(key=lambda x: x[1], reverse=True)
    
    return [f[0] for f in files_with_time]

def prompt_file_selection(folder):
    """Prompt user to select a file with smart suggestions"""
    files = get_available_files(folder)
    
    if not files:
        print("\n❌ No Excel files found in folder")
        return None
    
    print("\n" + "="*70)
    print("SELECT FILE TO PROCESS")
    print("="*70)
    
    # Show suggestions
    latest = files[0]
    latest_time = datetime.fromtimestamp(os.path.getctime(os.path.join(folder, latest)))
    
    print(f"\n📌 LATEST FILE:")
    print(f"   {latest}")
    print(f"   Created: {latest_time.strftime('%Y-%m-%d %H:%M:%S')}")
    
    if len(files) > 1:
        print(f"\n📁 Total files available: {len(files)}")
    
    print("\n" + "-"*70)
    print("OPTIONS:")
    print("  [ENTER]  - Process latest file (recommended)")
    print("  [list]   - Show all available files")
    print("  [name]   - Enter specific filename")
    print("  [q]      - Quit")
    print("-"*70)
    
    while True:
        choice = input("\nYour choice: ").strip()
        
        if choice.lower() == 'q':
            return None
        
        if choice == '':
            print(f"\n✅ Selected: {latest}")
            return os.path.join(folder, latest)
        
        if choice.lower() == 'list':
            print("\n" + "="*70)
            print("ALL AVAILABLE FILES")
            print("="*70)
            for i, filename in enumerate(files, 1):
                filepath = os.path.join(folder, filename)
                ctime = datetime.fromtimestamp(os.path.getctime(filepath))
                size_mb = os.path.getsize(filepath) / (1024 * 1024)
                print(f"{i:2}. {filename}")
                print(f"    {ctime.strftime('%Y-%m-%d %H:%M')} | {size_mb:.1f} MB")
            print("="*70)
            
            sub_choice = input("\nEnter number or filename (or ENTER for latest): ").strip()
            
            if sub_choice == '':
                print(f"\n✅ Selected: {latest}")
                return os.path.join(folder, latest)
            
            try:
                # Try as number
                index = int(sub_choice) - 1
                if 0 <= index < len(files):
                    print(f"\n✅ Selected: {files[index]}")
                    return os.path.join(folder, files[index])
                else:
                    print(f"❌ Please enter a number between 1 and {len(files)}")
                    continue
            except ValueError:
                # Try as filename
                choice = sub_choice
        
        # Try to match filename (partial match ok)
        matches = [f for f in files if choice.lower() in f.lower()]
        
        if len(matches) == 1:
            print(f"\n✅ Selected: {matches[0]}")
            return os.path.join(folder, matches[0])
        elif len(matches) > 1:
            print(f"\n⚠️  Multiple matches found:")
            for i, m in enumerate(matches, 1):
                print(f"   {i}. {m}")
            print("   Please be more specific or use 'list' to see all files")
        else:
            print(f"❌ File not found: {choice}")
            print("   Use 'list' to see all available files")

def check_null_values(df, stage_name):
    """
    Check for null values in dataframe and report
    """
    print(f"\n🔍 NULL CHECK - {stage_name}:")
    
    null_counts = df.isnull().sum()
    null_cols = null_counts[null_counts > 0]
    
    if len(null_cols) == 0:
        print(f"   ✅ No null values found")
        return
    
    print(f"   ⚠️  Found null values in {len(null_cols)} columns:")
    for col, count in null_cols.items():
        pct = (count / len(df)) * 100
        print(f"      {col}: {count:,} nulls ({pct:.1f}%)")
    
    # Show sample rows with nulls
    has_nulls = df[df.isnull().any(axis=1)]
    if len(has_nulls) > 0:
        print(f"\n   📋 Sample rows with nulls (showing first 3):")
        pd.set_option('display.max_columns', None)
        pd.set_option('display.width', 200)
        print(has_nulls.head(3).to_string(index=True))

def is_processed(df):
    """Check if dataframe has already been processed"""
    return 'INFO' in df.columns

def process_data(df, exclude_lots=None, debug=False):
    """Transform pipeline with data cleaning"""
    if is_processed(df):
        print(f"   ℹ️  Data already processed (has INFO column)")
        return df
    
    print(f"\n🔄 Processing data...")
    print(f"   Raw: {len(df):,} rows")
    
    if debug:
        check_null_values(df, "BEFORE PROCESSING")
    
    # Clean LotName
    if 'LotName' in df.columns:
        before_clean = len(df)
        df['LotName'] = df['LotName'].astype(str).str.strip()
        df = df[df['LotName'] != 'nan']
        df = df[df['LotName'] != '']
        removed = before_clean - len(df)
        if removed > 0:
            print(f"   Removed {removed} rows with invalid LotName")
    
    if debug:
        check_null_values(df, "AFTER LOTNAME CLEANING")
    
    # Exclude specific LotNames
    if exclude_lots and 'LotName' in df.columns:
        before = len(df)
        df = df[~df['LotName'].isin(exclude_lots)]
        excluded = before - len(df)
        if excluded > 0:
            print(f"   Excluded {excluded} rows (specific LotNames)")
    
    # Apply filters
    print(f"\n   📊 Applying filters:")
    for col, values in filters.items():
        if col in df.columns:
            before = len(df)
            df = df[df[col].isin(values)]
            after = len(df)
            removed_pct = ((before - after) / before * 100) if before > 0 else 0
            print(f"      {col}: {before:,} → {after:,} ({removed_pct:.1f}% removed)")
        else:
            print(f"      ⚠️  {col}: Column not found, skipping")
    
    print(f"\n   ✅ After filters: {len(df):,} rows")
    
    if len(df) == 0:
        return df
    
    if debug:
        check_null_values(df, "AFTER FILTERING")
    
    # Convert date
    if 'Issue_CreationDate_lt' in df.columns:
        df['Issue_CreationDate_lt'] = pd.to_datetime(df['Issue_CreationDate_lt'], errors='coerce')
        
        # Check if date conversion created nulls
        null_dates = df['Issue_CreationDate_lt'].isnull().sum()
        if null_dates > 0:
            print(f"\n   ⚠️  Warning: {null_dates} dates could not be parsed")
            if debug:
                print(f"      Sample invalid dates:")
                invalid_dates = df[df['Issue_CreationDate_lt'].isnull()][['LotName', 'Issue_CreationDate_lt']].head(3)
                print(invalid_dates.to_string(index=False))
    
    # Create INFO with detailed debugging
    if debug:
        print(f"\n   🔬 DEBUGGING INFO CREATION:")
        # Check what values exist before creating INFO
        for col in ['LotName', 'Issue_CreationDate_lt', 'Issue_Causing_Equipment', 'Lot_Package']:
            if col in df.columns:
                null_count = df[col].isnull().sum()
                print(f"      {col}: {null_count} nulls out of {len(df)}")
    
    info_parts = []
    
    if 'LotName' in df.columns:
        lot_part = 'Lot#: ' + df['LotName'].fillna('N/A').astype(str)
        info_parts.append(lot_part)
        if debug:
            print(f"      Added LotName part, nulls: {lot_part.isnull().sum()}")
    
    if 'Issue_CreationDate_lt' in df.columns:
        date_str = df['Issue_CreationDate_lt'].apply(
            lambda x: x.strftime('%Y-%m-%d') if pd.notna(x) else 'N/A'
        )
        date_part = '\nDate: ' + date_str
        info_parts.append(date_part)
        if debug:
            print(f"      Added Date part, nulls: {date_part.isnull().sum()}")
    
    if 'Issue_Causing_Equipment' in df.columns:
        mc_part = '\nMC: ' + df['Issue_Causing_Equipment'].fillna('N/A').astype(str)
        info_parts.append(mc_part)
        if debug:
            print(f"      Added MC part, nulls: {mc_part.isnull().sum()}")
    
    if 'Lot_Package' in df.columns:
        pg_part = '\nPG: ' + df['Lot_Package'].fillna('N/A').astype(str)
        info_parts.append(pg_part)
        if debug:
            print(f"      Added PG part, nulls: {pg_part.isnull().sum()}")
    
    if info_parts:
        df['INFO'] = info_parts[0]
        if debug:
            print(f"      Initial INFO (first part), nulls: {df['INFO'].isnull().sum()}")
        
        for i, part in enumerate(info_parts[1:], 1):
            df['INFO'] = df['INFO'] + part
            if debug:
                print(f"      After adding part {i+1}, INFO nulls: {df['INFO'].isnull().sum()}")
    
    if debug:
        check_null_values(df, "AFTER INFO CREATION")
        
        # Deep dive into INFO nulls
        info_null_rows = df[df['INFO'].isnull()]
        if len(info_null_rows) > 0:
            print(f"\n   🚨 FOUND {len(info_null_rows)} ROWS WITH NULL INFO!")
            print(f"   Inspecting these rows in detail:\n")
            
            for idx in info_null_rows.index[:3]:  # Check first 3
                print(f"   Row index {idx}:")
                row = df.loc[idx]
                
                for col in ['LotName', 'Issue_CreationDate_lt', 'Issue_Causing_Equipment', 'Lot_Package', 'INFO']:
                    if col in df.columns:
                        val = row[col]
                        print(f"      {col}: {repr(val)} (type: {type(val).__name__}, is_null: {pd.isna(val)})")
                print()
    
    # Drop columns
    cols_to_drop = ['MODULE_GROUP', 'Issue_Causing_Equipment', 'Lot_Package']
    df = df.drop(columns=[col for col in cols_to_drop if col in df.columns])
    
    # Rename
    rename_map = {
        'Lot_MESHoldReason': 'DEFECTS',
        'Issue_Description': 'DESCRIPTION',
        'Root Cause Containment': 'CONTAINMENT ACTION',
        'Root Cause Corrective': 'CORRECTIVE ACTION',
        'Root Cause Verification': 'ROOT CAUSE'
    }
    rename_map = {old: new for old, new in rename_map.items() if old in df.columns}
    df = df.rename(columns=rename_map)
    
    if debug:
        check_null_values(df, "FINAL (AFTER RENAME)")
        
        # Show which specific rows have nulls in final data
        final_nulls = df[df.isnull().any(axis=1)]
        if len(final_nulls) > 0:
            print(f"\n   🔬 DETAILED NULL ANALYSIS:")
            print(f"   Found {len(final_nulls)} rows with null values")
            print(f"\n   Sample problematic rows:")
            for idx in final_nulls.head(3).index:
                row = df.loc[idx]
                print(f"\n   Row {idx}:")
                for col in df.columns:
                    value = row[col]
                    if pd.isna(value):
                        print(f"      ❌ {col}: NULL")
                    else:
                        print(f"      ✅ {col}: {str(value)[:50]}")
    
    return df

def main():
    print("\n" + "="*70)
    print("PROCESS DATA FILES")
    print("="*70)
    
    # Ask if user wants debug mode
    debug_mode = input("\nEnable debug mode to track null values? (y/n, default=n): ").strip().lower() == 'y'
    
    # ========================================
    # SELECT FILE
    # ========================================
    selected_file = prompt_file_selection(folder_path)
    
    if not selected_file:
        print("\n❌ No file selected. Exiting.")
        return
    
    filename = os.path.basename(selected_file)
    
    # ========================================
    # READ BASE FILE
    # ========================================
    print("\n📖 Reading base file...")
    if os.path.exists(base_file_path):
        try:
            base_df = pd.read_excel(base_file_path)
            print(f"   Loaded: {len(base_df):,} rows")
            if debug_mode:
                check_null_values(base_df, "BASE FILE")
        except Exception as e:
            print(f"   ❌ Error reading base: {e}")
            print("   Creating new base file")
            base_df = pd.DataFrame()
    else:
        print("   Base file not found - will create new one")
        base_df = pd.DataFrame()
    
    # ========================================
    # READ SELECTED FILE
    # ========================================
    print(f"\n📄 Reading: {filename}")
    
    try:
        # Try reading with expected columns
        new_df = pd.read_excel(selected_file, usecols=columns_to_read)
        print(f"   Loaded: {len(new_df):,} rows")
    except ValueError as e:
        # If columns don't exist, read all columns
        print(f"   ⚠️  Some expected columns not found, reading all columns")
        new_df = pd.read_excel(selected_file)
        print(f"   Loaded: {len(new_df):,} rows with columns: {list(new_df.columns)}")
        
        # Check if required columns exist
        if 'LotName' not in new_df.columns:
            print(f"\n❌ ERROR: 'LotName' column not found!")
            print(f"   Available columns: {list(new_df.columns)}")
            return
    
    # ========================================
    # PROCESS DATA
    # ========================================
    new_df = process_data(new_df, exclude_lots=exclude_lotnames, debug=debug_mode)
    
    if len(new_df) == 0:
        print("\n⚠️  No rows remain after filtering!")
        print("   Possible reasons:")
        print("   1. Filters don't match data format")
        print("   2. All data was excluded")
        print("   3. Source data was pre-filtered")
        
        response = input("\nDo you want to see the raw data? (y/n): ").strip().lower()
        if response == 'y':
            raw_df = pd.read_excel(selected_file)
            print(f"\n📊 Raw data sample (first 10 rows):")
            print(raw_df.head(10).to_string())
        return
    
    # ========================================
    # FIND NEW RECORDS
    # ========================================
    print(f"\n🔍 Checking for duplicates based on '{unique_id_column}'...")
    
    if base_df.empty:
        new_records = new_df.copy()
        print(f"   Base is empty - all {len(new_records):,} records are new")
    elif 'LotName' in base_df.columns and 'LotName' in new_df.columns:
        existing_lots = set(base_df['LotName'].values)
        new_records = new_df[~new_df['LotName'].isin(existing_lots)].copy()
        
        print(f"   Total in file: {len(new_df):,}")
        print(f"   Already in base: {len(new_df) - len(new_records):,}")
        print(f"   New records: {len(new_records):,}")
    elif 'INFO' in base_df.columns and 'LotName' in new_df.columns:
        base_df['LotName_temp'] = base_df['INFO'].str.extract(r'Lot#:\s*([^\n]+)')
        existing_lots = set(base_df['LotName_temp'].dropna().values)
        new_records = new_df[~new_df['LotName'].isin(existing_lots)].copy()
        base_df = base_df.drop(columns=['LotName_temp'])
        
        print(f"   Total in file: {len(new_df):,}")
        print(f"   Already in base: {len(new_df) - len(new_records):,}")
        print(f"   New records: {len(new_records):,}")
    else:
        new_records = new_df.copy()
        print(f"   Cannot check duplicates - treating all as new")
    
    if len(new_records) == 0:
        print("\n✅ No new records to add - all data already in base")
        return
    
    # ========================================
    # DISPLAY NEW RECORDS
    # ========================================
    print("\n" + "="*70)
    print(f"NEW RECORDS TO ADD: {len(new_records)}")
    print("="*70)
    
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', 150)
    pd.set_option('display.max_colwidth', 40)
    
    display_cols = ['LotName', 'Issue_CreationDate_lt', 'DEFECTS', 'DESCRIPTION']
    display_cols = [col for col in display_cols if col in new_records.columns]
    
    if len(new_records) <= 20:
        print(new_records[display_cols].to_string(index=False))
    else:
        print(f"\nShowing first 10 and last 10 of {len(new_records)} records:\n")
        print(new_records[display_cols].head(10).to_string(index=False))
        print("\n... ({} more rows) ...\n".format(len(new_records) - 20))
        print(new_records[display_cols].tail(10).to_string(index=False))
    
    print("="*70)
    
    # Final null check before saving
    if debug_mode:
        check_null_values(new_records, "NEW RECORDS (FINAL CHECK)")
    
    # ========================================
    # CONFIRM
    # ========================================
    response = input(f"\n💾 Add these {len(new_records)} records to BASE FILE? (y/n): ").strip().lower()
    
    if response != 'y':
        print("\n❌ Operation cancelled")
        return
    
    # ========================================
    # BACKUP BASE FILE
    # ========================================
    if os.path.exists(base_file_path):
        os.makedirs(backup_folder, exist_ok=True)
        backup_name = f"BASE_FILE_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        backup_path = os.path.join(backup_folder, backup_name)
        shutil.copy2(base_file_path, backup_path)
        print(f"\n💾 Created backup: {backup_name}")
        
        # Cleanup old backups
        backups = sorted([f for f in os.listdir(backup_folder) if f.startswith('BASE_FILE_backup_')])
        if len(backups) > MAX_BACKUPS:
            for old_backup in backups[:-MAX_BACKUPS]:
                os.remove(os.path.join(backup_folder, old_backup))
                print(f"   🗑️  Deleted old backup: {old_backup}")
    
    # ========================================
    # APPEND AND SAVE
    # ========================================
    if base_df.empty:
        updated_base = new_records
    else:
        updated_base = pd.concat([base_df, new_records], ignore_index=True)
    
    updated_base.to_excel(base_file_path, index=False)
    
    print(f"\n✅ SUCCESS!")
    print(f"   Added: {len(new_records):,} new records")
    print(f"   Base file now has: {len(updated_base):,} total rows")
    print(f"   Backups kept: {min(len(os.listdir(backup_folder)), MAX_BACKUPS)}")

if __name__ == '__main__':
    main()