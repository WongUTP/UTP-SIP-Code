import pandas as pd
from pathlib import Path
import json
import hashlib
from datetime import datetime
import shutil
from time import perf_counter

T0 = perf_counter()

def log(msg):
    dt = perf_counter() - T0
    print(f"[{dt:8.2f}s] {msg}")

def log_kv(title, **kv):
    dt = perf_counter() - T0
    items = " | ".join(f"{k}={v}" for k, v in kv.items())
    print(f"[{dt:8.2f}s] {title}: {items}")

# ========================================
# USER INPUT HELPERS
# ========================================
def ask_yes_no(prompt, default=False):
    suffix = " [Y/n]: " if default else " [y/N]: "
    while True:
        answer = input(prompt + suffix).strip().lower()
        if answer == "":
            return default
        if answer in ("y", "yes"):
            return True
        if answer in ("n", "no"):
            return False
        print("⚠️  Please enter 'y' or 'n'")

def backup_output_file(output_file):
    if not output_file.exists():
        print("ℹ️  No existing output file to backup")
        return None

    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    backup_name = output_file.stem + f'_backup_{timestamp}' + output_file.suffix
    backup_path = output_file.parent / backup_name

    try:
        shutil.copy2(output_file, backup_path)
        print(f"📦 Backup created: {backup_path.name}")
        return backup_path
    except Exception as e:
        print(f"⚠️  Backup failed: {e}")
        return None

# ========================================
# CONFIGURATION
# ========================================
NETWORK_BASE = Path(r'\\SHARED_FOLDER_PATH')
BASE_FILE = NETWORK_BASE / 'Outlook PDP Autosave/BASE FILE.xlsx'
TCKO_BASE = Path(r'\\TCKO_PATH')

TEST_MODE = ask_yes_no("\n🧪 Run in TEST mode?", default=False)

if TEST_MODE:
    print("\n" + "="*70)
    print("⚠️  RUNNING IN TEST MODE ⚠️")
    print("="*70 + "\n")
    TRACKING_FILE = NETWORK_BASE / 'test/processed_files_TEST.json'
    RESULT_CACHE_FILE = NETWORK_BASE / 'test/result_cache_TEST.pkl'
    DDM_CACHE = NETWORK_BASE / 'test/DDM_List_TEST.pkl'  # baseline read-only
    COMPILED_TCKO_CACHE = NETWORK_BASE / 'test/Compiled_TCKO_TEST.pkl'
    APC_DATA_CACHE_FILE = NETWORK_BASE / 'test/apc_data_cache_TEST.pkl'
    OUTPUT_FILE = NETWORK_BASE / 'test/APCOutput_TEST.xlsx'
    (NETWORK_BASE / 'test').mkdir(exist_ok=True)
else:
    print("\n" + "="*70)
    print("🚀 RUNNING IN PRODUCTION MODE")
    print("="*70 + "\n")
    TRACKING_FILE = NETWORK_BASE / 'processed_files.json'
    RESULT_CACHE_FILE = NETWORK_BASE / 'result_cache.pkl'
    DDM_CACHE = NETWORK_BASE / 'DDM_List.pkl'            # baseline read-only
    COMPILED_TCKO_CACHE = NETWORK_BASE / 'Compiled_TCKO.pkl'
    APC_DATA_CACHE_FILE = NETWORK_BASE / 'apc_data_cache.pkl'
    OUTPUT_FILE = NETWORK_BASE / 'APCOutput.xlsx'

# ========================================
# CACHE RESET (NEVER DELETE DDM)
# ========================================
CACHES = {
    "tracking": TRACKING_FILE,
    "result_cache": RESULT_CACHE_FILE,
    "apc_cache": APC_DATA_CACHE_FILE,
    "compiled_tcko": COMPILED_TCKO_CACHE,
    # NOTE: DDM_CACHE intentionally NOT included
}

def delete_cache_files(*keys):
    """Delete selected cache files by key name. Never deletes DDM."""
    for k in keys:
        p = CACHES.get(k)
        if p is None:
            print(f"⚠️ Unknown cache key: {k}")
            continue
        p = Path(p)
        if p.exists():
            p.unlink()
            print(f"🗑️ Deleted {k}: {p}")
        else:
            print(f"ℹ️ Not found {k}: {p}")

def interactive_cache_reset_menu():
    print("\n" + "="*70)
    print("🧹 CACHE RESET MENU (DDM will NOT be deleted)")
    print("="*70)
    print("1) Reset ONLY result cache (forces re-merge)                [result_cache]")
    print("2) Reset compiled TCKO cache (forces rebuild when BASE changes) [compiled_tcko]")
    print("3) Reset APC cache + tracking (forces re-read APC excels)    [apc_cache, tracking]")
    print("4) Reset EVERYTHING except DDM                              [tracking, apc_cache, result_cache, compiled_tcko]")
    print("5) Cancel")
    choice = input("Select option (1-5): ").strip()

    if choice == "1":
        delete_cache_files("result_cache")
    elif choice == "2":
        delete_cache_files("compiled_tcko")
    elif choice == "3":
        delete_cache_files("apc_cache", "tracking")
    elif choice == "4":
        delete_cache_files("tracking", "apc_cache", "result_cache", "compiled_tcko")
    else:
        print("Cancelled.")

# ========================================
# REQUIRE / VALIDATION
# ========================================
def require_file(path: Path, desc: str):
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"❌ Missing required {desc}: {path}")
    if not path.is_file():
        raise FileNotFoundError(f"❌ {desc} is not a file: {path}")
    return path

def require_dir(path: Path, desc: str):
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"❌ Missing required {desc}: {path}")
    if not path.is_dir():
        raise FileNotFoundError(f"❌ {desc} is not a directory: {path}")
    return path

# ========================================
# FILE TRACKING SYSTEM
# ========================================
def get_file_hash(file_path: Path):
    hash_md5 = hashlib.md5()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()

def load_tracking_data():
    if Path(TRACKING_FILE).exists():
        with open(TRACKING_FILE, 'r') as f:
            return json.load(f)
    return {}

def save_tracking_data(tracking_data):
    Path(TRACKING_FILE).parent.mkdir(parents=True, exist_ok=True)
    with open(TRACKING_FILE, 'w') as f:
        json.dump(tracking_data, f, indent=2)

def is_file_processed(file_path: Path, tracking_data: dict):
    key = str(file_path)
    if key not in tracking_data:
        return False
    return tracking_data[key].get('hash') == get_file_hash(file_path)

# ========================================
# BASE (PDP) TRACKING SYSTEM
# ========================================
def has_base_changed(base_file: Path, tracking_data: dict) -> bool:
    """
    Keep your original behavior: binary hash of the BASE Excel file.
    (If this flips too often due to metadata changes, switch to content-hash later.)
    """
    key = "base_file_hash"
    current = get_file_hash(base_file)
    return tracking_data.get(key) != current

def mark_base_processed(base_file: Path, tracking_data: dict):
    tracking_data["base_file_hash"] = get_file_hash(base_file)

# ========================================
# RESULT CACHE SYSTEM
# ========================================
def load_result_cache():
    if Path(RESULT_CACHE_FILE).exists():
        try:
            df = pd.read_pickle(RESULT_CACHE_FILE)
            print(f"\n📦 Loaded result cache: {len(df):,} rows")
            return df
        except Exception as e:
            print(f"\n⚠️ Result cache corrupted: {e}")
            print("🗑️  Deleting corrupted cache...")
            RESULT_CACHE_FILE.unlink(missing_ok=True)
    return None

def save_result_cache(df: pd.DataFrame):
    df.to_pickle(RESULT_CACHE_FILE)
    print(f"💾 Saved result cache: {len(df):,} rows")

# ========================================
# APC DATA CACHE SYSTEM
# ========================================
def load_apc_data_cache():
    if Path(APC_DATA_CACHE_FILE).exists():
        try:
            df = pd.read_pickle(APC_DATA_CACHE_FILE, compression='gzip')
            print(f"\n📦 Loaded APC data cache: {len(df):,} rows")
            return df
        except Exception as e:
            print(f"\n⚠️ APC cache corrupted: {e}")
            print("🗑️  Deleting corrupted cache...")
            APC_DATA_CACHE_FILE.unlink(missing_ok=True)
    return None

def save_apc_data_cache(df: pd.DataFrame):
    df.to_pickle(APC_DATA_CACHE_FILE, compression='gzip')
    print(f"💾 Saved APC data cache: {len(df):,} rows (compressed)")

# ========================================
# HELPERS
# ========================================
def get_level1_folders(folder_path):
    return [f for f in Path(folder_path).iterdir() if f.is_dir()]

def get_level2_folders(level1_folder):
    return [f for f in level1_folder.iterdir() if f.is_dir()]

def get_level3_folders(level2_folder):
    return [f for f in level2_folder.iterdir() if f.is_dir()]

def get_xlsx_files_safe(folder):
    return [f for f in folder.glob('*.xlsx') if not f.name.startswith('~$') and not f.name.startswith('.')]

def convert_column_types(df):
    datetime_columns = ['RUNSTART', 'Trans Time', '15MinKey']
    string_columns = ['Equipment', 'Lot']

    for col in string_columns:
        if col in df.columns:
            df[col] = df[col].astype("string").str.strip()

    for col in datetime_columns:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')

    for col in df.columns:
        if col not in datetime_columns + string_columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    return df

def finalize_pdp(df: pd.DataFrame) -> pd.DataFrame:
    df = df.dropna(subset=['Equipment', 'Trans Time', 'Lot']).copy()
    df['Equipment'] = df['Equipment'].astype("string").str.strip().str.upper()
    df['OneHourBefore'] = df['Trans Time'] - pd.Timedelta(hours=1)
    return df

# ========================================
# ATTRIBUTE MAPPING
# ========================================
ATTRIBUTE_MAPPING = {
    'BOTTOM_SURFACE_TEMPERATURE': 'BOTTOM_TEMPERATURE',
    'BOTTOM_HOUSING_TEMPERATURE': 'BOTTOM_TEMPERATURE',
    'BOT_HOUSING_TEMPERATURE_PROF': 'BOTTOM_TEMPERATURE',
    'TOP_SURFACE_TEMPERATURE': 'TOP_TEMPERATURE',
    'TOP_HOUSING_TEMPERATURE': 'TOP_TEMPERATURE',
    'TOP_HOUSING_TEMPERATURE_PROF': 'TOP_TEMPERATURE',
    'CLAMP_FORCE': 'CLAMP_FORCE',
    'CLAMP_FORCE_PROFILE': 'CLAMP_FORCE',
    'CURING_TIME': 'CURE_TIME',
    'CURETIME': 'CURE_TIME',
    'FILLING_TIME': 'TRANSFER_TIME',
    'TRANSFERTIME': 'TRANSFER_TIME',
    'INJECT_TIME': 'TRANSFER_TIME',
    'TF_PRESSURE': 'TRANSFER_PRESSURE',
    'TRANSFER_PRESSURE': 'TRANSFER_PRESSURE',
    'TRANSFER_PRESSURE_PROFILE': 'TRANSFER_PRESSURE',
    'PREHEATER_TEMPERATURE': 'PREHEATER_TEMPERATURE'
}

def standardize_attributes(df):
    if 'Attribute' in df.columns:
        df['Attribute'] = df['Attribute'].map(ATTRIBUTE_MAPPING).fillna(df['Attribute'])
    return df

# ========================================
# APC PROCESSING
# ========================================
def process_single_file(file, cols, value_vars):
    file_hash = get_file_hash(file)
    df = pd.read_excel(file, usecols=cols)
    df = convert_column_types(df)
    df["Equipment"] = df["Equipment"].astype("string").str.strip().str.upper()
    df["15MinKey"] = df["RUNSTART"].dt.floor("15min")
    df = df.drop("RUNSTART", axis=1)

    df_long = pd.melt(
        df,
        id_vars=["15MinKey", "Equipment"],
        value_vars=value_vars,
        var_name="Attribute",
        value_name="Value"
    )

    df_long = standardize_attributes(df_long)
    df_long.dropna(subset=["Value"], inplace=True)

    parts = df_long["Equipment"].str.split("_", n=1, expand=True)
    df_long["Machine"] = parts[0]
    df_long["Station"] = parts[1] if parts.shape[1] > 1 else pd.NA

    return df_long, file_hash

def process_apc_data_smart(folder_path, force_reprocess=False):
    """
    Returns: (apc_df, apc_changed)
      - apc_changed=True  -> new APC files were processed
      - apc_changed=False -> no new files; using cached APC (or None)
    """
    print(f"\n{'='*70}")
    print(f"🚀 STARTING APC DATA PROCESSING")
    print(f"{'='*70}")
    print(f"Folder: {folder_path}")
    print(f"Force reprocess: {force_reprocess}\n")

    log("APC: start processing")
    log_kv("APC: params", folder=str(folder_path), force_reprocess=force_reprocess)

    tracking_data = load_tracking_data()

    if force_reprocess:
        print("🔄 Force reprocess enabled - clearing tracking\n")
        tracking_data = {}

    cached_apc = load_apc_data_cache() if not force_reprocess else None

    log_kv("APC: cache", cached_apc_rows=(len(cached_apc) if cached_apc is not None else 0))
    log_kv("APC: tracking", tracked_files=len(tracking_data))

    MACHINE_A_e_cols = ["RUNSTART", "Equipment", "CURING_TIME", "INJECT_TIME", "PREHEATER_TEMPERATURE"]
    MACHINE_A_t_cols = ["RUNSTART", "Equipment", "BOT_HOUSING_TEMPERATURE_PROF", "TOP_HOUSING_TEMPERATURE_PROF", "CLAMP_FORCE_PROFILE", "TRANSFER_PRESSURE_PROFILE"]
    MACHINE_B_cols = ["RUNSTART", "Equipment", "BOTTOM_SURFACE_TEMPERATURE", "TOP_SURFACE_TEMPERATURE", "CLAMP_FORCE", "CURING_TIME", "FILLING_TIME", "TF_PRESSURE", "PREHEATER_TEMPERATURE"]
    MACHINE_C_cols = ["RUNSTART", "Equipment", "BOTTOM_HOUSING_TEMPERATURE", "TOP_HOUSING_TEMPERATURE", "CLAMP_FORCE", "CURETIME", "TRANSFERTIME", "TRANSFER_PRESSURE", "PREHEATER_TEMPERATURE"]

    all_data = []
    stats = {'total_files': 0, 'new_files': 0, 'skipped_files': 0, 'error_files': 0}

    for level1_folder in get_level1_folders(folder_path):
        print(f"\n{'='*70}")
        print(f"📁 LEVEL 1: {level1_folder.name}")
        print(f"{'='*70}")

        if level1_folder.name == 'MACHINE_A':
            for level2_folder in get_level2_folders(level1_folder):
                print(f"\n  📁 Level 2: {level2_folder.name}")
                for level3_folder in get_level3_folders(level2_folder):
                    print(f"    📁 Level 3: {level3_folder.name}")

                    xlsx_files = get_xlsx_files_safe(level3_folder)
                    stats['total_files'] += len(xlsx_files)
                    if not xlsx_files:
                        continue

                    if level3_folder.name.strip().upper() == "E":
                        cols = MACHINE_A_e_cols
                        value_vars = ["CURING_TIME", "INJECT_TIME", "PREHEATER_TEMPERATURE"]
                    else:
                        cols = MACHINE_A_t_cols
                        value_vars = ["BOT_HOUSING_TEMPERATURE_PROF", "TOP_HOUSING_TEMPERATURE_PROF",
                                     "CLAMP_FORCE_PROFILE", "TRANSFER_PRESSURE_PROFILE"]

                    for file in xlsx_files:
                        if not force_reprocess and is_file_processed(file, tracking_data):
                            print(f"      ⊙ Skipped: {file.name}")
                            stats['skipped_files'] += 1
                            continue
                        try:
                            df_long, file_hash = process_single_file(file, cols, value_vars)
                            all_data.append(df_long)
                            tracking_data[str(file)] = {
                                'hash': file_hash,
                                'processed_at': datetime.now().isoformat(),
                                'size': file.stat().st_size
                            }
                            stats['new_files'] += 1
                            print(f"      ✓ {file.name} ({len(df_long):,} rows)")
                        except Exception as e:
                            print(f"      ✗ Error: {file.name} - {e}")
                            stats['error_files'] += 1

        elif level1_folder.name == 'MACHINE_B':
            for level2_folder in get_level2_folders(level1_folder):
                print(f"\n  📁 Level 2: {level2_folder.name}")

                xlsx_files = get_xlsx_files_safe(level2_folder)
                stats['total_files'] += len(xlsx_files)
                if not xlsx_files:
                    continue

                cols = MACHINE_B_cols
                value_vars = ["BOTTOM_SURFACE_TEMPERATURE", "TOP_SURFACE_TEMPERATURE",
                             "CLAMP_FORCE", "CURING_TIME", "FILLING_TIME",
                             "TF_PRESSURE", "PREHEATER_TEMPERATURE"]

                for file in xlsx_files:
                    if not force_reprocess and is_file_processed(file, tracking_data):
                        print(f"      ⊙ Skipped: {file.name}")
                        stats['skipped_files'] += 1
                        continue
                    try:
                        df_long, file_hash = process_single_file(file, cols, value_vars)
                        all_data.append(df_long)
                        tracking_data[str(file)] = {
                            'hash': file_hash,
                            'processed_at': datetime.now().isoformat(),
                            'size': file.stat().st_size
                        }
                        stats['new_files'] += 1
                        print(f"    ✓ {file.name} ({len(df_long):,} rows)")
                    except Exception as e:
                        print(f"    ✗ Error: {file.name} - {e}")
                        stats['error_files'] += 1

        elif level1_folder.name == 'MACHINE_C':
            for level2_folder in get_level2_folders(level1_folder):
                print(f"\n  📁 Level 2: {level2_folder.name}")

                xlsx_files = get_xlsx_files_safe(level2_folder)
                stats['total_files'] += len(xlsx_files)
                if not xlsx_files:
                    continue

                cols = MACHINE_C_cols
                value_vars = ["BOTTOM_HOUSING_TEMPERATURE", "TOP_HOUSING_TEMPERATURE",
                             "CLAMP_FORCE", "CURETIME", "TRANSFERTIME",
                             "TRANSFER_PRESSURE", "PREHEATER_TEMPERATURE"]

                for file in xlsx_files:
                    if not force_reprocess and is_file_processed(file, tracking_data):
                        print(f"      ⊙ Skipped: {file.name}")
                        stats['skipped_files'] += 1
                        continue
                    try:
                        df_long, file_hash = process_single_file(file, cols, value_vars)
                        all_data.append(df_long)
                        tracking_data[str(file)] = {
                            'hash': file_hash,
                            'processed_at': datetime.now().isoformat(),
                            'size': file.stat().st_size
                        }
                        stats['new_files'] += 1
                        print(f"    ✓ {file.name} ({len(df_long):,} rows)")
                    except Exception as e:
                        print(f"    ✗ Error: {file.name} - {e}")
                        stats['error_files'] += 1

        else:
            print(f"  ⊗ Skipping unknown folder: {level1_folder.name}")

    save_tracking_data(tracking_data)

    if all_data:
        FinalAPC = pd.concat(all_data, ignore_index=True)

        if cached_apc is not None:
            FinalAPC = pd.concat([cached_apc, FinalAPC], ignore_index=True)
            FinalAPC = FinalAPC.drop_duplicates(subset=['Equipment', '15MinKey', 'Attribute'], keep='last')

        save_apc_data_cache(FinalAPC)

        print(f"\n{'='*70}")
        print("📊 APC PROCESSING SUMMARY")
        print(f"{'='*70}")
        print(f"Total files found: {stats['total_files']}")
        print(f"New files processed: {stats['new_files']}")
        print(f"Skipped: {stats['skipped_files']}")
        print(f"Errors: {stats['error_files']}")
        print(f"Final APC shape: {FinalAPC.shape}")

        log_kv("APC: done (NEW DATA)", total_files=stats['total_files'], new_files=stats['new_files'],
            skipped=stats['skipped_files'], errors=stats['error_files'],
            final_rows=len(FinalAPC))

        return FinalAPC, True

    print("\n⚠️  No new APC data to process!")
    if cached_apc is not None:
        print("✓ Using cached APC data")
        log("APC: done (NO NEW FILES) -> using cached APC")
        log_kv("APC: cached", rows=len(cached_apc))
        return cached_apc, False

    return None, False

# ========================================
# PDP + MERGE
# ========================================
def process_pdp_and_merge(apc_data, apc_changed, use_cache=True):
    print(f"\n{'='*70}")
    print("🔗 MERGING WITH PDP DATA")
    log("MERGE: start")
    log_kv("MERGE: inputs",
       apc_rows=(len(apc_data) if apc_data is not None else 0),
       apc_changed=apc_changed,
       use_cache=use_cache)
    print(f"{'='*70}")

    tracking_data = load_tracking_data()
    base_changed = has_base_changed(BASE_FILE, tracking_data)

    # If compiled cache doesn't exist, we must rebuild once
    if not COMPILED_TCKO_CACHE.exists():
        print("⚠️ Compiled TCKO cache missing - forcing rebuild once.")
        log("MERGE: compiled cache missing -> forcing base_changed=True (rebuild once)")
        base_changed = True

    log_kv("MERGE: base/compiled status",
       base_file=str(BASE_FILE),
       base_changed=base_changed,
       compiled_exists=COMPILED_TCKO_CACHE.exists())

    cached_result = None
    if use_cache and (not base_changed):
        cached_result = load_result_cache()

    log_kv("MERGE: cached_result",
        loaded=(cached_result is not None),
        rows=(len(cached_result) if cached_result is not None else 0))

    # ✅ THIS IS THE SKIP-MERGE LOGIC
    if (not apc_changed) and (not base_changed) and (cached_result is not None):
        print("✓ No new APC and BASE unchanged - using cached result")
        log("MERGE: SKIP (no new APC + base unchanged) -> returning cached result")
        return cached_result

    # If BASE changed and APC not provided, load APC cache for full re-merge
    if base_changed and apc_data is None:
        #print("🔄 BASE changed; loading APC from cache for full re-merge...")
        log("🔄 BASE changed; loading APC from cache for full re-merge...")
        apc_data = load_apc_data_cache()
        if apc_data is None:
            print("❌ No APC cache found - must force reprocess")
            log("❌ APC cache missing -> cannot re-merge (run force_reprocess to rebuild)")
            return None

    if apc_data is None:
        print("⚠️ No APC data available")
        return None

    # Build / load PDP
    if base_changed:
        print("\n📋 BASE FILE changed - rebuilding compiled TCKO...")
        log("PDP: rebuilding compiled TCKO (BASE changed)")
        log_kv("PDP: sources", tcko=str(TCKO_BASE), base=str(BASE_FILE), ddm=str(DDM_CACHE))

        if not DDM_CACHE.exists():
            print(f"❌ Required baseline DDM cache missing: {DDM_CACHE}")
            return None

        # Load TCKO
        PDP = pd.read_excel(
            TCKO_BASE,
            header=1,
            usecols=["Lot Number", "Transaction Timestamp", "Equipment Name"]
        ).rename(columns={
            "Lot Number": "Lot",
            "Transaction Timestamp": "Trans Time",
            "Equipment Name": "Equipment"
        })
        PDP = convert_column_types(PDP)

        # Filter by BASE list
        lot_list = pd.read_excel(BASE_FILE, usecols=['LotName'])['LotName']
        PDP = PDP[PDP['Lot'].isin(lot_list)].reset_index(drop=True)

        log_kv("PDP: after BASE filter", pdp_rows=len(PDP))

        # Combine with baseline DDM and write compiled
        DDM = pd.read_pickle(DDM_CACHE, compression='gzip')
        Compiled_TCKO = pd.concat([DDM, PDP], ignore_index=True).drop_duplicates(
            subset=['Lot', 'Trans Time', 'Equipment'], keep='last'
        )
        Compiled_TCKO.to_pickle(COMPILED_TCKO_CACHE, compression='gzip')
        print(f"✓ Saved compiled TCKO cache: {COMPILED_TCKO_CACHE.name}")

        log_kv("PDP: compiled", compiled_rows=len(Compiled_TCKO), save_to=str(COMPILED_TCKO_CACHE))

        PDP = finalize_pdp(Compiled_TCKO)
    else:
        print("\n📋 BASE unchanged - loading compiled TCKO cache")
        log("PDP: BASE unchanged -> loading compiled TCKO cache")
        log_kv("PDP: compiled load", path=str(COMPILED_TCKO_CACHE))
        PDP = finalize_pdp(pd.read_pickle(COMPILED_TCKO_CACHE, compression='gzip'))
        log_kv("PDP: ready", rows=len(PDP), time_min=str(PDP['OneHourBefore'].min()), time_max=str(PDP['Trans Time'].max()))

    log_kv("PDP: unique lots", unique_lots=int(PDP['Lot'].nunique()))

    # Merge window
    pdp_min_time = PDP['OneHourBefore'].min()
    pdp_max_time = PDP['Trans Time'].max()

    apc_filtered = apc_data[
        (apc_data['15MinKey'] >= pdp_min_time) &
        (apc_data['15MinKey'] <= pdp_max_time)
    ]

    log_kv("MERGE: APC time filter",
       before=len(apc_data),
       after=len(apc_filtered),
       pdp_min=str(pdp_min_time),
       pdp_max=str(pdp_max_time))

    all_results = []
    unique_equipment = PDP['Equipment'].unique()
    log_kv("MERGE: equipment list", count=len(unique_equipment))

    for idx, equipment in enumerate(unique_equipment, 1):
        pdp_subset = PDP[PDP['Equipment'] == equipment]
        apc_subset = apc_filtered[apc_filtered['Machine'] == equipment]
        if apc_subset.empty:
            continue

        merged = pd.merge(pdp_subset, apc_subset, left_on='Equipment', right_on='Machine', how='left')

        merged = merged.rename(columns={
            "Equipment_x": "PDP_Equipment",
            "Equipment_y": "Equipment"
        })

        filtered = merged[
            (merged['15MinKey'] >= merged['OneHourBefore']) &
            (merged['15MinKey'] <= merged['Trans Time'])
        ].copy()

        all_results.append(filtered)

    if idx % 5 == 0 or idx == len(unique_equipment):
        log_kv("MERGE: progress", done=f"{idx}/{len(unique_equipment)}", equipment=equipment, added_rows=len(filtered))

    if not all_results:
        print("⚠️ No APC rows matched any PDP equipment/time window.")
        return cached_result

    result_new = pd.concat(all_results, ignore_index=True)
    log_kv("MERGE: concatenated", rows=len(result_new), cols=len(result_new.columns))
    if 'Equipment' not in result_new.columns:
        log("❌ MERGE: 'Equipment' column missing after merge. Columns are:")
        print(list(result_new.columns))
        return cached_result

    result_new_grouped = result_new.groupby(['Lot', 'Equipment', 'Attribute']).agg(
        Value=('Value', 'mean'),
        TransTime=('Trans Time', 'first'),
        OneHourBefore=('OneHourBefore', 'first')
    ).reset_index()

    log_kv("MERGE: grouped", rows=len(result_new_grouped))

    # Combine with cache if BASE unchanged
    if (not base_changed) and (cached_result is not None):
        final_result = pd.concat([cached_result, result_new_grouped], ignore_index=True)
        final_result = final_result.sort_values('TransTime')
        final_result = final_result.drop_duplicates(subset=['Lot', 'Equipment', 'Attribute'], keep='last')
    else:
        final_result = result_new_grouped

    # Mark BASE hash processed
    mark_base_processed(BASE_FILE, tracking_data)
    save_tracking_data(tracking_data)

    save_result_cache(final_result)
    return final_result

# ========================================
# MAIN EXECUTION
# ========================================
if __name__ == "__main__":
    try:
        print("\n" + "="*70)
        print("🚀 APC DATA PROCESSING PIPELINE")
        print("="*70)
        print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Tracking file: {TRACKING_FILE}")
        print(f"Result cache: {RESULT_CACHE_FILE}")
        print(f"APC cache: {APC_DATA_CACHE_FILE}")
        print(f"DDM cache: {DDM_CACHE}")
        print(f"Compiled TCKO cache: {COMPILED_TCKO_CACHE}")
        print(f"Output file: {OUTPUT_FILE}")
        print("="*70 + "\n")

        folder_path = NETWORK_BASE / 'APC Data'
        force_reprocess = ask_yes_no("Force reprocess all APC files?", default=False)

        # Validate required dirs/files once
        require_dir(NETWORK_BASE, "NETWORK_BASE folder")
        require_dir(NETWORK_BASE / 'APC Data', "APC Data folder")
        require_file(BASE_FILE, "BASE_FILE (lot whitelist)")
        require_file(TCKO_BASE, "TCKO_BASE (QFP+ lot list)")
        require_file(DDM_CACHE, "DDM baseline cache (read-only)")

        if ask_yes_no("Open cache reset menu?", default=False):
            interactive_cache_reset_menu()
            print()

        apc_data, apc_changed = process_apc_data_smart(folder_path, force_reprocess=force_reprocess)
        log_kv("MAIN: APC result", apc_changed=apc_changed, apc_rows=(len(apc_data) if apc_data is not None else 0))

        result = process_pdp_and_merge(apc_data, apc_changed, use_cache=True)
        print(f"Unique Lots in result: {result['Lot'].nunique():,}")
        log_kv("MAIN: merge result", produced=(result is not None), rows=(len(result) if result is not None else 0))

        if result is None:
            raise RuntimeError("No result produced.")

        if OUTPUT_FILE.exists():
            should_backup = ask_yes_no(
                f"\n'{OUTPUT_FILE.name}' already exists. Create backup before overwriting?",
                default=True
            )
            if should_backup:
                backup_output_file(OUTPUT_FILE)

        print(f"\n💾 Attempting to save to: {OUTPUT_FILE}")
        OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
        result.to_excel(OUTPUT_FILE, index=False)
        print(f"✓ Saved to: {OUTPUT_FILE}")
        print(f"✓ Output rows: {len(result):,}")

        print(f"\n{'='*70}")
        print("✅ PIPELINE COMPLETED")
        print(f"{'='*70}")

    except Exception:
        import traceback
        print("\n" + "="*70)
        print("❌ PIPELINE CRASHED")
        print("="*70)
        traceback.print_exc()

    finally:
        input("\n👉 Press ENTER to close this window...")