import pandas as pd
import os
import glob

# Define the base directory containing the Excel files
base_dir = "../data/reports/"

# Define the directory to store the merged Parquet files
merged_folder = os.path.join(base_dir, "merged_2023")
os.makedirs(merged_folder, exist_ok=True)  # Create the folder if it doesn't exist

# Use glob to find all Excel files (.xls and .xlsx) in the folder
excel_files = glob.glob(os.path.join(base_dir, "*.xls*"))

# Process each Excel file in the folder
for excel_file in excel_files:
    print(f"Processing file: {excel_file}")
    
    # Read all sheets from the current Excel file into a dictionary of DataFrames
    try:
        sheets_dict = pd.read_excel(excel_file, sheet_name=None)
    except Exception as e:
        print(f"Error reading {excel_file}: {e}")
        continue

    # Merge all non-empty sheets into a single DataFrame
    df_list = []
    for sheet_name, df in sheets_dict.items():
        if not df.empty:
            df["Sheet_Name"] = sheet_name  # Optionally add the sheet name as a column
            df_list.append(df)

    if df_list:
        merged_df = pd.concat(df_list, ignore_index=True)
        
        # Convert all columns to suitable types for Parquet format
        for col in merged_df.columns:
            if merged_df[col].dtype == 'object':  # Convert object columns to string
                merged_df[col] = merged_df[col].astype(str)
            elif pd.api.types.is_categorical_dtype(merged_df[col]):  # Convert categorical to string
                merged_df[col] = merged_df[col].astype(str)
            elif pd.api.types.is_bool_dtype(merged_df[col]):  # Ensure boolean type
                merged_df[col] = merged_df[col].astype(bool)
            elif pd.api.types.is_numeric_dtype(merged_df[col]):  # Ensure numeric consistency
                merged_df[col] = pd.to_numeric(merged_df[col], errors='coerce')
            elif pd.api.types.is_datetime64_any_dtype(merged_df[col]):  # Ensure datetime format
                merged_df[col] = pd.to_datetime(merged_df[col])

        # Generate a Parquet filename based on the Excel file name
        base_filename = os.path.splitext(os.path.basename(excel_file))[0]
        parquet_filename = f"{base_filename}.parquet"
        parquet_path = os.path.join(merged_folder, parquet_filename)

        # Save the merged DataFrame as a Parquet file
        try:
            merged_df.to_parquet(parquet_path, engine="pyarrow", index=False)
            print(f"Merged data saved as Parquet: {parquet_path}\n")
        except Exception as e:
            print(f"Error saving Parquet file {parquet_path}: {e}\n")
    else:
        print(f"No valid data found in sheets for {excel_file}\n")
