import pandas as pd
import os
import numpy as np
from datetime import datetime

class MigrationProcessor:
    def __init__(self, config):
        self.config = config
        self.primary_file = os.path.join(config.UPLOAD_FOLDER, config.PRIMARY_FILE)
        self.secondary_file = os.path.join(config.UPLOAD_FOLDER, config.SECONDARY_FILE)
        self.template_file = os.path.join(config.UPLOAD_FOLDER, config.TEMPLATE_FILE)
        self.result_file = os.path.join(config.DOWNLOAD_FOLDER, config.RESULT_FILE)
        
        # Ensure download directory exists
        os.makedirs(config.DOWNLOAD_FOLDER, exist_ok=True)
        
    def process(self):
        """Execute the migration process"""
        # Load the source data
        primary_df = pd.read_excel(self.primary_file)
        secondary_df = pd.read_excel(self.secondary_file)
        template_df = pd.read_excel(self.template_file, sheet_name='Sheet')
        
        # Create a new DataFrame for the result
        result_df = pd.DataFrame()
        
        # Step 1: Map direct columns from primary source
        for template_col, source_col in self.config.COLUMN_MAPPINGS.items():
            if source_col in primary_df.columns:
                # Add the template column
                result_df[template_col] = primary_df[source_col]
                # Add the original column next to it
                result_df[source_col] = primary_df[source_col]
        
        # Step 2: Add constant values
        for col, value in self.config.CONSTANT_VALUES.items():
            result_df[col] = value
        
        # Step 3: Apply correspondence via GCPJ
        # Create a mapping dictionary from secondary source
        gcpj_mapping = {}
        
        for _, row in secondary_df.iterrows():
            if 'GCPJ' in row and pd.notna(row['GCPJ']):
                gcpj = str(row['GCPJ'])
                gcpj_mapping[gcpj] = {
                    col: row[src_col] for col, src_col in self.config.SECONDARY_MAPPINGS.items()
                    if src_col in row and pd.notna(row[src_col])
                }
        
        # Apply the mapping to the result
        for template_col, source_col in self.config.SECONDARY_MAPPINGS.items():
            # Initialize the columns
            result_df[template_col] = np.nan
            result_df[source_col] = np.nan
            
        # Fill in the values from the mapping
        for idx, row in result_df.iterrows():
            if 'GCPJ' in row and pd.notna(row['GCPJ']):
                gcpj = str(row['GCPJ'])
                if gcpj in gcpj_mapping:
                    for template_col, source_col in self.config.SECONDARY_MAPPINGS.items():
                        if template_col in gcpj_mapping[gcpj]:
                            result_df.at[idx, template_col] = gcpj_mapping[gcpj][template_col]
                            result_df.at[idx, source_col] = gcpj_mapping[gcpj][template_col]
        
        # Step 4: Generate statistics
        stats = self.generate_statistics(result_df)
        
        # Step 5: Save the result
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        result_filename = f"migration_result_{timestamp}.xlsx"
        result_filepath = os.path.join(self.config.DOWNLOAD_FOLDER, result_filename)
        
        result_df.to_excel(result_filepath, index=False)
        
        return {
            'filename': result_filename,
            'filepath': result_filepath,
            'stats': stats
        }
    
    def generate_statistics(self, df):
        """Generate statistics about the migration process"""
        total_rows = len(df)
        
        # Column completion statistics
        column_stats = {}
        
        for col in df.columns:
            if col in self.config.COLUMN_MAPPINGS.values() or col in self.config.SECONDARY_MAPPINGS.values():
                # This is an original column, skip
                continue
                
            filled = df[col].notna().sum()
            percentage = (filled / total_rows) * 100 if total_rows > 0 else 0
            
            column_stats[col] = {
                'total': total_rows,
                'filled': int(filled),
                'percentage': round(percentage, 2)
            }
        
        # Overall statistics
        fully_filled_columns = sum(1 for stats in column_stats.values() if stats['percentage'] == 100)
        partially_filled_columns = sum(1 for stats in column_stats.values() if 0 < stats['percentage'] < 100)
        empty_columns = sum(1 for stats in column_stats.values() if stats['percentage'] == 0)
        
        return {
            'total_rows': total_rows,
            'column_stats': column_stats,
            'fully_filled_columns': fully_filled_columns,
            'partially_filled_columns': partially_filled_columns,
            'empty_columns': empty_columns
        }
