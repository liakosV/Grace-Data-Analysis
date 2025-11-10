import pandas as pd
import os
import re

def main():

    # Reads user input paths
    real_path = input("Please enter the path of the real data excel (e.g. C:/Users/User/Downloads/RealData.xlsx): ")
    csv_path = input("Please enter the path of the grace data excel (e.g. C:/Users/User/Downloads/GRACE_RA_Fix_6ML.csv): ")

    # Read the files
    real_data = pd.read_excel(real_path)
    csv_data = pd.read_csv(csv_path)

    # Prepare column names
    real_data = real_data.rename(columns={'Codice' : 'name', 'Date': 'date', 'x' : 'first'})
    real_selected = real_data[['name', 'date', 'first']]
    csv_selected = csv_data[['name', 'date', 'first']]

    # Group by name and date, and averaging duplicates
    real_selected = real_selected.groupby(['name', 'date'], as_index=False)['first'].mean()
    csv_selected = csv_selected.groupby(['name', 'date'], as_index=False)['first'].mean()

    # Merge
    merged = pd.merge(real_selected, csv_selected, on=['name', 'date'], how='inner', suffixes=('_x', '_y'))

    # Date conversion and filtering
    merged['date'] = pd.to_datetime(merged['date'], errors='coerce',)
    merged_range = merged[
        (merged['date'].dt.year >= 2009) &
        (merged['date'].dt.year <= 2014)
    ]

    # Averages
    avg = merged_range.groupby('name')[['first_x', 'first_y']].mean().rename(columns={'first_x' : 'avg_x', 'first_y' : 'avg_y'}).reset_index()

    # Merge with averages and compute anomalies
    merged = pd.merge(merged, avg, on='name', how='left')
    merged['anomaly_x'] = merged['first_x'] - merged['avg_x']
    merged['anomaly_y'] = merged['first_y'] - merged['avg_y']

    # Correlations
    correlations = (
        merged.groupby('name')[['anomaly_x', 'anomaly_y']]
        .apply(lambda g: g['anomaly_x'].corr(g['anomaly_y']))
        .reset_index(name='correlation')
    )

    def categorize_corr(r):
        if r > 0.5:
            return 1
        elif r < -0.5:
            return -1
        else:
            return 0

    correlations['corr_category'] = correlations['correlation'].apply(categorize_corr)

    # Format date for output
    merged['date'] = merged['date'].dt.strftime('%Y-%m')

    # Build output filename
    csv_filename = os.path.basename(csv_path)
    match = re.search(r'_([A-Za-z0-9]+)\.csv$', csv_filename)
    suffix = match.group(1) if match else 'Unknown'

    output_path = f'analysis_results_{suffix}.xlsx'

    # Export results in xlsx file
    with pd.ExcelWriter(output_path) as writer:
        merged[['name', 'date', 'first_x', 'first_y', 'anomaly_x', 'anomaly_y']].to_excel(writer, sheet_name="Merged_Data", index=False)
        avg.to_excel(writer, sheet_name="Averages", index=False)
        correlations.to_excel(writer, sheet_name="Correlations", index=False)

    print(f"All data exported successfully to {output_path}")

if __name__ == "__main__":
    main()












