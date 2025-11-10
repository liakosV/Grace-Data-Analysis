# Grace Data Analysis

## Overview
Grace Data Analysis is a Python-based utility that processes two datasets — one static Excel file (real data) and one variable CSV file (GRACE data) — to compute averages, anomalies, and correlation coefficients for each dataset group.
It automatically exports the analysis results into a formatted Excel workbook with multiple sheets.

The project can be run directly as a Python script or as a stand-alone Windows .exe (built with PyInstaller).

## About
This project was originally developed for my brother, an Environmental Engineering student, to assist with analyzing GRACE satellite data and comparing it with real-world measurements.

## Features
* Reads one fixed Excel dataset (RealData.xlsx) and one user-selected CSV file

* Merges data on matching name and date columns

* Filters results for a specific date range (e.g., 2009–2014)

* Calculates:

    * Average values (first_x, first_y)

   * Anomalies (first_x - avg_x, first_y - avg_y)

   * Correlation and correlation category (−1, 0, 1)

* Exports all results into:

   * Merged_Data sheet

   * Averages sheet

   * Correlations sheet

* Dynamically names the output file (e.g., analysis_results_6ML.xlsx)

## Requirements
If you're running the Python script (not the `.exe`), install:
```
pip install pandas xlsxwriter openpyxl
```

## Usage (Python script)
1. Clone the repository:
   ```
   git clone https://github.com/liakosV/Grace-Data-Analysis.git
   cd GraceDataAnalysis
   ```
2. Run the script:
   ```
   python .\grace_data_calculator.py
   ```

## Usage (Executable)
1. Building the executable
   ```
   pyinstaller --onefile .\grace_data_calculator.py
   ```
2. Double click it or run it from CMD:
   ```
   cd dist
   .\grace_data_calculator.exe
   ```



