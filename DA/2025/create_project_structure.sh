#!/bin/bash
# --- Створення структури проєкту Global Mobile Reviews Analytics ---

mkdir -p global_mobile_reviews_project/{data/{raw,processed},notebooks,sql,tableau/{dashboard_screenshots},reports}

# Порожні файли для зручності
touch global_mobile_reviews_project/{README.md,requirements.txt,.gitignore}
touch global_mobile_reviews_project/data/README.md
touch global_mobile_reviews_project/sql/{create_tables.sql,analysis_queries.sql,views_for_tableau.sql,test_queries.sql}
touch global_mobile_reviews_project/notebooks/{01_data_cleaning.ipynb,02_eda_analysis.ipynb,03_sql_export.ipynb,04_final_summary.ipynb}
touch global_mobile_reviews_project/tableau/tableau_dashboard_link.txt
touch global_mobile_reviews_project/reports/summary_report.pdf

echo "✅ Проєктна структура створена: global_mobile_reviews_project/"
