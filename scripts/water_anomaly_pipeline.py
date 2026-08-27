"""
Environmental Intelligence Pipeline - Ingestion & Anomaly Isolation Engine
Target Dataset: UK Environment Agency Water Quality Archive (Modern API Layout)
"""

import os
import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
from reportlab.lib.pagesizes import letter
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.enums import TA_LEFT
from reportlab.lib import colors

PROJECT_ROOT = r"C:\Users\markc\Documents\Data Science\PROJECT 1 - EA WATER ANOMALIES"
RAW_DATA_PATH = os.path.join(PROJECT_ROOT, "midlands_water_raw.csv")
DB_PATH = os.path.join(PROJECT_ROOT, "midlands_water.db")

AMMONIA_MAX_ALLOWED = 0.5  
BOD_MAX_ALLOWED = 50.0      

def validate_environment():
    for subfolder in ['scripts', 'outputs', 'documentation']:
        path = os.path.join(PROJECT_ROOT, subfolder)
        if not os.path.exists(path):
            os.makedirs(path)
            
    if not os.path.exists(RAW_DATA_PATH):
        raise FileNotFoundError(f"CRITICAL: Raw source file missing at {RAW_DATA_PATH}.")

def run_etl():
    validate_environment()
    print("[*] Launching ETL Core Engine...")
    
    target_columns = [
        'id', 'phenomenonTime', 'determinand.prefLabel', 'result', 
        'samplingPoint.longitude', 'samplingPoint.latitude', 
        'samplingPoint.prefLabel', 'unit.label'
    ]
    
    try:
        df = pd.read_csv(RAW_DATA_PATH, usecols=lambda c: c in target_columns, low_memory=False)
    except Exception as e:
        print(f"[-] Data Ingestion Failed: {str(e)}")
        return

    df['phenomenonTime'] = pd.to_datetime(df['phenomenonTime'], errors='coerce')
    df['result'] = pd.to_numeric(df['result'], errors='coerce')
    
    df = df.dropna(subset=['phenomenonTime', 'result', 'samplingPoint.longitude', 'samplingPoint.latitude'])
    print(f"[+] Cleaned records. Remaining records: {len(df)}")

    filtered_df = df[df['determinand.prefLabel'].str.contains('Ammoniacal|Oxygen Demand|BOD|COD', case=False, na=False, regex=True)].copy()

    is_ammonia_spike = (filtered_df['determinand.prefLabel'].str.contains('Ammoniacal', case=False)) & (filtered_df['result'] >= 50.0)
    is_bod_spike = (filtered_df['determinand.prefLabel'].str.contains('Oxygen|BOD|COD', case=False)) & (filtered_df['result'] >= 800.0)
    
    anomalies = filtered_df[is_ammonia_spike | is_bod_spike].copy()
    print(f"[+] Isolated {len(anomalies)} priority contamination anomalies.")

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("DROP TABLE IF EXISTS water_anomalies")
    cursor.execute("""
        CREATE TABLE water_anomalies (
            id TEXT PRIMARY KEY,
            phenomenonTime TEXT,
            determinand_prefLabel TEXT,
            result REAL,
            longitude REAL,
            latitude REAL,
            facility_name TEXT,
            unit TEXT
        )
    """)
    
    db_ready_df = anomalies.rename(columns={
        'determinand.prefLabel': 'determinand_prefLabel',
        'samplingPoint.longitude': 'longitude',
        'samplingPoint.latitude': 'latitude',
        'samplingPoint.prefLabel': 'facility_name',
        'unit.label': 'unit'
    })
    
    if 'unit' not in db_ready_df.columns:
        db_ready_df['unit'] = 'MILLIGRAM PER LITRE'
    else:
        db_ready_df['unit'] = db_ready_df['unit'].fillna('MILLIGRAM PER LITRE')
        
    if 'facility_name' not in db_ready_df.columns:
        db_ready_df['facility_name'] = 'UNKNOWN REGIONAL FACILITY CATCHMENT'

    db_ready_df.to_sql('water_anomalies', conn, if_exists='append', index=False)
    cursor.execute("CREATE INDEX idx_coords ON water_anomalies(longitude, latitude)")
    conn.commit()
    conn.close()
    print(f"[+] Database Writing Complete: {DB_PATH}")

def check_results():
    conn = sqlite3.connect(DB_PATH)
    query = "SELECT * FROM water_anomalies ORDER BY result DESC LIMIT 5"
    top_anomalies = pd.read_sql_query(query, conn)
    conn.close()
    return top_anomalies

def map_anomalies():
    try:
        conn = sqlite3.connect(DB_PATH)
        df = pd.read_sql_query("SELECT longitude, latitude FROM water_anomalies", conn)
        conn.close()
        if df.empty: return
        plt.figure(figsize=(10, 6))
        plt.scatter(df['longitude'], df['latitude'], alpha=0.6, s=25, color='red', edgecolors='darkred', linewidths=0.5)
        plt.title("Midlands Water Anomaly Clusters", fontsize=12, fontweight='bold')
        plt.grid(True, linestyle='--', alpha=0.5)
        output_image = os.path.join(PROJECT_ROOT, "outputs", "midlands_anomaly_map.png")
        plt.savefig(output_image, dpi=150)
        plt.close()
        print(f"[+] Spatial map image asset saved to: {output_image}")
    except Exception as err:
        print(f"[-] Mapping Error: {str(err)}")

def generate_pdf_documentation():
    text_report_path = os.path.join(PROJECT_ROOT, "outputs", "Critical_Pollution_Audit.txt")
    pdf_report_path = os.path.join(PROJECT_ROOT, "documentation", "Water_Pipeline_Documentation.pdf")
    print("[*] Querying database for live incident log exports...")
    conn = sqlite3.connect(DB_PATH)
    report_df = pd.read_sql_query("SELECT * FROM water_anomalies ORDER BY result DESC", conn)
    conn.close()
    if report_df.empty:
        print("[!] Warning: No records matched. Reports bypassed.")
        return

    with open(text_report_path, 'w', encoding='utf-8') as f:
        f.write("====================================================\n")
        f.write("      OFFICIAL REGIONAL WATER QUALITY AUDIT         \n")
        f.write("====================================================\n")
        for _, row in report_df.iterrows():
            raw_time = str(row['phenomenonTime'])
            clean_time = raw_time.replace(" ", "T") if " " in raw_time else raw_time
            f.write(f"FACILITY: {str(row['facility_name']).upper()}\n")
            f.write(f"SUBSTANCE: {row['determinand_prefLabel']}\n")
            f.write(f"READING: {row['result']} {str(row['unit']).upper()}\n")
            f.write(f"COORDINATES: {row['latitude']}, {row['longitude']}\n")
            f.write(f"TIMESTAMP: {clean_time}\n")
            f.write("-" * 50 + "\n")
    print(f"[+] Success: Generated the raw text audit ledger at: {text_report_path}")

    doc = SimpleDocTemplate(pdf_report_path, pagesize=letter, rightMargin=45, leftMargin=45, topMargin=45, bottomMargin=45)
    styles = getSampleStyleSheet()
    title_style = ParagraphStyle('AuditTitle', parent=styles['Normal'], fontName='Helvetica-Bold', fontSize=22, leading=26, textColor=colors.HexColor('#1A365D'), spaceAfter=4)
    subtitle_style = ParagraphStyle('AuditSub', parent=styles['Normal'], fontName='Helvetica', fontSize=10, leading=14, textColor=colors.HexColor('#4A5568'), spaceAfter=15)
    facility_style = ParagraphStyle('FacName', parent=styles['Normal'], fontName='Helvetica-Bold', fontSize=10, leading=13, textColor=colors.HexColor('#2B6CB0'), spaceBefore=8, keepWithNext=True)
    metric_style = ParagraphStyle('MetricData', parent=styles['Normal'], fontName='Helvetica', fontSize=9, leading=13, textColor=colors.HexColor('#2D3748'), leftIndent=12, spaceAfter=4)
    divider_style = ParagraphStyle('DivLine', parent=styles['Normal'], fontName='Helvetica', fontSize=9, leading=11, textColor=colors.HexColor('#A0AEC0'), spaceAfter=4)
    
    story = [Paragraph("Official Regional Water Quality Audit Report", title_style), Paragraph("Dataset Scale: Active Anomaly Tracking | Ingested via SQLite Layer", subtitle_style)]
    for _, row in report_df.iterrows():
        raw_time = str(row['phenomenonTime'])
        clean_time = raw_time.replace(" ", "T") if " " in raw_time else raw_time
        story.append(Paragraph(f"FACILITY: {str(row['facility_name']).upper()}", facility_style))
        metrics_text = f"<b>SUBSTANCE:</b> {row['determinand_prefLabel']}<br/><b>READING:</b> {row['result']} {str(row['unit']).upper()}<br/><b>COORDINATES:</b> {row['latitude']}, {row['longitude']}<br/><b>TIMESTAMP:</b> {clean_time}"
        story.append(Paragraph(metrics_text, metric_style))
        story.append(Paragraph("-" * 88, divider_style))
        
    try:
        doc.build(story)
        print(f"[+] Success: Generated matching visual PDF audit report at: {pdf_report_path}")
    except Exception as e:
        print(f"[-] PDF Structural Build Failure: {str(e)}")

if __name__ == "__main__":
    run_etl()
    check_results()
    map_anomalies()
    generate_pdf_documentation()
