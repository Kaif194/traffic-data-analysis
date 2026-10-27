import os
import sys
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# Ensure parent directory is in sys.path for config import
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from config import LOCAL_PROCESSED_DIR, LOCAL_REPORTS_DIR

def set_visual_style():
    """Sets a modern, clean visual theme for charts."""
    sns.set_theme(style="whitegrid")
    plt.rcParams["font.sans-serif"] = "DejaVu Sans"
    plt.rcParams["font.family"] = "sans-serif"

def generate_peak_hours_chart(processed_dir=LOCAL_PROCESSED_DIR, output_dir=LOCAL_REPORTS_DIR):
    path = os.path.join(processed_dir, "peak_hours", "results.csv")
    if not os.path.exists(path):
        print(f"[Visualization ERROR] File '{path}' missing. Run PySpark analysis first.")
        return False
        
    df = pd.read_csv(path).sort_values(by="Hour")
    
    fig, ax1 = plt.subplots(figsize=(10, 5))
    
    color = '#1f77b4'
    ax1.set_xlabel('Hour of Day (0-23)', fontsize=12, fontweight='bold')
    ax1.set_ylabel('Average Vehicle Count', color=color, fontsize=12, fontweight='bold')
    line1 = ax1.plot(df['Hour'], df['Avg_Vehicle_Count'], color=color, marker='o', linewidth=2.5, label='Avg Vehicles')
    ax1.tick_params(axis='y', labelcolor=color)
    ax1.set_xticks(range(0, 24))
    
    ax2 = ax1.twinx()
    color = '#ff7f0e'
    ax2.set_ylabel('Average Speed (km/h)', color=color, fontsize=12, fontweight='bold')
    line2 = ax2.plot(df['Hour'], df['Avg_Speed_KMH'], color=color, marker='s', linestyle='--', linewidth=2, label='Avg Speed')
    ax2.tick_params(axis='y', labelcolor=color)
    
    # Combined title & grid
    plt.title('Traffic Volume & Speed Distribution by Hour of Day', fontsize=14, fontweight='bold', pad=15)
    fig.tight_layout()
    
    output_path = os.path.join(output_dir, "peak_hours_trend.png")
    plt.savefig(output_path, dpi=300)
    plt.close()
    print(f"[Visualization] Chart generated -> '{output_path}'")
    return True

def generate_top_locations_chart(processed_dir=LOCAL_PROCESSED_DIR, output_dir=LOCAL_REPORTS_DIR):
    path = os.path.join(processed_dir, "location_analysis", "results.csv")
    if not os.path.exists(path):
        return False
        
    df = pd.read_csv(path).sort_values(by="Total_Vehicles", ascending=False).head(10)
    
    plt.figure(figsize=(11, 6))
    ax = sns.barplot(x="Total_Vehicles", y="Location", data=df, palette="crest")
    
    plt.title('Top 10 Busiest Traffic Locations by Total Volume', fontsize=14, fontweight='bold', pad=15)
    plt.xlabel('Total Vehicle Count', fontsize=12, fontweight='bold')
    plt.ylabel('Location', fontsize=12, fontweight='bold')
    
    for p in ax.patches:
        width = p.get_width()
        ax.annotate(f'{int(width):,}', (width + 500, p.get_y() + p.get_height() / 2.),
                    ha='left', va='center', fontsize=10, fontweight='semibold')
                    
    plt.tight_layout()
    output_path = os.path.join(output_dir, "top_locations.png")
    plt.savefig(output_path, dpi=300)
    plt.close()
    print(f"[Visualization] Chart generated -> '{output_path}'")
    return True

def generate_weekday_vs_weekend_chart(processed_dir=LOCAL_PROCESSED_DIR, output_dir=LOCAL_REPORTS_DIR):
    path = os.path.join(processed_dir, "day_type_analysis", "results.csv")
    if not os.path.exists(path):
        return False
        
    df = pd.read_csv(path)
    
    fig, axes = plt.subplots(1, 2, figsize=(12, 5))
    
    sns.barplot(x="Day_Type", y="Avg_Vehicle_Count", data=df, ax=axes[0], palette="Set2")
    axes[0].set_title('Average Vehicle Count (Weekday vs Weekend)', fontweight='bold')
    axes[0].set_ylabel('Avg Vehicles per Slot', fontweight='bold')
    
    sns.barplot(x="Day_Type", y="Avg_Speed_KMH", data=df, ax=axes[1], palette="Set2")
    axes[1].set_title('Average Speed in km/h (Weekday vs Weekend)', fontweight='bold')
    axes[1].set_ylabel('Avg Speed (km/h)', fontweight='bold')
    
    plt.tight_layout()
    output_path = os.path.join(output_dir, "weekday_vs_weekend.png")
    plt.savefig(output_path, dpi=300)
    plt.close()
    print(f"[Visualization] Chart generated -> '{output_path}'")
    return True

def generate_weather_impact_chart(processed_dir=LOCAL_PROCESSED_DIR, output_dir=LOCAL_REPORTS_DIR):
    path = os.path.join(processed_dir, "weather_analysis", "results.csv")
    if not os.path.exists(path):
        return False
        
    df = pd.read_csv(path).sort_values(by="Avg_Speed_KMH", ascending=False)
    
    plt.figure(figsize=(9, 5))
    ax = sns.barplot(x="Weather", y="Avg_Speed_KMH", data=df, palette="flare")
    
    plt.title('Impact of Weather Conditions on Average Traffic Speed', fontsize=14, fontweight='bold', pad=15)
    plt.xlabel('Weather Condition', fontsize=12, fontweight='bold')
    plt.ylabel('Average Speed (km/h)', fontsize=12, fontweight='bold')
    
    for p in ax.patches:
        height = p.get_height()
        ax.annotate(f'{height:.1f} km/h', (p.get_x() + p.get_width() / 2., height + 0.5),
                    ha='center', va='bottom', fontsize=10, fontweight='semibold')
                    
    plt.ylim(0, max(df["Avg_Speed_KMH"]) + 10)
    plt.tight_layout()
    output_path = os.path.join(output_dir, "weather_impact.png")
    plt.savefig(output_path, dpi=300)
    plt.close()
    print(f"[Visualization] Chart generated -> '{output_path}'")
    return True

def generate_all_visualizations():
    """Generates all 4 visual analytical charts."""
    set_visual_style()
    os.makedirs(LOCAL_REPORTS_DIR, exist_ok=True)
    print("\n[Visualization] Generating visual charts from PySpark output...")
    
    success = True
    success &= generate_peak_hours_chart()
    success &= generate_top_locations_chart()
    success &= generate_weekday_vs_weekend_chart()
    success &= generate_weather_impact_chart()
    
    if success:
        print("[Visualization] ALL CHARTS GENERATED SUCCESSFULLY IN:", LOCAL_REPORTS_DIR)
    return success

if __name__ == "__main__":
    generate_all_visualizations()
