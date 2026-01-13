import sqlite3
import pandas as pd
import numpy as np
import config
from datetime import datetime, timedelta

class DatabaseManager:
    def __init__(self, db_path=None):
        self.db_path = db_path or config.DB_PATH
        self.vni_map = {}
        self.vni_dates = []

    def get_connection(self):
        return sqlite3.connect(self.db_path)

    def load_vnindex(self):
        """
        Loads VNINDEX history into memory for fast lookup.
        """
        conn = self.get_connection()
        try:
            query = "SELECT Date, Close FROM VNINDEX ORDER BY Date"
            df = pd.read_sql_query(query, conn)
            df['Date'] = pd.to_datetime(df['Date']).dt.normalize()
            self.vni_map = dict(zip(df['Date'], df['Close']))
            self.vni_dates = sorted(self.vni_map.keys())
        except Exception as e:
            print(f"Error loading VNINDEX: {e}")
        finally:
            conn.close()

    def get_nearest_vni(self, target_date):
        """
        Finds VNI close price on target_date. 
        If missing (weekend/holiday), finds the nearest previous trading day.
        """
        if not self.vni_map:
            self.load_vnindex()
            
        target_date = pd.to_datetime(target_date)
        
        # Try direct lookup first
        if target_date in self.vni_map:
            return self.vni_map[target_date]
        
        # Look backwards
        # We can use numpy searchsorted if performance is critical, 
        # but simple loop or pandas asof is fine for small data.
        # Since self.vni_dates is sorted:
        import bisect
        idx = bisect.bisect_right(self.vni_dates, target_date)
        if idx > 0:
            nearest_date = self.vni_dates[idx - 1]
            return self.vni_map[nearest_date]
        
        return None

    def get_all_stocks(self):
        """
        Key function to get and process all stock data.
        """
        self.load_vnindex() # Ensure VNI is loaded
        conn = self.get_connection()
        
        try:
            query = """
                SELECT 
                    Id, Ticker, Pick_Date, Price_At_Call, 
                    TargetPrice, StopLoss, 
                    LastPrice, IsClosed, Current_Action,
                    ShortTermConviction, LongTermConviction, Action_Date,
                    PickedBy
                FROM StockInFocus
                ORDER BY Pick_Date DESC
            """
            df = pd.read_sql_query(query, conn)
            
            # 1. Date Conversions
            # Use mixed format inference and normalize to midnight (remove time)
            df['Pick_Date'] = pd.to_datetime(df['Pick_Date']).dt.normalize()
            
            # Action_Date: "2026-01-08 07:32:14..." -> 2026-01-08 00:00:00
            df['Action_Date'] = pd.to_datetime(df['Action_Date'], format='mixed', errors='coerce').dt.normalize()
            
            # 2. Determine Close Date for Calculation
            # If Closed -> Use Action_Date.
            # If Active -> Use Today normalized
            today = pd.Timestamp.now().normalize()
            
            # Helper to extract just the date part for safety
            def get_calc_date(row):
                if row['IsClosed'] == 1 and pd.notnull(row['Action_Date']):
                    return row['Action_Date']
                return today

            df['Calc_Close_Date'] = df.apply(get_calc_date, axis=1)

            # 3. Calculate Stock Return
            # Logic: (LastPrice - Price_At_Call) / Price_At_Call
            df['Stock_Ret'] = (df['LastPrice'] - df['Price_At_Call']) / df['Price_At_Call']

            # 4. Calculate VNI Return
            # VNI_Start
            df['VNI_Pick'] = df['Pick_Date'].apply(self.get_nearest_vni)
            # VNI_End
            df['VNI_Close'] = df['Calc_Close_Date'].apply(self.get_nearest_vni)
            
            # Handle case where VNI lookup returns None (e.g. data strictly missing)
            # Fill with 0 return or NaN to avoid crash
            df['VNI_Ret'] = (df['VNI_Close'] - df['VNI_Pick']) / df['VNI_Pick']

            # 5. Alpha & Rating
            df['Alpha'] = df['Stock_Ret'] - df['VNI_Ret']
            
            def get_rating(alpha):
                if pd.isna(alpha): return "N/A"
                if alpha > 0: return "Outperform"
                if alpha < 0: return "Underperform"
                return "Neutral"

            df['Rating'] = df['Alpha'].apply(get_rating)
            
            return df

        except Exception as e:
            print(f"Error processing data: {e}")
            import traceback
            traceback.print_exc()
            return pd.DataFrame()
        finally:
            conn.close()

    def get_yearly_summary(self, df):
        """
        Aggregates data by Year with specific formatting.
        Includes a "Total" row at the bottom.
        """
        if df.empty:
            return pd.DataFrame()

        # Work on a copy to avoid side effects
        df = df.copy()
        df['Year'] = df['Pick_Date'].dt.year
        
        # Helper to format percentage string
        def format_pct_count(series, target_rating, total_count):
            count = (series == target_rating).sum()
            if total_count == 0: return "0% (0/0)"
            pct = (count / total_count) * 100
            return f"{pct:.1f}% ({count}/{total_count})"

        # 1. Calculate Yearly Stats
        years = sorted(df['Year'].unique(), reverse=True)
        summary_data = []

        for year in years:
            year_df = df[df['Year'] == year]
            total = len(year_df)
            
            summary_data.append({
                "Year": str(year),
                "Total Calls": total,
                "Avg Return": year_df['Stock_Ret'].mean(),
                "Avg Alpha": year_df['Alpha'].mean(),
                "% Outperform": format_pct_count(year_df['Rating'], "Outperform", total),
                "% Underperform": format_pct_count(year_df['Rating'], "Underperform", total)
            })

        # 2. Calculate Grant Total (All Time)
        total_all = len(df)
        summary_data.append({
            "Year": "Total",
            "Total Calls": total_all,
            "Avg Return": df['Stock_Ret'].mean(),
            "Avg Alpha": df['Alpha'].mean(),
            "% Outperform": format_pct_count(df['Rating'], "Outperform", total_all),
            "% Underperform": format_pct_count(df['Rating'], "Underperform", total_all)
        })
            
        return pd.DataFrame(summary_data)
