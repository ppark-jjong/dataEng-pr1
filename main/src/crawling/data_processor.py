import pandas as pd
import numpy as np
from typing import Tuple, Dict, Any
import logging
from datetime import date

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DataProcessor:
    def __init__(self, config: Dict[str, Any]):
        self.config = config

    def process_dataframe(
        self, df: pd.DataFrame
    ) -> Tuple[pd.DataFrame, Dict[str, Any]]:
        logger.info(f"Original columns: {df.columns.tolist()}")
        original_unique_count = df["Cust Sys No"].nunique()

        df = self._rename_columns(df)
        df = self._convert_data_types(df)
        df = self._clean_replen_balance_order(df)
        df = self._handle_ship_from_code(df)
        df = self._calculate_fiscal_data(df)
        df = self._map_order_type(df)

        if "Country" in df.columns:
            df = df[df["Country"] == "KR"]
            logger.info("Filtered data for Country 'KR'")

        df = self._aggregate_duplicates(df)
        df = self._handle_missing_values(df)

        stats = self._calculate_stats(df, original_unique_count)
        logger.info(f"Final columns: {df.columns.tolist()}")
        return df, stats

    def _rename_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        logger.info("Renaming columns")
        return df.rename(columns=self.config["COLUMN_MAPPING"])

    def _convert_data_types(self, df: pd.DataFrame) -> pd.DataFrame:
        logger.info("Converting data types")
        if "Quantity" in df.columns:
            df["Quantity"] = (
                pd.to_numeric(df["Quantity"], errors="coerce").fillna(0).astype("Int64")  # 문자열을 숫자형으로 변환하고 변환 불가능한 값은 NaN으로 처리
            )

        date_columns = ["PutAwayDate", "ActualPhysicalReceiptDate"]
        for col in date_columns:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors="coerce")

        # PutAwayDate가 없는 경우 ActualPhysicalReceiptDate로 대체
        if "PutAwayDate" in df.columns and "ActualPhysicalReceiptDate" in df.columns:
            df.loc[df["PutAwayDate"].isnull(), "PutAwayDate"] = df.loc[
                df["PutAwayDate"].isnull(), "ActualPhysicalReceiptDate"
            ]

        if "PutAwayDate" in df.columns:
            df["InventoryDate"] = df["PutAwayDate"].dt.date

        # ActualPhysicalReceiptDate 컬럼 제거
        if "ActualPhysicalReceiptDate" in df.columns:
            df = df.drop(columns=["ActualPhysicalReceiptDate"])

        return df


    def _clean_replen_balance_order(self, df: pd.DataFrame) -> pd.DataFrame:
        logger.info("Cleaning Replen_Balance_Order")
        if "Replen_Balance_Order" in df.columns:
            df["Replen_Balance_Order"] = (
                df["Replen_Balance_Order"].astype(str).str.split(".").str[0]
            )
            numeric_mask = df["Replen_Balance_Order"].str.isnumeric()
            df.loc[numeric_mask, "Replen_Balance_Order"] = pd.to_numeric(
                df.loc[numeric_mask, "Replen_Balance_Order"], errors="coerce"
            ).astype("Int64")
        return df

    # ShipFromCode의 값이 REMARK라면 none으로 처리
    def _handle_ship_from_code(self, df: pd.DataFrame) -> pd.DataFrame:
        logger.info("Handling ShipFromCode")
        if "ShipFromCode" in df.columns:
            df.loc[df["ShipFromCode"].str.upper() == "REMARK", "ShipFromCode"] = np.nan
        return df

    def _calculate_fiscal_data(self, df: pd.DataFrame) -> pd.DataFrame:
        logger.info("Calculating fiscal data")
        if "PutAwayDate" in df.columns:
            fiscal_data = df["PutAwayDate"].apply(
                lambda x: (
                    self.get_dell_week_and_fy(x)
                    if pd.notna(x)
                    else ("Unknown", "Unknown")
                )
            )
            df["Week"] = fiscal_data.apply(lambda x: x[0])
            df["FY"] = fiscal_data.apply(lambda x: x[1])
            df["Quarter"] = df["PutAwayDate"].apply(
                lambda x: self.get_quarter(x) if pd.notna(x) else "Unknown"
            )
            df["Month"] = df["PutAwayDate"].dt.strftime("%m").fillna("00")
        return df

    def _map_order_type(self, df: pd.DataFrame) -> pd.DataFrame:
        logger.info("Mapping order types")
        df["OrderType"] = (
            df["EDI_Order_Type"]
            .map(self.config["ORDER_TYPE_MAPPING"])
            .fillna("Unknown")
        )
        return df

    def _aggregate_duplicates(self, df: pd.DataFrame) -> pd.DataFrame:
        logger.info("Aggregating duplicates")

        # Count_PO를 미리 계산
        df["Count_PO"] = df.groupby("Cust_Sys_No")["Cust_Sys_No"].transform("count")

        # 모든 컬럼에 대해 첫 번째 값을 선택하되, Quantity는 합계를 계산
        agg_funcs = {
            col: "first" for col in df.columns if col not in ["Cust_Sys_No", "Quantity"]
        }
        agg_funcs["Quantity"] = "sum"

        # 그룹화 및 집계
        df = df.groupby("Cust_Sys_No", as_index=False).agg(agg_funcs)

        return df


    def _handle_missing_values(self, df: pd.DataFrame) -> pd.DataFrame:
        logger.info("Handling missing values")
        for col in df.columns:
            if pd.api.types.is_object_dtype(df[col]):
                df[col] = df[col].fillna("")
            elif pd.api.types.is_numeric_dtype(df[col]):
                df[col] = df[col].fillna(0)
        return df

    def _calculate_stats(
        self, df: pd.DataFrame, original_unique_count: int
    ) -> Dict[str, Any]:
        processed_unique_count = len(df)
        match_rate = (
            (processed_unique_count / original_unique_count) * 100
            if original_unique_count > 0
            else 0
        )
        return {
            "original_unique_count": original_unique_count,
            "processed_unique_count": processed_unique_count,
            "match_rate": match_rate,
        }

    def get_fy_start(self, date: pd.Timestamp) -> pd.Timestamp:
        year = (
            date.year
            if date.month >= 2 or (date.month == 2 and date.day >= 7)
            else date.year - 1
        )
        feb_1 = pd.Timestamp(year=year, month=2, day=1)
        return feb_1 + pd.Timedelta(days=(5 - feb_1.dayofweek) % 7)

    def get_dell_week_and_fy(self, date: pd.Timestamp) -> Tuple[str, str]:
        fy_start = self.get_fy_start(date)
        fy = fy_start.year + 1 if date >= fy_start else fy_start.year
        days_since_fy_start = (date - fy_start).days
        dell_week = (days_since_fy_start // 7) + 1
        return f"WK{dell_week:02d}", f"FY{fy % 100:02d}"

    def get_quarter(self, date: pd.Timestamp) -> str:
        fy_start = self.get_fy_start(date)
        days_since_fy_start = (date - fy_start).days
        quarter = (days_since_fy_start // 91) + 1
        return f"Q{min(quarter, 4)}"


def main_data_processing(
    df: pd.DataFrame, config: Dict[str, Any]
) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    processor = DataProcessor(config)
    processed_df, stats = processor.process_dataframe(df)

    logger.info("Data Processing Statistics:")
    logger.info(f"Original unique records: {stats['original_unique_count']}")
    logger.info(f"Processed unique records: {stats['processed_unique_count']}")
    logger.info(f"Match rate: {stats['match_rate']:.2f}%")

    return processed_df, stats
