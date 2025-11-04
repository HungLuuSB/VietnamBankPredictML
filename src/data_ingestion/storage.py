from os import replace
from typing import Literal
import pandas as pd
from pathlib import Path
from loguru import logger
from sqlalchemy import create_engine, Engine
from sqlalchemy.exc import SQLAlchemyError

class SQLStorage:
    def __init__(self, db_uir: str = "sqlite:///data/finance_data.db") -> None:
        Path("data").mkdir(parents=True, exist_ok=True)
        logger.info(f"[sqlstorage] Attempting to create SQLite storage at {db_uir}...")
        try:
            self.engine: Engine = create_engine(db_uir)
            logger.success(f"[sqlstorage] Successfully created SQLite storage at {db_uir}.")
        except Exception as e:
            logger.error(f"[sqlstorage] Encounterd error(s) while creating SQLAlchemy Engine: {e}")
            raise

    def save_data(self, df: pd.DataFrame, table_name: str, if_exists: Literal["replace", "append", "fail"] = "replace") -> None:
        if df.empty:
            logger.warning(f"[sqlstorage] Skipped save_data as DataFrame is empty.")
            return
        safe_table_name = "".join(c for c in table_name if c.isalnum() or c == '_')
        try:
            df.to_sql(
                safe_table_name,
                self.engine,
                if_exists=if_exists,
                index=True
            )
            logger.success(f"[sqlstorage] Successfully saved data into {safe_table_name} (MODE: {if_exists}).")
        except SQLAlchemyError as e:
            logger.error(f"[sqlstorage] Encounterd SQL Error when attempting to save data into {safe_table_name}: {e}")
        except Exception as e:
            logger.error(f"[sqlstorage] An error occourred when attempting to save data into {safe_table_name}: {e}")

    def load_data(self, table_name: str, start_date: str, end_date: str) -> pd.DataFrame:
        safe_table_name = "".join(c for c in table_name if c.isalnum() or c == '_')

        try:
            query = f'SELECT * FROM "{safe_table_name}"'
            conditions = []
            params = {}
            if start_date:
                conditions.append('"date" >= :start_date')
                params['start_date'] = start_date
            if end_date:
                conditions.append('"date" <= :end_date')
                params['end_date'] = end_date
                
            if conditions:
                query += " WHERE " + " AND ".join(conditions)
                
            query += ' ORDER BY "date" ASC'

            df = pd.read_sql_query(
                query,
                con=self.engine,
                params=params,
                index_col='date',
                parse_dates=['date']
            )
            
            logger.info(f"[sqlstorage] Finished loading data {len(df)} from dòng từ bảng '{safe_table_name}'")
            return df
        
        except SQLAlchemyError as e:
            if "no such table" in str(e).lower():
                logger.warning(f"[sqlstorage] Table '{safe_table_name}' is not exist.")
            else:
                logger.error(f"[sqlstorage] Encounterd SQL Error when loading data from table {safe_table_name}: {e}")
            return pd.DataFrame()
        except Exception as e:
            logger.error(f"[sqlstorage] An error occourred when attempting to save data into {safe_table_name}: {e}")
            return pd.DataFrame()

