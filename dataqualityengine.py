from datetime import datetime
import time
import logging
import json
from pyspark.sql import functions as F
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from typing import List, Dict, Optional, Any
from pathlib import Path

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DataQualityEngine:
    def __init__(self, spark: SparkSession, bad_data_path: Path)-> None:
        self.spark = spark
        self.bad_data_path = bad_data_path

    def _create_spark_dataframe(self, data: Dict[str, Any]) -> DataFrame:
        """
        Convert a dictionary to a Spark DataFrame.
        """

        schema = StructType([
            StructField("check_name", StringType()),
            StructField("entity", StringType()),
            StructField("check_type", StringType()),
            StructField("columns", StringType()),  # JSON stored as string
            StructField("passed_count", IntegerType()),
            StructField("failed_count", IntegerType()),
            StructField("total_count", IntegerType()),
            StructField("failure_percentage", DoubleType()),
            StructField("execution_time", DoubleType()),
            StructField("bad_data_path", StringType(), nullable=True),
            StructField("status", StringType()),
            StructField("error_message", StringType(), nullable=True)
        ])
        return self.spark.createDataFrame([data], schema=schema)
    
    def _save_data_to_database(self, df) -> None:
        """
        Save the data to a database.
        """
        df.write.mode("append").saveAsTable("dqaas.dataquality.data_quality_checks")


    def _run_null_check(self, df, config):
        col_name = config['column']
        total_count = df.count()
        bad_df = df.filter(F.col(col_name).isNull())
        failed_count = bad_df.count()
        return total_count, failed_count, bad_df
    

    def _run_unique_check(self, df, config):
        """
            Args: df: Spark DataFrame as input
                  config: Dictionary containing the configuration for the check
            
            Returns: total_count, failed_count, bad_df
        """
        col_name = config['column']
        total_count = df.count()
        bad_df = df.groupBy(col_name).count().filter(F.col("count") > 1)
        failed_count = bad_df.count()
        return total_count, failed_count, bad_df
    
    def _run_regex_check(self, df, config):
        """
            Args: df: Spark DataFrame as input
                  config: Dictionary containing the configuration for the check
            returns: total_count, failed_count, bad_df
        """        
        col_name = config['column']
        pattern = config['pattern']
        total_count = df.count()
        bad_df = df.filter(~F.col(col_name).rlike(pattern))
        failed_count = bad_df.count()
        return total_count, failed_count, bad_df
    
    def _run_date_format_check(self, df, config):
        col_name = config['column']
        fmt = config['format']
        total_count = df.count()
        try:
            parsed = F.to_date(F.col(col_name), fmt)
            bad_df = df.filter(parsed.isNull() & F.col(col_name).isNotNull())
            failed_count = bad_df.count()
            return total_count, failed_count, bad_df
        except:
            return total_count, total_count, df
    
    def _run_allowed_values_check(self, df, config):
        col_name = config['column']
        allowed = config['allowed_values']
        total_count = df.count()
        bad_df = df.filter(~F.col(col_name).isin(allowed))
        failed_count = bad_df.count()
        return total_count, failed_count, bad_df

    def _run_cross_column_check(self, df, config):
        cols = config['columns']
        condition = config['condition']
        total_count = df.count()
        bad_df = df.filter(f"({condition})")
        failed_count = bad_df.count()
        return total_count, failed_count, bad_df

    def _run_completeness_check(self, df, config):
        col_name= config["columns"]
        total_count = df.count()
        cond = F.lit(False)
        for col in col_name:
            cond = cond | F.col(col).isNull()
        bad_df = df.filter(cond)
        failed_count = bad_df.count()
        return total_count, failed_count, bad_df

    def _run_referential_integrity_check(self, df, config):
        """
        This method is a placeholder for referential integrity checks.
        It should be implemented based on specific requirements.
        """
        ref_table = config.get("reference_table")
        ref_col = config.get("reference_column")
        col = config["column"]
        total_count = df.count()

        ref_table_path = f"dataquality/reference_data/{ref_table}.csv"

        if not ref_col:
            raise ValueError("Reference column  must be specified for referential intgerity checks")
        
        if not Path(ref_table_path).exists():
            raise FileNotFoundError(f"Reference table {ref_table} does not exist in the expected path.")

        ref_df = self.spark.read.csv(ref_table_path, header=True, inferSchema=True)
        bad_df = df.join(ref_df, df[col] == ref_df[ref_col], "left_anti")
        failed_count = bad_df.count()
        return total_count, failed_count, bad_df

    def _run_statistical_check(self, df, config):
        col_name = config['column']
        threshold = config['threshold']
        total_count = df.count()
        
        # Handle possible nulls
        df = df.filter(F.col(col_name).isNotNull())
        
        # Calculate z-scores
        stats = df.select(
            F.mean(col_name).alias('mean'),
            F.stddev(col_name).alias('std')
        ).collect()[0]
        
        if stats['std'] is None or stats['std'] == 0:
            return total_count, 0, None
            
        z_score = (F.col(col_name) - stats['mean']) / stats['std']
        bad_df = df.filter(F.abs(z_score) > threshold)
        failed_count = bad_df.count()
        return total_count, failed_count, bad_df
    
    def _run_time_difference_check(self, df, config):
        cols = config['columns']
        max_duration = config['max_duration']
        hours = int(max_duration.split()[0])
        total_count = df.count()
        
        # Convert to timestamp if needed
        for col in cols:
            if str(df.schema[col].dataType) not in ["TimestampType", "DateType"]:
                df = df.withColumn(col, F.to_timestamp(col))
        
        diff = (F.unix_timestamp(F.col(cols[1])) - F.unix_timestamp(F.col(cols[0])))
        bad_df = df.filter(diff > hours * 3600)
        failed_count = bad_df.count()
        return total_count, failed_count, bad_df

    def _run_length_check(self, df, config):
        col_name = config['column']
        min_len = config['min_length']
        max_len = config['max_length']
        total_count = df.count()
        
        length = F.length(F.col(col_name))
        bad_df = df.filter((length < min_len) | (length > max_len))
        failed_count = bad_df.count()
        return total_count, failed_count, bad_df
    
    def checks_registry(self) -> Dict[str, Any]:
        return {
            "null_check": self._run_null_check,
            "unique_check": self._run_unique_check,
            "regex_match": self._run_regex_check,
            "date_format": self._run_date_format_check,
            "allowed_values": self._run_allowed_values_check,
            "cross_column": self._run_cross_column_check,
            "completeness_check": self._run_completeness_check,
            "referential_integrity": self._run_referential_integrity_check,
            "statistical": self._run_statistical_check,
            "time_difference": self._run_time_difference_check,
            "length_check": self._run_length_check
        }
    
    def run_checks(self, config_path, data_dict):
        with open(config_path) as f:
            checks = json.load(f)
        
        total_checks = len(checks)
        start_time = time.time()

        df = data_dict.get("transactions")

        with tqdm(total=total_checks, desc="Running Data Quality Checks") as pbar:
            for check in checks:
                result = {
                        'check_name': check['name'],
                        'entity': check["entity"],
                        'check_type': check['type'],
                        'columns': json.dumps(check.get('column') or check.get('columns')),
                        'passed_count': 0,
                        'failed_count': 0,
                        'total_count': 0,
                        'failure_percentage': 0.0,
                        'execution_time': 0.0,
                        'bad_data_path': None,
                        'status': 'SUCCESS',
                        'error_message': None
                    }
                
                if df is None:
                        result.update({
                            'status': 'ERROR',
                            'error_message': f"Data source not found"
                        })
                        self._save_results(result)
                        pbar.update(1)
                        continue
                func_dict = self.checks_registry()

                try:
                    check_name = func_dict.get(check["type"])
                    if not check_name:
                        print(f"Check type {check['type']} not recognized. Skipping...")
                        continue

                    t0 = time.time()
                    total_count, failed_count, bad_df = check_name(df, check)
                    exec_time = time.time() - t0
                    
                    if failed_count > 0:
                        self._write_bad_data_to_storage(bad_df, check["name"])
                        print(f"Check {check['name']} failed: {failed_count} out of {total_count} records are bad.")
                    else:
                        print(f"Check {check['name']} passed: All {total_count} records are good.")
                    # Update result
                    result.update({
                        'passed_count': total_count - failed_count,
                        'failed_count': failed_count,
                        'total_count': total_count,
                        'failure_percentage': (failed_count / total_count) * 100 if total_count > 0 else 0,
                        'execution_time': exec_time,
                        'bad_data_path': self.bad_data_path,
                    })
                except Exception as e:
                    result.update({
                        'status': 'ERROR',
                        'error_message': str(e)
                    })
                    print(f"Error in check {check['name']}: {str(e)}")
                
                result_df = self._create_spark_dataframe(data=result)
                self._save_data_to_database(df=result_df)
                self._save_results(result)
                pbar.set_postfix_str(f"{pbar.n+1}/{total_checks} | Last: {check['name']}")
                pbar.update(1)  
     
        total_time = time.time() - start_time
        logger.info(f"\nAll checks completed in {total_time:.2f} seconds")
    













