from pydeequ.checks import Check, CheckLevel
from utils import validate_data, log_etl_step
import logging

logger = logging.getLogger(__name__)

def validate_data(df, stage, paths):
    try:
        check = Check(sparkSession=df.sparkSession, checkLevel=CheckLevel.Warning, description=f"Data validation for {stage} stage")
        check.isComplete("column_name").isUnique("id_column").isNonNegative("numeric_column")
        result = validate_data(df, check)
        if result.filter("check_status != 'Success'").count() > 0:
            raise Exception(f"Data validation failed at {stage} stage")
        log_etl_step(f"validate_data_{stage}", "success", paths['log_bucket'], f"validation/{stage}/log.json")
    except Exception as e:
        logger.error(f"Data validation failed at {stage} stage: {e}")
        log_etl_step(f"validate_data_{stage}", "failure", paths['log_bucket'], f"validation/{stage}/log.json")
        raise
