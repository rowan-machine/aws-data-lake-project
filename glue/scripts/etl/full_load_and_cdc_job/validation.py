from pydeequ.checks import Check, CheckLevel
from utils import validate_data

def validate_data(df, stage):
    check = Check(sparkSession=df.sparkSession, checkLevel=CheckLevel.Warning, description=f"Data validation for {stage} stage")
    check.isComplete("column_name").isUnique("id_column").isNonNegative("numeric_column")
    result = validate_data(df, check)
    if result.filter("check_status != 'Success'").count() > 0:
        raise Exception(f"Data validation failed at {stage} stage")
