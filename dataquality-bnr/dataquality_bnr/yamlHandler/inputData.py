from pyspark.sql.dataframe import DataFrame
from dataquality_bnr.dqSupport import main as dqSup

def getDataframe(spark, inputData):
    print("getDataframe...")
    
    df=None
    
    if type(inputData) == DataFrame:
        df=inputData
    elif type(inputData) == str:
        inputDataYaml = inputData
        df = dqSup.getDataframe(spark, yaml_path=inputDataYaml)
    
    return df