from dataquality_bnr.yamlHandler import infrastructure as yI

def writeSuccessMetrics(successMetrics_df, view_path):
    hdfsPath_successMetrics = view_path+"/currentResult/successMetrics.parquet"
    successMetrics_df.write.mode('overwrite').parquet(hdfsPath_successMetrics)
    return hdfsPath_successMetrics
    
def writeCheckResult(checkResult_df, view_path):
    hdfsPath_checkResult = view_path+"/currentResult/checkResult.parquet"   
    checkResult_df.write.mode('overwrite').parquet(hdfsPath_checkResult)
    return hdfsPath_checkResult