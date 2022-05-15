from dataquality_bnr.dqOutput import overallResult as ovResult
from dataquality_bnr.dqOutput import outputReturn as opReturn

def getOverallResult(spark, viewName_list, main_path, r):
    return ovResult.getOverallResult(spark, viewName_list, main_path, r)

def buildReturn(step, viewName, r, **kwargs):
    return opReturn.buildReturn(step, viewName, r, **kwargs)

def DqReturn(r):
    return opReturn.DqReturn(r)