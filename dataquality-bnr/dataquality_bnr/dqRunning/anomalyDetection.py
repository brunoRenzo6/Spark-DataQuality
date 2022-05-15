from pydeequ.analyzers import *
from pydeequ.anomaly_detection import *
from pydeequ.repository import *
from pydeequ.verification import *

from dataquality_bnr.yamlHandler import verificationSuite as yVS

def getVerificationRunBuilder(spark, df, repository):
    vRunBuilder = (VerificationSuite(spark)
               .onData(df)
               .useRepository(repository))
    
    return vRunBuilder

def addAnomalyChecks(vRunBuilder, adParameters):
    for d in adParameters:
        anomalyDetection = d["addAnomalyCheck"]
        if "custom_Histogram" not in anomalyDetection["analyzer"]:
            strategy = eval(anomalyDetection["strategy"])
            analyzer = eval(anomalyDetection["analyzer"])

            vRunBuilder = vRunBuilder.addAnomalyCheck(strategy, analyzer)
    return vRunBuilder


def runAnomalyDetection(spark, df, repository, adParameters):
    vRunBuilder = getVerificationRunBuilder(spark, df, repository)
    vRunBuilder = addAnomalyChecks(vRunBuilder, adParameters)
    currResult = vRunBuilder.run()
    
    return currResult


def getAnomalyDetection(spark, df, vsYamlPath, repository):
    print("getAnomalyDetection...")
    
    adParameters = yVS.getADParameters(vsYamlPath)

    currResult = runAnomalyDetection(spark, df, repository, adParameters)
    
    return currResult
            
            

