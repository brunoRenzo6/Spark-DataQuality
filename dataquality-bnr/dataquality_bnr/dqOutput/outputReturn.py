import json


# In this file a series of functions used to build an dq_run_return object
# An object who can help users to navigate through the different boolean, parquet and .csv's results

def buildReturn(step, viewName, r, **kwargs):
    if step=="run":
        r[viewName]={}
        r[viewName]["persistentReposiotry"]={}
        r[viewName]["persistentReposiotry"]["successMetrics"]=kwargs["p1"]
        r[viewName]["persistentReposiotry"]["checkResult"]=kwargs["p2"]

        r[viewName]["currentResult"]={}
        r[viewName]["currentResult"]["successMetrics"]=kwargs["p3"]
        r[viewName]["currentResult"]["checkResult"]=kwargs["p4"]
        
        if "p5" in kwargs:
            r[viewName]["temporaryRepository"]=kwargs["p5"]

    elif step=="ovResult":
        #r[viewName]["currentResult"]["csvResult"]=p1
        if viewName == "overall":
            r["overall"]={"currentResult":{"csvResult": kwargs["p1"]}}
        else:
            r[viewName]["currentResult"]["csvResult"] = kwargs["p1"]        
    return r

    
class DqReturn():
    def __init__(self, r):
        self.r=r
    
    def printReturn(self):
        print(json.dumps(self.r, indent=5))
        
    def getDict(self):
        return self.r
    
    def get_overall_booleanResult(self):
        overallResultCsv = self.getDict()["overall"]["currentResult"]["csvResult"]
    
        if overallResultCsv.endswith("_OK.csv"):
            return True
        elif overallResultCsv.endswith("_NOK.csv"):
            return False
        else:
            print("Error! Overall csvResult name out of pattern.")
            return False
        
    def get_overall_csvResult_df(self, spark):
        overall_csvResult_path = self.getDict()["overall"]["currentResult"]["csvResult"]
        overall_csvResult_df = spark.read.option("header",True).csv(overall_csvResult_path)
        
        return overall_csvResult_df
        
        
        
        