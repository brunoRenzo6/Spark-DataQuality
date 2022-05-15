from dataquality_bnr.yamlHandler import infrastructure as yI
from dataquality_bnr.yamlHandler import inputData as yInputData
from dataquality_bnr.dqRunning import persistentRep as dqRunPR
from dataquality_bnr.dqRunning import filtering as dqRunF
from dataquality_bnr.dqRunning import temporaryRep as dqRunTR
from dataquality_bnr.dqRunning import anomalyDetection as dqRunAD
from dataquality_bnr.dqRunning import customHistogram as dqRunCustomH
from dataquality_bnr.dqRunning import check as dqRunCheck
from dataquality_bnr.dqRunning import consolidate as dqRunC
from dataquality_bnr.dqRunning import currentResult as dqRunCR
from dataquality_bnr.dqOutput import main as dqOutput

from pyspark.sql import functions as F

        
class DqView:
    def __init__(self, main_path, viewDict, verificationSuite_type):
        """
        :param main_path string: String Path of the main Dq application
        :param viewDict dict: dataQuality dict containing all necessary parameters
        :param verificationSuite_type string: verificationSuite type, being 'AnomalyDetection' or 'Check'

        :var viewName string: String name of the dqView
        :var df|yamlFile inputData: inputData which DQ is going to process
        :var yamlFile infraYaml: yamlFile containing the infrastructure configurations
        :var yamlFile vsYaml: yamlFile containing the verificationSuite configurations
        """
        
        self.viewDict = viewDict
        self.vsType = verificationSuite_type
        self.main_path = main_path
        
        self.viewName = self.viewDict["viewName"]
        self.inputData = self.viewDict["inputData"]
        self.infraYaml = self.viewDict["infraYaml"]
        self.vsYaml = self.viewDict["vsYaml"]
        
        self.view_path = self.main_path + self.viewName
        
class Dq:
    def __init__(self, spark, main_path):
        self.spark = spark
        self.main_path = self.set_main_path_pattern(main_path)
        self.dqViews = {}
        self.count=0
        
    def set_main_path_pattern(self, main_path):
        if main_path.endswith("/") == False:
            main_path = main_path+"/"
            
        return main_path
    
    def getMainPath(self):
        return self.main_path
    
    def addView(self, viewDict, verificationSuite_type):
        print("__Evaluate DataQuality View and add to Main DataQuality Process__")
        newView = DqView(self.main_path, viewDict, verificationSuite_type)
        yI.evaluatePattern(newView.infraYaml, newView.vsType, newView.viewName)
        
        self.dqViews[newView.viewName]=newView
        
        return self
        
    def getViews(self):
        return "NotImplementedYet"

    def run(self):
        r={}
        print()
        print()
        print("__Running all DataQuality views__")
        for viewName in self.dqViews:
            print("run() "+viewName+":")
            dqView = self.dqViews[viewName]
            
            if dqView.vsType == "AnomalyDetection":
                r=self.runAnomalyDetection(dqView, r)
                
            elif dqView.vsType == "Check":
                r=self.runCheck(dqView, r)
                
            print()
            
        print()    
        print("__Get overall result__")
        r = dqOutput.getOverallResult(self.spark, self.dqViews, self.main_path, r)
        
        dqReturn = dqOutput.DqReturn(r)
        return dqReturn
                
        
    def runAnomalyDetection(self,dqView, r):
        pRep_df = dqRunPR.getPersistentRepository(self.spark, dqView.view_path)
        filtered_df = dqRunF.applyFiltering(pRep_df, dqView.infraYaml)
        repository = dqRunTR.buildTemporaryRepository(self.spark, filtered_df, dqView.view_path)
        
        df = yInputData.getDataframe(self.spark, dqView.inputData)
        
        currResult = dqRunAD.getAnomalyDetection(self.spark, df, dqView.vsYaml, repository)
        successMetrics_df, checkResult_df = dqRunC.getOuputDFs(self.spark, currResult, dqView.vsYaml)
        
        custom_currResult = dqRunCustomH.custom_getAnomalyDetection(self.spark, df, dqView.vsYaml, repository)

        if(custom_currResult!=None):
            custom_successMetrics_df, custom_checkResult_df = dqRunC.getOuputDFs(self.spark, custom_currResult, dqView.vsYaml)
            successMetrics_df = dqRunCustomH.union_custom_df(successMetrics_df, custom_successMetrics_df)
            checkResult_df = dqRunCustomH.union_custom_df(checkResult_df, custom_checkResult_df)

        
        if repository.load().getSuccessMetricsAsJson() == []:
            print("As AnomalyDetection temporaryRepository is still empty. Forcing Success Outcome")
            successMetrics_df = successMetrics_df.withColumn("check_status", F.lit("Success"))
            checkResult_df = checkResult_df.withColumn("check_status", F.lit("Success"))
            
        
        print("writeTo_persistentRepository...")
        r_p1=(dqRunPR.writeSuccessMetrics(successMetrics_df,dqView.view_path))
        r_p2=(dqRunPR.writeCheckResult(checkResult_df,dqView.view_path))

        print("writeTo_currentResults...")
        r_p3=(dqRunCR.writeSuccessMetrics(successMetrics_df,dqView.view_path))
        r_p4=(dqRunCR.writeCheckResult(checkResult_df,dqView.view_path))

        print("DataQuality process finished.")
        r = dqOutput.buildReturn("run", dqView.viewName, r=r, p1=r_p1, p2=r_p2, p3=r_p3, p4=r_p4, p5=repository.path)
        
        return r
    
    
    
    def runCheck(self, dqView, r):
        df = yInputData.getDataframe(self.spark, dqView.inputData)
        
        currResult = dqRunCheck.getCheck(self.spark, df, dqView.vsYaml)
        successMetrics_df, checkResult_df = dqRunC.getOuputDFs(self.spark, currResult, dqView.vsYaml)

        print("writeTo_persistentRepository...")
        r_p1 = dqRunPR.writeSuccessMetrics(successMetrics_df, dqView.view_path)
        r_p2 = dqRunPR.writeCheckResult(checkResult_df, dqView.view_path)

        print("writeTo_currentResults...")
        r_p3 = dqRunCR.writeSuccessMetrics(successMetrics_df, dqView.view_path)
        r_p4 = dqRunCR.writeCheckResult(checkResult_df, dqView.view_path)

        print("DataQuality process finished.")
        r = dqOutput.buildReturn("run", dqView.viewName, r=r, p1=r_p1, p2=r_p2, p3=r_p3, p4=r_p4)
        return r
         
        
        
        