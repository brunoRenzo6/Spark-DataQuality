import dataquality_bnr.dqSupport.dfGennerator as dfGennerator
import dataquality_bnr.dqSupport.excelGennerator as excelGennerator
import dataquality_bnr.dqSupport.dfHandler as dfHandler
import importlib_resources

import pydeequ


def getDataframe(spark, yaml_path):
    return dfGennerator.getDataframe(spark, yaml_path)

def convertToExcel(spark, df, excel_writer="dfAsExcel", sheet_name="sheet1"):
    return excelGennerator.convertToExcel(spark, df, excel_writer, sheet_name)

def getDeequJar_path():
    my_resources = importlib_resources.files("dataquality_bnr")
    data = (my_resources / "static" / "deequ-1.0.5.jar")
    return str(data)

def getDeequJar_excludes():
    return pydeequ.f2j_maven_coord

def df_to_list(df):
    return dfHandler.df_to_list(df)