import pandas

def convertToExcel(spark, df, excel_writer="dfAsExcel", sheet_name="sheet1"):
    #excel_writer: path-like, file-like, or ExcelWriter object
    
    #this process could be done by only : inMemory_df = analysisResult_df.toPandas()
    #but as sometimes it raises error on dfGennerated by pydeequ modules, 
    #its safier to get df in memory with .collect() first

    sc = spark.sparkContext
    inMemory_df =df.collect()
    inMemory_df= sc.parallelize(inMemory_df).toDF()
    inMemory_df = inMemory_df.toPandas()
    
    if(type(excel_writer)==str):
        #make sure file_name/file_path is valid both when parameter string ends with or without .xlsx 
        excel_writer = excel_writer.replace(".xlsx","")
        excel_writer = excel_writer+".xlsx"
    
    inMemory_df.to_excel(excel_writer=excel_writer, sheet_name = sheet_name, index = False)