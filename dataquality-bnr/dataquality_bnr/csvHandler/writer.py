def sparkWriteCsv(df, writeMode, path):
    
    
    (df
    .coalesce(1)
    .write
    .mode(writeMode)
    .option("header","true")
    .csv(path))

def deleteNotPartitionFiles(sc, df, path):
    hadoop = sc._jvm.org.apache.hadoop
    conf = sc._jsc.hadoopConfiguration()
    src_dir = hadoop.fs.Path(path)
    fs = src_dir.getFileSystem(conf)
    list_status = fs.listStatus(src_dir)

    for file in list_status:
        fileName=file.getPath().getName()
        if fileName.startswith('part-')==False:
            fs.delete(file.getPath())
            #print("File deleted: "+fileName)

def getCsvPartitionName(sc, path):
    hadoop = sc._jvm.org.apache.hadoop
    conf = sc._jsc.hadoopConfiguration()
    src_dir = hadoop.fs.Path(path)
    fs = src_dir.getFileSystem(conf)
    list_status = fs.listStatus(src_dir)

    for file in list_status:
        fileName=file.getPath().getName()
        if fileName.startswith('part-'):
            return fileName
        
def renameFile(sc, originalPath, newPath):
    hadoop = sc._jvm.org.apache.hadoop
    conf = sc._jsc.hadoopConfiguration()
    src_file = hadoop.fs.Path(originalPath)
    dest_file = hadoop.fs.Path(newPath)
    fs = src_file.getFileSystem(conf)

    fs.rename(src_file, dest_file)

def writeSingleCsvFile(spark, df, writeMode, path, csvName):
    sc=spark.sparkContext
    
    sparkWriteCsv(df, writeMode, path)
    deleteNotPartitionFiles(sc, df, path)
    originalFileName = getCsvPartitionName(sc, path)
    newFileName = csvName
    
    originalPath = path + originalFileName
    newPath = path + newFileName
    
    #print("originalPath: "+originalPath)
    print("writing to: "+newPath)
    renameFile(sc, originalPath, newPath)