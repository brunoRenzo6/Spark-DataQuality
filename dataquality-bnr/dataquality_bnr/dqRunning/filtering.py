from pyspark.sql.window import Window
from pyspark.sql import functions as F

from dataquality_bnr.yamlHandler import infrastructure as  yI

def filterLastNDays(df, lastNDays):
    windowSpec = Window.orderBy(F.col("YY_MM_DD").desc())
    
    filtered_df = (df.withColumn("dense_rank", F.dense_rank().over(windowSpec))
                   .filter(F.col("dense_rank")<=lastNDays)
                   .drop("dense_rank"))
    
    return filtered_df

def filterOnlySucced(df):
    filtered_df = (df
                   .filter(F.col("check_status")=="Success"))
    return filtered_df

def filterLastValidRecord(df):
    windowSpec = Window.partitionBy("YY_MM_DD").orderBy(F.col("dataset_date").desc())
    
    filtered_df = (df
                   .withColumn("dense_rank", F.dense_rank().over(windowSpec))
                   .filter(F.col("dense_rank")==1)
                   .drop("dense_rank"))
    return filtered_df

def filterMaxSize(df, maxSize):
    windowSpec = Window.orderBy(F.col("YY_MM_DD").desc())
    filter_df = (df
                 .withColumn("dense_rank", F.dense_rank().over(windowSpec))
                 .filter(F.col("dense_rank")<=maxSize)
                 .drop("dense_rank"))
    return filter_df


def applyFiltering(persistentRepository, infraYaml):
    print("applyFiltering...")
    
    df = persistentRepository
    filters= yI.getDefinedFilters(infraYaml)
    
    if df != None:
        filtered_df = filterLastNDays(df, filters["lastNDays"])
        if filters["onlySucceed"]=="True":
            filtered_df = filterOnlySucced(filtered_df)
        filtered_df = filterLastValidRecord(filtered_df)
        filtered_df = filterMaxSize(filtered_df, filters["maxSize"])
    
        return filtered_df
    else:
        return df