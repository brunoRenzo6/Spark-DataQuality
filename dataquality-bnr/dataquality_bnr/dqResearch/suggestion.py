from pyspark.sql import SparkSession, Row, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import *

from datetime import datetime
import pandas as pd

import pydeequ
from pydeequ.suggestions import *

def runConstraintSuggestion(spark, df):
    suggestionResult = ConstraintSuggestionRunner(spark) \
             .onData(df) \
             .addConstraintRule(DEFAULT()) \
             .run()
    
    suggestionResult_df = spark.createDataFrame([Row(**i) for i in suggestionResult["constraint_suggestions"]])
    
    return suggestionResult_df