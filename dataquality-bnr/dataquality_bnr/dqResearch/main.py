from dataquality_bnr.dqResearch import analyzer as analyzer
from dataquality_bnr.dqResearch import profile as profile
from dataquality_bnr.dqResearch import suggestion as suggestion


def runAnalyzer(spark, df, yaml_path):
    return analyzer.runAnalyzer(spark, df, yaml_path)

def runProfile(spark, df, metrics_file):
    return profile.runProfile(spark, df, metrics_file)

def runConstraintSuggestion(spark, df):
    return suggestion.runConstraintSuggestion(spark, df)
