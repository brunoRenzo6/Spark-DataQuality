from pydeequ.analyzers import *
from pydeequ.anomaly_detection import *
from pydeequ.repository import *
from pydeequ.verification import *
from pydeequ.checks import *

from dataquality_bnr.yamlHandler import verificationSuite as yVS

def isAnPydeequValidCheck(constraint):
    """
    Verifies if the constraint is truly valid. There are a few constraints that were not moved from deequ(scala) to pydeequ(python), therefore the necessity of such precaution.
    isContainedIn is one of those. In scala lib it accepts maximum of 4 params, python version of same Check accepts only 2 params.
    """

    split = constraint.split("(")
    checkMethod = split[0]
    checkMethod_arguments = split[1:]
    
    if checkMethod=="isContainedIn":
        full_param = "("+checkMethod_arguments[0]
        full_param_eval = eval(full_param)

        if len(full_param_eval)>2:
            return False
        
        else:
            return True

def evaluateContraints(check, constraintDict):
    validConstraints=[]
    
    for d in constraintDict:
        constraint=d["addConstraint"]
        r = isAnPydeequValidCheck(constraint)

        if r==False:
            print("Invalid Constraint: "+constraint)
        else:
            validConstraints.append(constraint)
            check=(eval("check."+constraint))
    

    nConstraints=len(constraintDict)
    nValidaConstraints=len(validConstraints)
    nInvalidConstraints = nConstraints-nValidaConstraints
    if nValidaConstraints==0:
        raise ValueError("Error! Total of 0 valid constraints defined on verificationSuite.yaml")
    if nInvalidConstraints>0:
        sentence1="Warn: Total of "+str(nInvalidConstraints)+" invalid constraints"
        sentence2=" (out of "+str(nConstraints)+")."
        sentence3=" These constraints will be kept out of process."
        print(sentence1+sentence2+sentence3)
        
    return check

def getVerificationRunBuilder(spark, df, check):
    vRunBuilder = VerificationSuite(spark) \
            .onData(df) \
            .addCheck(check)
    return vRunBuilder

def runCheck(spark, df, constraintParameters):
    check = Check(spark, CheckLevel.Warning, "Review Check")
    check = evaluateContraints(check, constraintParameters)
    vRunBuilder = getVerificationRunBuilder(spark, df, check)
    currResult = vRunBuilder.run()
    
    return currResult

def getCheck(spark, df, vsYamlPath):
    print("getCheck...")
    
    constraintParameters = yVS.getConstraintParameters(vsYamlPath)
    result = runCheck(spark, df, constraintParameters)

    return result
