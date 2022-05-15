import yaml
from yaml.loader import SafeLoader


#########################################################################
#                   Complex Analyzers
#########################################################################

def get_analyzer_method(analyzer_dict):
    analyzer_method=""

    if "analyzer_method" in analyzer_dict:
        analyzer_method=analyzer_dict["analyzer_method"]
        analyzer_method=analyzer_method.strip("()")
        
        return analyzer_method
    else:
        errorMsg = """
        AnomalyDetection structure not recognized.
        Analyzer description is missing the 'analyzer_method' parameter.
        """
        raise ValueError(errorMsg)
        
    return

# Currently, get_parameters_sentence is only mastered to Compliance Analyzer,
#but it can be mirrored when writing to new complex analyzers
def get_parameters_sentence(analyzer_dict):
    all_params=[]
    
    analyzer_params = dict(analyzer_dict)
    analyzer_params.pop("analyzer_method")

    for param in analyzer_params:
        param_name=param
        param_value='\"'+analyzer_dict[param_name]+'\"'
        param_setter=param_name+"="+param_value
        all_params.append(param_setter)
        
    parameters_sentence = ""
    for p in all_params:
        if parameters_sentence=="":
            parameters_sentence = p
        else:
            parameters_sentence=parameters_sentence+", "+p
    
    print(parameters_sentence)
    return parameters_sentence
        

def buildComplexAnalyzer(analyzer_dict):
    analyzer_method = get_analyzer_method(analyzer_dict)
    parameters_sentence = get_parameters_sentence(analyzer_dict)
    
    the_analyzer = analyzer_method+"("+parameters_sentence+")"
    return the_analyzer


#########################################################################
#                   Complex Check
#########################################################################

def get_check_method(check_dict):
    check_method=""

    if "check_method" in check_dict:
        check_method=check_dict["check_method"]
        check_method=check_method.strip("()")
        
        return check_method
    else:
        errorMsg = """
        Constraint structure not recognized.
        addConstraint description is missing the 'check_method' parameter.
        """
        raise ValueError(errorMsg)
        
    return

# Currently, get_parameters_sentence is only mastered to Compliance Analyzer,
#but it can be mirrored when writing to new complex analyzers
def get_check_parameters_sentence(check_dict):
    """
    Concatenate all parameters from a CheckDefinedAsDictionary into a single string sentence.
    One inportant step of this sentence builder is to understand whether the check parameter must to be a string or not.
    'assertion' is such a case where it comes as a string but Check __init__() function must to receives it as raw code.
    """
    all_params=[]
    
    check_params = dict(check_dict)
    check_params.pop("check_method")

    for param in check_params:
        param_name=param
        param_value=check_dict[param_name]
        
        if param_name!="assertion":
            param_value='\"'+param_value+'\"'
            
        param_setter=param_name+"="+param_value
        all_params.append(param_setter)
        
    parameters_sentence = ""
    for p in all_params:
        if parameters_sentence=="":
            parameters_sentence = p
        else:
            parameters_sentence=parameters_sentence+", "+p
    
    return parameters_sentence
        

def buildComplexCheck(check_dict):
    check_method = get_check_method(check_dict)
    parameters_sentence = get_check_parameters_sentence(check_dict)
    
    the_analyzer = check_method+"("+parameters_sentence+")"
    return the_analyzer

#########################################################################
#                   Configs from vsYmal
#########################################################################

def loadYamlList(yaml_path):
    yamlData_list = []
    with open(yaml_path, 'r') as f:
        yamlData_list = list(yaml.load_all(f, Loader=SafeLoader))[0]
    return yamlData_list

def getResultKeyParameters(yamlData_list): #OK
    patternRecognized = False
    if "ResultKey" in yamlData_list:
        resultKeyDict = yamlData_list["ResultKey"]
        if "key_tags" in resultKeyDict:
            patternRecognized = True
            return resultKeyDict
        
        if patternRecognized == False:
            raise ValueError("ResultKey structure not recognized")
        
    else:
        mock_resultKeyParameters = {"key_tags":{}}
        return mock_resultKeyParameters
        
def getCheckParameters(yamlData_list):  #OK
    patternRecognized = False
    if "Check" in yamlData_list:
        checkDict = yamlData_list["Check"]
        
        if "forceApprove" in checkDict:
            if type(checkDict["forceApprove"])!=bool:
                raise ValueError("Check structure not recognized. forceApprove param must be either True or False")
        else:
            checkDict["forceApprove"]=False
                
        if "level" in checkDict:
            if "description" in checkDict:
                patternRecognized = True
                return checkDict
            
    if patternRecognized == False:
        raise ValueError("Check structure not recognized")
        
def getCheckLevel(s):  #OK
    s = s.replace("CheckLevel.", "")
    s = s.lower()
    
    if s=="warning":
        return CheckLevel.Warning
    elif s=="error":
        return CheckLevel.Error
    else:
        raise ValueError("CheckLevel must be Warning or Error")
        
def getCheckLevelAsString(s):
    s = s.replace("CheckLevel.", "")
    s = s.lower()
    
    if s=="warning":
        return "Warning"
    elif s=="error":
        return "Error"
    else:
        raise ValueError("CheckLevel must be Warning or Error")
        
def getCheckDescription(s): #OK
    return str(s)

def getVerificationSuiteConfigs(yaml_path):
    yamlData_list = loadYamlList(yaml_path)
    
    checkParameters = getCheckParameters(yamlData_list)
    resultKeyParameters = getResultKeyParameters(yamlData_list)
    
    verificationSuiteConfigs={
        "checkParameters":checkParameters,
        "resultKeyParameters":resultKeyParameters
    }
    
    return verificationSuiteConfigs

#########################################################################
#                   AnomalyDetection from vsYmal
#########################################################################

def evaluateADPattern(yamlData_list):
    approvedPattern = True
    adParameters=[]
    
    if "AnomalyDetection" in yamlData_list:
        neededParameters = ["strategy","analyzer"]
        
        adParameters = yamlData_list["AnomalyDetection"]
        for mainParameter in adParameters:
            if "addAnomalyCheck" in mainParameter:
                addAnomalyCheck = mainParameter["addAnomalyCheck"]
                
                for parameter in neededParameters:
                    if parameter not in addAnomalyCheck:
                        approvedPattern = False
            else:
                approvedPattern = False
    else:
        approvedPattern = False
        
    return approvedPattern

def getADParameters(yaml_path):
    yamlData_list = loadYamlList(yaml_path)
    approvedPattern = evaluateADPattern(yamlData_list)
                        
    if approvedPattern==True:
        adParameters = yamlData_list["AnomalyDetection"]
        for i, mainParameter in enumerate(adParameters):
            
            addAnomalyCheck = adParameters[i]["addAnomalyCheck"]
            if type(addAnomalyCheck["analyzer"])==dict:
                complex_analyzer = buildComplexAnalyzer(addAnomalyCheck["analyzer"])
                adParameters[i]["addAnomalyCheck"]["analyzer"] = complex_analyzer 
                
        return adParameters
        
    elif approvedPattern == False:
        raise ValueError("AnomalyDetection structure not recognized.")


#########################################################################
#                   Check from vsYmal
#########################################################################

def evaluateCheckPattern(yamlData_list):
    approvedPattern = False
    if "Constraints" in yamlData_list:
        constraintDict = yamlData_list["Constraints"]
        
        allConstraints = True
        for d in constraintDict:
            if list(d.keys())[0] != "addConstraint":
                allConstraints = False
        
        if allConstraints==True:
            approvedPattern = True
        
    return approvedPattern
        
def getConstraintParameters(yaml_path):
    yamlData_list = loadYamlList(yaml_path)
    
    approvedPattern = evaluateCheckPattern(yamlData_list)    
    
    if approvedPattern==True:
        constraintDict = yamlData_list["Constraints"]
        
        for i, d in enumerate(constraintDict):
            current_addConstraint=constraintDict[i]["addConstraint"]
            if type(current_addConstraint)==dict:
                complex_check = buildComplexCheck(current_addConstraint)
                constraintDict[i]["addConstraint"] = complex_check
         
        return constraintDict
        
    elif approvedPattern == False:
        raise ValueError("Constraint structure not recognized.")
        

def getConstraintParameters2(yaml_path):
    yamlData_list = loadYamlList(yaml_path)
    
    patternRecognized = False
    if "Constraints" in yamlData_list:
        constraintDict = yamlData_list["Constraints"]

        allConstraints = True
        for d in constraintDict:
            if list(d.keys())[0] != "addConstraint":
                allConstraints = False
        
        if allConstraints==True:
            patternRecognized = True
            return constraintDict
        
    if patternRecognized == False:
        raise ValueError("Constraint structure not recognized")
        
        
#########################################################################
#                   Template of vsYmal
#########################################################################       


def build_check_template():
    default_pattern = """
Check: {
    level: Error,
    description: 'CheckObject by yaml File'
}
"""
    return default_pattern

def build_resultKey_template():
    default_pattern = """
ResultKey: {
    key_tags: {
        tag1: 1,
        tag2: 2,
        tag3: 3
    } 
}
"""
    return default_pattern

def build_constraints_template(suggestions_df):
    constraints = (suggestions_df
                   .orderBy("column_name")
                   .select("code_for_constraint")).collect()
    
    break_line = "\r\n"
    constraints_list = ""
    open_tag = break_line + "Constraints: ["
    close_tag = break_line + "]"
    
    for row in constraints:
        constraint = str(row[0][1:])
        current_addConstraint = break_line + "    addConstraint: '" + constraint+ "',"
        constraints_list = constraints_list + current_addConstraint
        
    vsYaml_content = open_tag+constraints_list+close_tag
    return vsYaml_content


def printTemplate(suggestions_df):
    check_content=build_check_template()
    resultKey_content=build_resultKey_template()
    constraints_content=build_constraints_template(suggestions_df)
    vsYaml_template = check_content+resultKey_content+constraints_content
    
    print(vsYaml_template)





        
        