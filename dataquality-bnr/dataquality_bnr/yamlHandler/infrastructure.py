import yaml
from yaml.loader import SafeLoader



#########################################################################
#                   Configs from infraYaml
#########################################################################

def loadYamlList(yaml_path):
    yamlData_list = []
    with open(yaml_path, 'r') as f:
        yamlData_list = list(yaml.load_all(f, Loader=SafeLoader))[0]
    return yamlData_list

def containsPersRepPath(yamlData_list):
    containsPRPath=False
    if "persistentRepository" in yamlData_list:
        pR=yamlData_list["persistentRepository"]
        if "hdfsPath" in pR:
            if pR["hdfsPath"]!=None:
                containsPRPath=True
    if containsPRPath==False:
        raise ValueError("infraYaml. No persistentRepository was found")

def containsTempRepPath(yamlData_list):
    containsTRPath=False
    if "temporaryRepository" in yamlData_list:
        pR=yamlData_list["temporaryRepository"]
        if "hdfsPath" in pR:
            if pR["hdfsPath"]!=None:
                containsTRPath=True
                
    if containsTRPath==False:
        raise ValueError("infraYaml. No temporaryRepository was found")
        
        
def evaluatePersRepository(yamlData_list):
    evaluated=False
    if "persistentRepository" in yamlData_list:
        pR=yamlData_list["persistentRepository"]
        if "maxSize" in pR:
            evaluated=True
    
    if evaluated==False:
        raise ValueError("infraYaml. persistentRepository out of pattern")
        
def evaluateTempRepository(yamlData_list):
    evaluated=False
    if "temporaryRepository" in yamlData_list:
        tR=yamlData_list["temporaryRepository"]
        if "maxSize" in tR:
            if "filters" in tR:
                evaluated=True
    
    if evaluated==False:
        raise ValueError("infraYaml. temporaryRepository out of pattern")
    

def evaluatePattern_AD(yamlData_list):
    evaluateTempRepository(yamlData_list)
    evaluatePersRepository(yamlData_list)
    
def evaluatePattern_Check(yamlData_list):
    evaluatePersRepository(yamlData_list)
    
def evaluatePattern(infraYaml, vsType, viewName):
    yamlData_list = loadYamlList(infraYaml)
    
    if vsType=="AnomalyDetection":
        evaluatePattern_AD(yamlData_list)
        #print("AnomalyDetection pattern evaluated.")
        print(viewName+" pattern evaluated.")
    elif vsType=="Check":   
        evaluatePattern_Check(yamlData_list)
        #print("Check pattern evaluated.")
        print(viewName+" pattern evaluated.")
    else:
        raise ValueError("Invalid vsType. It should be either 'AnomalyDetection' or 'Check'")
        
    return yamlData_list
        

###################################################################
#
###################################################################

def getPersistentPath(infraYaml):
    yamlData_list = loadYamlList(infraYaml)
    persistentPath = yamlData_list["persistentRepository"]["hdfsPath"]
    return persistentPath

def getTemporaryPath(infraYaml):
    yamlData_list = loadYamlList(infraYaml)
    temporaryPath = yamlData_list["temporaryRepository"]["hdfsPath"]
    return temporaryPath

def getDefinedFilters(infraYaml):
    yamlData_list = loadYamlList(infraYaml)
    #this selection should be a method
    maxSize = yamlData_list["temporaryRepository"]["maxSize"]
    lastNDays = yamlData_list["temporaryRepository"]["filters"]["lastNDays"]
    onlySucceed = yamlData_list["temporaryRepository"]["filters"]["onlySucceed"]

    filters={"lastNDays":lastNDays,
             "onlySucceed":str(onlySucceed),
             "maxSize":maxSize,}
    return filters
    