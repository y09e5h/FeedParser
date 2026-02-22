import logging
from itertools import islice
from GlobalConfig import STATIC_VARIABLES
#from GlobalConfig import _apply_filter
def _apply_rule(column,data, rule):
    rule = rule.strip()
    if not rule:
        return data
    try:
        result = eval(rule, STATIC_VARIABLES, {column:data})
        return result
    except Exception as e:
        logging.critical(f"Error evaluating rule '{rule}': {e}")
        return data

def _apply_filter(localvars, rule):
    if not rule or not rule.strip():
        return True    
    try:
        return eval(rule, STATIC_VARIABLES, localvars)
    except Exception as e:
        logging.critical(f"Error evaluating rule '{rule}': {e}")
        return False

def staticGenerator(fileName,fileType,staticColumns):
    if fileType == 'CSV' or fileType == 'TXT' :
        try:
            with open(fileName,'r') as f:
                i=1
                for staticColumn in staticColumns:

                    static_value=""
                    column = staticColumn["column"]
                    filter_str = staticColumn["filter"]
                    resultIndex = int(staticColumn["resultIndex"])
                    if staticColumn["rowNumber"] :
                        n = int(staticColumn["rowNumber"])
                        static_value = next(islice(f,n,n+1),None)
                    else:
                        f.seek(0)
                        result=[]
                        for line in f:
                            if _apply_filter({column: line}, filter_str):
                                result.append(line)
                        static_value = result[resultIndex] if len(result) > resultIndex else None
                    if static_value:
                        STATIC_VARIABLES[column] = _apply_rule(column, static_value, staticColumn["rule"])
                    else:
                        STATIC_VARIABLES[column] = ""
        except FileNotFoundError as e:
            logging.critical(e)
                
        