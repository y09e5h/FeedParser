import logging
import re
from GlobalConfig import STATIC_VARIABLES
ruleSucess = True
def _apply_filter(row, rule):
    #print(row)
    if not rule or not rule.strip():
        return True    
    try:
        return eval(rule, STATIC_VARIABLES, row.to_dict())
    except Exception as e:
        global ruleSucess
        if ruleSucess:
            logging.critical(f"❌ Error evaluating rule '{rule}': {e}")
            ruleSucess = False
        return False

def getOutput(df, output):
    FilterRule_rule = output['filter']
    global ruleSucess
    ruleSucess = True
    print(FilterRule_rule)
    is_valid = df.apply(_apply_filter, args=(FilterRule_rule,), axis=1)
    df = df.loc[is_valid].copy()
    #df = df.reset_index(drop=True)
    print(df)
    return df