import logging

from GlobalConfig import STATIC_VARIABLES

def _apply_filter(row, rule):
    if not rule or not rule.strip():
        return False    
    try:
        return eval(rule, STATIC_VARIABLES, row.to_dict())
    except Exception as e:
        logging.critical(f"Error evaluating rule '{rule}': {e}")
        return False

def applyDiscard(df, discards):
    for discard_rule in discards:
        is_discarded = df.apply(_apply_filter, args=(discard_rule,), axis=1)
        df = df.loc[~is_discarded].copy()     
    return df
