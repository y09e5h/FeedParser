import logging

from GlobalConfig import STATIC_VARIABLES

def _apply_filter(row, rule):
    #print(row)
    if not rule or not rule.strip():
        return True    
    try:
        return eval(rule, STATIC_VARIABLES, row.to_dict())
    except Exception as e:
        logging.critical(f"Error evaluating rule '{rule}': {e}")
        return False
def applyEnrichment(df,enrichments):
    #print(df)
    for enrichment in enrichments:

        column=enrichment["column"]
        #print(column)
        filter=enrichment["filter"]
        rule= enrichment["rule"]
        datatype = enrichment["dataType"]
        etype = enrichment["type"]

        if rule is None or rule == "":
            print("Rule not specified")
            return df
        else:
            if etype == "RECORD" :
                is_eligible = df.apply(_apply_filter, args=(filter,), axis=1)
                #print(is_eligible)
                df.loc[is_eligible, column] = df.apply(_apply_filter, args=(rule,), axis=1)
                #print("After enrichment:")
                #print(df)
                if datatype:
                    df[column] = df[column].astype(datatype)
            else :
                pass
    return df