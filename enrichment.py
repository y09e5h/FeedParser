import logging
import re
from GlobalConfig import STATIC_VARIABLES

def _apply_filter(row, rule):
    #print(row)
    if not rule or not rule.strip():
        return True    
    try:
        return eval(rule, STATIC_VARIABLES, row.to_dict())
    except Exception as e:
        logging.critical(f"❌ Error evaluating rule '{rule}': {e}")
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
        grpcoltext = enrichment["groupBy"]
        groupCol = enrichment['groupBy'].split(",")


        if rule is None or rule == "":
            logging.info("Rule not specified")
            return df
        else:

            if etype == "RECORD" :
                #print(is_eligible)
                if filter is None or filter == "":
                    df[column] = df.apply(_apply_filter, args=(rule,), axis=1)
                else:
                    is_eligible = df.apply(_apply_filter, args=(filter,), axis=1)
                    df.loc[is_eligible, column] = df.apply(_apply_filter, args=(rule,), axis=1)

            else :
                pattern = r"(?P<func>\w+)\((?P<col>\w+)\)"

                match = re.search(pattern, rule)
                if match:
                    gfunction = match.group('func')
                    fcol = match.group('col')
                    print(fcol)
                    print(gfunction)
                    if filter is None or filter == "":
                        if grpcoltext is None or grpcoltext == "":
                            #print(df)
                            df[column] = getattr(df[fcol], gfunction)()
                        else:
                            #print(df.columns)
                            df[column] = df.groupby(groupCol)[fcol].transform(gfunction)
                    else:
                        is_eligible = df.apply(_apply_filter, args=(filter,), axis=1)
                        if grpcoltext is None or grpcoltext == "":
                            df.loc[is_eligible, column] = getattr(df.loc[is_eligible,fcol], gfunction)()
                            #df.loc[is_eligible, column] = df.loc[is_eligible,fcol].transform(gfunction)
                        else:
                            df.loc[is_eligible, column] = df.loc[is_eligible].groupby(groupCol)[fcol].transform(gfunction)
                else:
                    logging.critical(f"⚠️ Group Function not specified correctly : {rule}")
        if datatype:
            df[column] = df[column].astype(datatype)
    return df