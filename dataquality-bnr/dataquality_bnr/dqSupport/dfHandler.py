def get_keys_list(r1):
    keys_list = list(r1.asDict().keys())

    return keys_list

def get_values_list(r1, keys_list):
    values_list=[]
    for k in keys_list:
        values_list.append(str(r1[k]))

    return values_list

def df_to_list(df):
    rows = df.collect()

    keys_list=get_keys_list(rows[0])

    df_list=[keys_list]
    for r in rows:
        values_list = get_values_list(r, keys_list)
        df_list.append(values_list)

    return df_list