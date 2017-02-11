import sys

try:
    from tabula import read_pdf
    from sqlalchemy import create_engine
    import pandas as pd
except:
    print "The libs are not available in the env"
    sys.exit(1)

"""
    Persist to database
"""


def write_to_db(df,table_name):
    """
    write to mysql db
    :param df:
    :param table_name:
    :return: None
    """
    engine = create_engine('mysql://root:root123@localhost:3306/test')
    df.to_sql(table_name, engine, if_exists='replace', index=True, index_label=None, chunksize=100)

"""
    Clean up the dataframe
"""


def clean_df(df):
    """
    clean dataframe
    :param df:
    :return: df
    """
    cols_to_del = []
    for x, y in enumerate(list(df)):
        if "Unnamed" in y:
            col_num = x
            cols_to_del.append(y)
            if col_num != 0:
                column1 = list(df)[col_num - 1]
                column2 = list(df)[col_num]
            elif col_num == 0 and df.shape[1] > 1:
                column1 = list(df)[col_num + 1]
                column2 = list(df)[col_num]
            elif col_num == 0 and df.shape[1] == 1:
                df.columns = ['default']
            df[column1].fillna(df[column2], inplace=True)
    for col in cols_to_del:
        del (df[col])
    df.rename(columns=lambda c: c.replace(" ", "_").replace(",", ""), inplace=True)

    """
        Setting empty as NULL
    """
    df = df.where(pd.notnull(df), "NULL")

    """
        Remove white space inner func
    """

    def remove_whitespace(t):
        if isinstance(t, basestring):
            t = t.strip()
        else:
            t = ""
        return t

    for c in list(df):
        df[c] = df[c].astype(str)
        df[c] = df[c].apply(remove_whitespace)

    return df


"""
    Write to csv
"""


def write_df_to_csv(df, file_name):
    """
    write to
    :param df:
    :param file_name:
    :return:
    """
    df.to_csv(file_name, sep='|', encoding='utf-8')


"""
    Convert pdf to dataframe
"""


def read_table_from_pdf(pdf):
    """
    read table from pdf
    :param pdf:
    :return: data frame
    """
    try:
        df = read_pdf(pdf, pages="1", guess=False)
        return df
    except UnicodeDecodeError:
        return None


"""
    Main script starts here
"""
if __name__ == "__main__":
    df = read_pdf("../in/doc_9.pdf", pages="1", guess=True)
    df_to_write = clean_df(df)
    write_to_db(df_to_write, "shipping_Details")
