import sys

try:
    import slate
    from google.cloud import language
    from sqlalchemy import create_engine, Table, Column, Float, Integer, MetaData
except:
    print "The libs not imported"
    sys.exit(1)

"""
    write to db the score obtained on sentimental analysis
"""


def write_to_db(score):
    """
    Persist the score to db
    :param score:
    :return: None
    """
    engine = create_engine('mysql://root:root123@localhost:3306/test')
    metadata = MetaData(engine, schema="test")
    score_table = Table('score_table', metadata,
                        Column('score', Integer),
                        Column('magnitude', Float))
    score_table.create(engine, checkfirst=True)
    """ insert into the tables """
    insert = score_table.insert()
    insert.execute(score)


"""
    get the score and magnitude of the sentimental analysis
"""


def get_analysis(annotations):
    """
    Get score od the text after analysis
    :param annotations:
    :return: dict
    """
    score = annotations.sentiment.score
    magnitude = annotations.sentiment.magnitude
    return {'score': score, 'magnitude': magnitude}


"""
    Analyse the file for sentiment using google apis
"""


def analyze(file_name):
    """
    analyse the file
    :param file_name:
    :return: annotations
    """
    client = language.Client()
    with open(file_name, "rb") as review_file:
        doc = client.document_from_text(review_file.read())
        annotations = doc.annotate_text(include_sentiment=True,
                                        include_entities=False,
                                        include_syntax=False)
        return get_analysis(annotations)


"""
    Main script starts here
"""

if __name__ == "__main__":
    with open("../in/SIW Issue 1254 06_01_2017.pdf", "rb") as f:
        doc = slate.PDF(f)
        for item in doc:
            with open("../out/doc_to_review.txt", "a") as out:
                out.write(item)
                out.close()
        f.close()
        score = analyze("../out/doc_to_review.txt")
        write_to_db(score)
