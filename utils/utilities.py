import yaml
import sqlalchemy as db

def parse_yaml(path):
    """
    Read the yaml file and parse its contents
    :param path: the absoulte path of the file
    :return: the parsed dictionary
    """
    with open(path, 'r') as stream:
        try:
            out_dict = yaml.load(stream)
        except yaml.YAMLError as exc:
            print(exc)
        assert type(out_dict) == dict
        return out_dict

def open_connection(db_engine="MYSQL", db_name="tpch_4g", db_url="localhost"):
    engine = None
    if db_engine == "MYSQL":
        engine = db.create_engine("mysql+msqldb://umesh:*Google@{}/{}".format(db_name))
    elif db_engine == "":
    return None