from sqlalchemy import create_engine

_ENGINES = {}

def get_engine_for_db(db_cfg, db_name):
    key = db_name
    if key not in _ENGINES:
        cred = db_cfg["cred"]
        url = (
            f"mysql+pymysql://{cred['user']}:{cred['password']}"
            f"@{cred['host']}:{cred['port']}/{db_name}"
        )
        _ENGINES[key] = create_engine(url, pool_pre_ping=True)
    return _ENGINES[key]
