from sqlalchemy import create_engine
from airflow.models import Variable

#Create psql engine instance
def ps_engine_init(database):
    DB_NAME = database #hint "research_db_v1"
    USER = Variable.get("POSTGRES_USER", default_var="not found")
    PASSWORD = Variable.get("POSTGRES_PASSWORD", default_var="not found")
    HOST = "postgres"
    PORT = "5432"
    return create_engine(f"postgresql+psycopg2://{USER}:{PASSWORD}@{HOST}:{PORT}/{DB_NAME}")