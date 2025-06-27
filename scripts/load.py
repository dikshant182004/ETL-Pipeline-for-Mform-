import sys
from sqlalchemy import create_engine
from etl.logger import logging
from etl.entity.artifact_entity import TransformationArtifact
from etl.exception import ETL_Exception

class PostgresLoader:
    def __init__(self, db_user, db_password, db_host, db_port, db_name):
        try:
            logging.info("Connecting to the postgresql database")

            self.url = f"postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
            self.engine = create_engine(self.url)

        except Exception as e:
            raise ETL_Exception(e,sys)

    def load_dataframe(self, df, table_name):

        df.toPandas().to_sql(table_name, self.engine, if_exists='replace', index=False, method='multi', chunksize=1000)

    def load_all(self, artifact: TransformationArtifact, form_id: str):
        """Loading all the created dataframes in the PostgreSQL"""

        form_info = f"form_{form_id}"

        dataframes = {
            "meta": artifact.form_df,
            "language": artifact.language_df,
            "questions": artifact.question_df,
            "parent": artifact.parent_df,
            "child": artifact.child_df,
            "validation": artifact.validation_df,
            "answer_option": artifact.answer_option_df,
            "range_rule": artifact.range_rule_df,
            "restriction": artifact.restriction_df,
            "resource_url": artifact.resource_url_df,
            "weightage": artifact.weightage_df,
            "get_dynamic_option": artifact.get_dynamic_option_df,
            "get_dynamic_option_mapping": artifact.get_dynamic_option_mapping_df,
            "create_dynamic_option": artifact.create_dynamic_option_df,
            "projects": artifact.projects_df,
        }
        try:
            logging.info("Loading the transformed data for furthur analysis.")

            for table_name, df in dataframes.items():
                if df is not None:
                    table_name = f"{form_info}_{table_name}"
                    self.load_dataframe(df, table_name)

        except Exception as e:
            raise ETL_Exception(e,sys)


