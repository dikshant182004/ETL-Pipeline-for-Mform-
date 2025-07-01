import sys
import urllib.parse
from sqlalchemy import create_engine, text  # using sqlalchemy just to automatically create database 
from sqlalchemy.exc import OperationalError
from etl.logger import logging
from etl.entity.artifact_entity import TransformationArtifact, ClientTransformationArtifact
from etl.exception import ETL_Exception

class PostgresLoader:
    def __init__(self, db_user, db_password, db_host, db_port, db_name):
        try:
            logging.info("Connecting to the PostgreSQL database...")
            self.db_user = db_user              
            self.db_password = db_password       
            self.db_name = db_name
            encoded_password = urllib.parse.quote_plus(db_password)
            self.base_url = f"postgresql+psycopg2://{db_user}:{encoded_password}@{db_host}:{db_port}/"
            default_engine = create_engine(self.base_url, isolation_level="AUTOCOMMIT")

            with default_engine.connect() as conn:
                # check if DB exists, create if not
                result = conn.execute(text(f"SELECT 1 FROM pg_database WHERE datname='{db_name}'"))
                exists = result.scalar()
                if not exists:
                    conn.execute(text(f"CREATE DATABASE \"{db_name}\""))
                    logging.info(f"Database {db_name} created.")
                else:
                    logging.info(f"Database {db_name} already exists.")

            self.url = f"jdbc:postgresql://{db_host}:{db_port}/{db_name}"   # JDBC for writing the data 
            self.engine = create_engine(f"postgresql+psycopg2://{db_user}:{encoded_password}@{db_host}:{db_port}/{db_name}")
            logging.info(f"Connected to {db_name} successfully.")

        except OperationalError as e:
            logging.error("Database connection failed.")
            raise ETL_Exception(e, sys)
        except Exception as e:
            raise ETL_Exception(e, sys)

    def load_dataframe(self, df, table_name):
        try:
            logging.info(f"Loading table '{table_name}' into PostgreSQL.")
            df.write \
                .format("jdbc") \
                .option("url", self.url) \
                .option("dbtable", table_name) \
                .option("user", self.db_user) \
                .option("password", self.db_password) \
                .option("driver", "org.postgresql.Driver") \
                .mode("overwrite") \
                .save()
            
            logging.info(f"Table '{table_name}' loaded successfully.")
        except Exception as e:
            logging.error(f"Failed to load table '{table_name}'.")
            raise ETL_Exception(e, sys)

    def load_all(self, artifact: TransformationArtifact, form_id: str):
        """Loads all transformed form DataFrames into PostgreSQL."""
        try:
            form_info = f"form_{form_id}"
            logging.info(f"Starting data load for form: {form_info}")

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
                "restriction_order": artifact.restriction_order_df,
                "resource_url": artifact.resource_url_df,
                "weightage": artifact.weightage_df,
                "get_dynamic_option": artifact.get_dynamic_option_df,
                "get_dynamic_option_mapping": artifact.get_dynamic_option_mapping_df,
                "create_dynamic_option": artifact.create_dynamic_option_df,
                "projects": artifact.projects_df,
            }

            # if df is not None and not df.rdd.isEmpty():   --> if we want to avoid storage of empty tables
            # well if there is a chance of updation in future we should keep them

            for suffix, df in dataframes.items():
                if df is not None :
                    table_name = f"{form_info}_{suffix}"
                    self.load_dataframe(df, table_name)

            logging.info(f"All tables for form {form_id} loaded successfully.")

        except Exception as e:
            raise ETL_Exception(e, sys)
        
    def load_client_all(self, artifact: ClientTransformationArtifact, form_id: str):
        """Loads client form Dataframes into PostgreSQL."""

        try:
            form_info = f"clientform_{form_id}"
            logging.info(f"Starting data load for client form: {form_info}")

            dataframes = {
                "meta": artifact.clientform_df,
                "question": artifact.question_df,
                "initial_answer": artifact.initial_answer_df,
                "answer": artifact.answer_df,
                "reference": artifact.reference_df,
                "location": artifact.location_df
            }

            for suffix, df in dataframes.items():
                if df is not None:
                    table_name = f"{form_info}_{suffix}"
                    self.load_dataframe(df, table_name)

            logging.info(f"All client form tables for {form_id} loaded successfully.")

        except Exception as e:
            raise ETL_Exception(e, sys)
