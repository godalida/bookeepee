import dlt
import toml
import subprocess
import logging
from dlt.common import pendulum
from dlt.common.data_writers import TDataItemFormat
from dlt.common.pipeline import LoadInfo
from dlt.common.typing import TDataItems
from dlt.pipeline.pipeline import Pipeline
import os
from dotenv import load_dotenv

load_dotenv()  # Load environment variables from .env file

credentials_path='../credentials.json'
os.environ['GOOGLE_APPLICATION_CREDENTIALS']=credentials_path

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Load secrets for BigQuery configuration
secrets = toml.load('.dlt/secrets.toml')

# As this pipeline can be run as standalone script or as part of the tests, we need to handle the import differently.
try:
    from .mongodb import mongodb, mongodb_collection  # type: ignore
except ImportError:
    from mongodb import mongodb, mongodb_collection

def transform_columns(data, column_mapping):
    """Transform column names based on the provided mapping."""
    return [{column_mapping.get(k, k): v for k, v in item.items()} for item in data]


def load_select_collection_db(pipeline: Pipeline = None) -> LoadInfo:
    """Use the mongodb source to reflect an entire database schema and load select tables from it.

    This example sources data from a sample mongo database data from [mongodb-sample-dataset](https://github.com/neelabalan/mongodb-sample-dataset).
    """
    if pipeline is None:
        # Create a pipeline
        pipeline = dlt.pipeline(
            pipeline_name="mongodb_pipeline",
            destination='bigquery',
            dataset_name="mongo_select",
        )

    # Configure the source to load a few select collections incrementally
    mflix = mongodb(incremental=dlt.sources.incremental("date")).with_resources(
        "comments"
    )

    # Run the pipeline. The merge write disposition merges existing rows in the destination by primary key
    info = pipeline.run(mflix, write_disposition="merge")

    return info


def load_select_collection_db_items(parallel: bool = False) -> TDataItems:
    """Get the items from a mongo collection in parallel or not and return a list of records"""
    comments = mongodb(
        incremental=dlt.sources.incremental("date"), parallel=parallel
    ).with_resources("comments")
    return list(comments)

# 
def load_select_collection_user(pipeline: Pipeline = None) -> LoadInfo:
    """Get the user collection and load it to BigQuery."""

    if pipeline is None:
        # Create a pipeline
        pipeline = dlt.pipeline(
            pipeline_name="mongodb_pipeline",
            destination='bigquery',
            dataset_name="bookipi_mongodb_ingested",
        )

    user = mongodb().with_resources("user")
    
    # Define a mapping for user collection
    user_mapping = {
        "_id" : "id",
        "e": "email",
        "dci": "company_id",
        "f" : "first_name",
        "l" : "last_name",
    }

    user = transform_columns(user, user_mapping)
    
    # Keep all columns from the original data and apply mapping for known columns
    user = [
        {**item, **{k: item[k] for k in user_mapping if k in item}} 
        for item in user
    ]  # Keep all columns, applying mapping for known ones
    
    # Create a resource with a name for the transformed user data
    resource = dlt.resource(user, name="user")
    
    # Write the transformed user data to BigQuery
    info = pipeline.run(resource, write_disposition="replace")
    return info

def load_select_collection_company(pipeline: Pipeline = None) -> LoadInfo:
    """Get the Company collection and load it to BigQuery."""

    if pipeline is None:
        # Create a pipeline
        pipeline = dlt.pipeline(
            pipeline_name="mongodb_pipeline",
            destination='bigquery',
            dataset_name="bookipi_mongodb_ingested",
        )

    company = mongodb().with_resources("company")
    
    # Define a mapping for Company collection
    company_mapping = {
        "_id" : "id",
        "owner": "owner",
        "c": "company_name",
        "co" : "country",
        "st" : "state",
        "cr" : "currency",
        "e" : "email",
    }

    company = transform_columns(company, company_mapping)
    
    # Keep all columns from the original data and apply mapping for known columns
    company = [
        {**item, **{k: item[k] for k in company_mapping if k in item}} 
        for item in company
    ]  # Keep all columns, applying mapping for known ones
    
    # Create a resource with a name for the transformed user data
    resource = dlt.resource(company, name="company")
    
    # Write the transformed user data to BigQuery
    info = pipeline.run(resource, write_disposition="replace")
    return info

def load_select_collection_invoice(pipeline: Pipeline = None) -> LoadInfo:
    """Get the invoice collection and load it to BigQuery."""

    if pipeline is None:
        # Create a pipeline
        pipeline = dlt.pipeline(
            pipeline_name="mongodb_pipeline",
            destination='bigquery',
            dataset_name="bookipi_mongodb_ingested",
        )

    invoice = mongodb().with_resources("invoice")
    
    # Define a mapping for invoice collection
    invoice_mapping = {
        "_id": "id",
        "e": "email",
        "ci": "company_id",
        "it": "items",  # Keep as array
        "pm": "payments",  # Keep as array
        "nt": "note"
    }

    invoice = transform_columns(invoice, invoice_mapping)
    
    # Keep all columns from the original data and apply mapping for known columns
    invoice = [
        {**item, **{k: item[k] for k in invoice_mapping if k in item}} 
        for item in invoice
    ]  # Keep all columns, applying mapping for known ones, preserving arrays
    
    # Create a resource with a name for the transformed invoice data
    resource = dlt.resource(invoice, name="invoice")
    
    # Write the transformed invoice data to BigQuery
    info = pipeline.run(resource, write_disposition="replace")
    return info

def load_select_collection_subscription(pipeline: Pipeline = None) -> LoadInfo:
    """Get the subscription collection and load it to BigQuery."""

    if pipeline is None:
        # Create a pipeline
        pipeline = dlt.pipeline(
            pipeline_name="mongodb_pipeline",
            destination='bigquery',
            dataset_name="bookipi_mongodb_ingested",
        )

    subscription = mongodb().with_resources("subscription")
    
    # Define a mapping for subscription collection
    subscription_mapping = {
        "_id": "id",
        "companyId": "company_id",
        "status": "status",
        "subType": "sub_type",
        "billingPeriod": "billing_period",
        "startDate": "start_date",
        "endDate": "end_date",
        "nextBillingDate": "next_billing_date"
    }

    subscription = transform_columns(subscription, subscription_mapping)
    
    # Keep all columns from the original data and apply mapping for known columns
    subscription = [
        {**item, **{k: item[k] for k in subscription_mapping if k in item}} 
        for item in subscription
    ]  # Keep all columns, applying mapping for known ones
    
    # Create a resource with a name for the transformed invoice data
    resource = dlt.resource(subscription, name="subscription")
    
    # Write the transformed invoice data to BigQuery
    info = pipeline.run(resource, write_disposition="replace")
    return info


def load_select_collection_subscription_payments(pipeline: Pipeline = None) -> LoadInfo:
    """Get the subscription collection and load it to BigQuery."""

    if pipeline is None:
        # Create a pipeline
        pipeline = dlt.pipeline(
            pipeline_name="mongodb_pipeline",
            destination='bigquery',
            dataset_name="bookipi_mongodb_ingested",
        )

    subscription_payments = mongodb().with_resources("subscription_payments")
    
    # Define a mapping for subscription collection
    subscription_payments_mapping = {
        "_id": "id",
        "subscriptionId": "subscription_id",
        "status": "status",
        "failedReason": "failed_reason",
        "date": "date",
        "billingPeriod": "billing_period",
        "amount": "amount",
        "tax": "tax",
        "totalExcludingTax": "total_excluding_tax"
    }

    subscription_payments = transform_columns(subscription_payments, subscription_payments_mapping)
    
    # Keep all columns from the original data and apply mapping for known columns
    subscription_payments = [
        {**item, **{k: item[k] for k in subscription_payments_mapping if k in item}} 
        for item in subscription_payments
    ]  # Keep all columns, applying mapping for known ones
    
    # Create a resource with a name for the transformed invoice data
    resource = dlt.resource(subscription_payments, name="subscription_payments")
    
    # Write the transformed invoice data to BigQuery
    info = pipeline.run(resource, write_disposition="replace")
    return info



def load_select_collection_db_items_parallel(
    data_item_format: TDataItemFormat, parallel: bool = False
) -> TDataItems:
    comments = mongodb_collection(
        incremental=dlt.sources.incremental("date"),
        parallel=parallel,
        data_item_format=data_item_format,
        collection="comments",
    )
    return list(comments)


def load_select_collection_db_filtered(pipeline: Pipeline = None) -> LoadInfo:
    """Use the mongodb source to reflect an entire database schema and load select tables from it.

    This example sources data from a sample mongo database data from [mongodb-sample-dataset](https://github.com/neelabalan/mongodb-sample-dataset).
    """
    if pipeline is None:
        # Create a pipeline
        pipeline = dlt.pipeline(
            pipeline_name="mongodb_pipeline",
            destination='bigquery',
            dataset_name="mongo_select_incremental",
        )

    # Configure the source to load a few select collections incrementally
    movies = mongodb_collection(
        collection="movies",
        incremental=dlt.sources.incremental(
            "lastupdated", initial_value=pendulum.DateTime(2016, 1, 1, 0, 0, 0)
        ),
    )

    # Run the pipeline. The merge write disposition merges existing rows in the destination by primary key
    info = pipeline.run(movies, write_disposition="merge")

    return info


def load_select_collection_hint_db(pipeline: Pipeline = None) -> LoadInfo:
    """Use the mongodb source to reflect an entire database schema and load select tables from it.

    This example sources data from a sample mongo database data from [mongodb-sample-dataset](https://github.com/neelabalan/mongodb-sample-dataset).
    """
    if pipeline is None:
        # Create a pipeline
        pipeline = dlt.pipeline(
            pipeline_name="mongodb_pipeline",
            destination='bigquery',
            dataset_name="mongo_select_hint",
        )

    # Load a table incrementally with append write disposition
    # this is good when a table only has new rows inserted, but not updated
    airbnb = mongodb().with_resources("listingsAndReviews")
    airbnb.listingsAndReviews.apply_hints(
        incremental=dlt.sources.incremental("last_scraped")
    )

    info = pipeline.run(airbnb, write_disposition="append")

    return info


def load_entire_database(pipeline: Pipeline = None) -> LoadInfo:
    """Use the mongo source to completely load all collection in a database"""
    if pipeline is None:
        # Create a pipeline
        pipeline = dlt.pipeline(
            pipeline_name="mongodb_pipeline",
            destination='bigquery',
            dataset_name="bookipi_mongodb",
        )

    # By default the mongo source reflects all collections in the database
    source = mongodb()

    # Run the pipeline. For a large db this may take a while
    info = pipeline.run(source, write_disposition="replace")

    return info

def load_collection_with_arrow(pipeline: Pipeline = None) -> LoadInfo:
    """
    Load a MongoDB collection, using Apache
    Error as the data processor.
    """
    if pipeline is None:
        # Create a pipeline
        pipeline = dlt.pipeline(
            pipeline_name="mongodb_pipeline",
            destination='bigquery',
            dataset_name="mongo_select_incremental",
            full_refresh=True,
        )

    # Configure the source to load data with Arrow
    comments = mongodb_collection(
        collection="comments",
        incremental=dlt.sources.incremental(
            "date",
            initial_value=pendulum.DateTime(
                2005, 1, 1, tzinfo=pendulum.timezone("UTC")
            ),
            end_value=pendulum.DateTime(2005, 6, 1, tzinfo=pendulum.timezone("UTC")),
        ),
        data_item_format="arrow",
    )

    info = pipeline.run(comments)
    return info


def parse_billing_period_pipeline():
    """Parse billingPeriod attribute in Subscription Payments table from BigQuery."""
    project_id = os.getenv('PROJECT_ID')  # Update to use environment variable
    dataset = os.getenv('DATASET')  # Update to use environment variable
    input_table = os.getenv('INPUT_TABLE')  # Update to use environment variable
    output_table = os.getenv('OUTPUT_TABLE')  # Update to use environment variable
    
    logging.info("Starting to parse billing period from BigQuery.")
    
    try:
        # Call the PySpark script
        subprocess.run([
            "spark-submit",
            "--packages", "com.google.cloud.spark:spark-3.5-bigquery:0.40.0",
            "pyspark_scripts/parse_billing_period.py",
            project_id,
            dataset,
            input_table,
            output_table
        ], check=True)
        logging.info("Successfully parsed billing period and wrote to BigQuery.")
    except subprocess.CalledProcessError as e:
        logging.error(f"Error occurred while running the PySpark script: {e}")
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")

if __name__ == "__main__":
    # Load all tables from the database.
    print(load_select_collection_user())
    logging.info("Successfully ingested User table from MongoDB to BigQuery!")
    print(load_select_collection_company())
    logging.info("Successfully ingested Company table from MongoDB to BigQuery!")
    print(load_select_collection_invoice())
    logging.info("Successfully ingested Invoice table from MongoDB to BigQuery!")
    print(load_select_collection_subscription())
    logging.info("Successfully ingested Subscription table from MongoDB to BigQuery!")
    print(load_select_collection_subscription_payments())
    logging.info("Successfully ingested Subscription Payments table from MongoDB to BigQuery!")
    logging.info("Successfully ingested Collections from MongoDB to BigQuery!")
    parse_billing_period_pipeline()

    #print(load_entire_database())
    #logging.info("Successfully ingested data from MongoDB to BigQuery!")
    # Parse billingPeriod from Subscription Payments and write to BigQuery
    # Load data with Apache Arrow.
    # print(load_collection_with_arrow())