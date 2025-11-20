import pandas as pd
from database import warehouse_engine, presentation_engine
from .logger import log_start, log_end


def run_aggregate():
    log_id = log_start("aggregate_presentation")

    try:
        if warehouse_engine is None or presentation_engine is None:
            raise Exception("Database engines not initialized.")
    except Exception as e:
        error_message = f"Error during aggregation: {e}"
        print(error_message)
        if log_id:
            log_end(log_id, 'failed', 0, error_message)
        raise e


if __name__ == "__main__":
    run_aggregate()
