import azure.functions as func
import logging
from shared import *

app = func.Blueprint()

@app.function_name("timer_trigger")
@app.timer_trigger(schedule="0 * * * * *", arg_name="myTimer", run_on_startup=False,
              use_monitor=False) 
def timer_trigger(myTimer: func.TimerRequest) -> None:
    if myTimer.past_due:
        logging.info('The timer is past due!')
        
    import polars as pl

    # Create a simple test DataFrame
    df_test = pl.DataFrame({
        "id": [1, 2, 3],
        "name": ["Alice", "Bob", "Charlie"],
        "age": [25, 30, 35]
    })

    # print(df_test)

    write_delta(df_test.to_arrow(), table="orderupdates_timer", source="Tables")

    logging.info('Python timer trigger function executed.')