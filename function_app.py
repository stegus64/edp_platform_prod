import azure.functions as func
import logging

from orderupdates_eh_trigger import app as orderupdates_eh_trigger
from timer_trigger import app as timer_trigger

app = func.FunctionApp()

# app.register_functions(timer_trigger)
app.register_functions(orderupdates_eh_trigger)

