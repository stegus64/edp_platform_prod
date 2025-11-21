import azure.functions as func
import logging

from orderupdates_eh_trigger import app as orderupdates_eh_trigger
from orderupdates_bucket_eh_trigger import app as orderupdates_bucket_eh_trigger
from orderupdates_order_eh_trigger import app as orderupdates_order_eh_trigger
from orderupdates_payment_eh_trigger import app as orderupdates_payment_eh_trigger

from timer_trigger import app as timer_trigger

app = func.FunctionApp()

# app.register_functions(timer_trigger)
app.register_functions(orderupdates_eh_trigger)
# app.register_functions(orderupdates_bucket_eh_trigger)
# app.register_functions(orderupdates_order_eh_trigger)
# app.register_functions(orderupdates_payment_eh_trigger)
