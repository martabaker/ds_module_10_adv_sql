# Import the dependencies.
import numpy as np
import pandas as pd
from flask import Flask, jsonify
from sqlHelper import SQLHelper
import datetime as dt

#################################################
# Database Setup
#################################################
# This is done in the sqlHelper script
# Work is done in a separate script to separate interactive layer from queries into the database

#################################################
# Flask Setup
#################################################
app = Flask(__name__)
sql = SQLHelper()

#################################################
# Flask Routes
#################################################
@app.route("/")
def welcome():
    """List all available api routes."""
    return (
        f"Available Routes:<br/>"
        f"/api/v1.0/precipitation<br/>"
        f"/api/v1.0/stations<br/>"
        f"/api/v1.0/tobs<br/>"
        f"/api/v1.0/2016-08-23<br/>"
        f"/api/v1.0/2015-04-01/2016-04-01<br/>"
    )

@app.route("/api/v1.0/precipitation")
def precipitation():
    data = sql.query_precipitation()
    return(jsonify(data))

@app.route("/api/v1.0/stations")
def stations():
    data = sql.query_stations()
    return(jsonify(data))

@app.route("/api/v1.0/tobs")
def tobs():
    data = sql.query_tobs()
    return(jsonify(data))

@app.route("/api/v1.0/<start_date>")
def tobs_dyn_start(start_date):
    data = sql.query_tobs_dyn_start(start_date)
    return(jsonify(data))

@app.route("/api/v1.0/<start_date>/<end_date>")
def tobs_dyn_start_end(start_date, end_date):
    data = sql.query_tobs_dyn_start_end(start_date, end_date)
    return(jsonify(data))

if __name__ == '__main__':
    app.run(debug=True)