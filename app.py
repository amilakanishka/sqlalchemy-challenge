import numpy as np
import pandas as pd
import datetime as dt

import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func

from flask import Flask, jsonify

#################################################
# Database Setup
#################################################
engine = create_engine("sqlite:///Resources/hawaii.sqlite")

# reflect an existing database into a new model
Base = automap_base()
# reflect the tables
Base.prepare(engine, reflect=True)

# Save reference to the table
Measurement = Base.classes.measurement
Station = Base.classes.station

#################################################
# Flask Setup
#################################################
app = Flask(__name__)

#################################################
# Flask Routes
#################################################

def get_temperature(startDate, endDate):
    startD = dt.datetime.strptime(startDate, '%Y-%m-%d')
    endD = dt.datetime.strptime(endDate, '%Y-%m-%d')    
    session = Session(engine)
    sel = [func.min(Measurement.tobs), \
        func.avg(Measurement.tobs),func.max(Measurement.tobs)]
    results = session.query(*sel).\
        filter(func.datetime(Measurement.date) >= startD).\
        filter(func.datetime(Measurement.date) <= endD).\
        order_by(Measurement.date).all()   
    tobs_list = []
    for i in range(len(results)):
        tobs_list.append({
            'TMIN':results[i][0],\
            'TAVG':results[i][1],\
            'TMAX':results[i][2]})

    return tobs_list


@app.route("/")
def welcome():
    return (
        f"Available Routes:<br/>"
        f'<a href="/api/v1.0/precipitation">/api/v1.0/precipitation</a><br/>'
        f'<a href="/api/v1.0/stations">/api/v1.0/stations</a><br/>'
        f'<a href="/api/v1.0/tobs">/api/v1.0/tobs<a/><br/>'
        f'<a href="/api/v1.0/<start>">/api/v1.0/start (date passed in format YYYY-MM-DD)<a/><br/>'
        f'<a href="/api/v1.0/<start>/<end>">/api/v1.0/start/end (dates passed in format YYYY-MM-DD)<a/><br/>'
    )


@app.route("/api/v1.0/precipitation")
def get_precipitation():
    # Create our session (link) from Python to the DB
    session = Session(engine)

    # Query Measurement date and prcp
    sel = [Measurement.date, Measurement.prcp]
    results = session.query(*sel).all()

    session.close()

    prcp_list =[]
    for i in range(len(results)):
        prcp_list.append({'date':results[i][0],'precipitation':results[i][1]}) 
    return jsonify(prcp_list)


@app.route("/api/v1.0/stations")
def get_stations():
    # Create our session (link) from Python to the DB
    session = Session(engine)

    """Return a list of passenger data including the name, age, and sex of each passenger"""
    # Query all Stations
    results = session.query(Station.station,Station.name).all()

    session.close()

    # Create a dictionary from the row data and append to a list of stations
    all_stations = []
    for i in range(len(results)):
        all_stations.append({'station':results[i][0],'name':results[i][1]})
    
    return jsonify(all_stations)

@app.route("/api/v1.0/tobs")
def get_tobs_most_active():
    # Create our session (link) from Python to the DB
    session = Session(engine)

    measurement_stn_grp = session.query(Measurement.station,func.count(Measurement.station)).\
        group_by(Measurement.station).\
        order_by(func.count(Measurement.station).desc()).all() 

    latest_measure = session.query(Measurement).order_by(Measurement.date.desc()).limit(1).all()
    last_date_list = latest_measure[0].date.split('-')
    last12_start = dt.date(int(last_date_list[0]),int(last_date_list[1]),int(last_date_list[2])) - dt.timedelta(365)

    tob_last12_measure = session.query(Measurement.date,Measurement.station,Measurement.tobs).\
                filter(func.datetime(Measurement.date) > last12_start).\
                filter(Measurement.station == measurement_stn_grp[0][0]).\
                order_by(Measurement.date).all()
   
    session.close()

    # Create a dictionary from the row data and append 
    tobs_list = []
    for i in range(len(tob_last12_measure)):
        tobs_list.append({'date':tob_last12_measure[i][0],\
            'station':tob_last12_measure[i][1],'temperature':tob_last12_measure[i][2]})

    return jsonify(tobs_list)

@app.route("/api/v1.0/<start>")
def get_tobs_start(start):
    
    temp_list = get_temperature(start,'2017-08-23')
    return jsonify(temp_list)

@app.route("/api/v1.0/<start>/<end>")
def get_tobs_start_end(start,end):
    
    temp_list = get_temperature(start,end)
    return jsonify(temp_list)

if __name__ == '__main__':
    app.run(debug=True)

