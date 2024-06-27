import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, text, func

import pandas as pd


# The Purpose of this Class is to separate out any Database logic
class SQLHelper():
    #################################################
    # Database Setup
    #################################################

    # define properties
    def __init__(self):
        self.engine = create_engine("sqlite:///Resources/hawaii.sqlite")
        self.Base = None

        # automap Base classes
        self.init_base()

    def init_base(self):
        # reflect an existing database into a new model
        self.Base = automap_base()
        # reflect the tables
        self.Base.prepare(autoload_with=self.engine)

    #################################################
    # Database Queries
    #################################################
    # Gets finds the date 1 year before the last date in the data set
    def year_before_func(self):
        # Save reference to the table
        Measurement = self.Base.classes.measurement

        # Create our session (link)
        session = Session(self.engine)

        # Query all results
        # Finding most recent date in the Measurement data set using ORM
        latest_date = session.query(Measurement.date).order_by(Measurement.date.desc()).first()

        # Making the latest date into a datetime
        latest_date_str = pd.to_datetime(latest_date)

        # Finding the year before of the date using DateOffset (gotten from: https://stackoverflow.com/a/31170136/23471668)
        year_before = latest_date_str - pd.DateOffset(years=1)

        # Convert the year_before DatetimeIndex into a date; code from Xpert
        year_before_str = year_before.strftime('%Y-%m-%d')

        # Close session
        session.close()

        return year_before_str[0]
    
    # Gets all precipitation data from the last year
    def query_precipitation(self):
        # Save reference to the table
        Measurement = self.Base.classes.measurement

        # Create our session (link)
        session = Session(self.engine)

        # Calling the year_before_func() that was defined above
        year_before_str = self.year_before_func()

        # Finding all values in the range year_before to latest_date_str
        results = session.query(Measurement.date, Measurement.prcp).filter(Measurement.date >= year_before_str).all()

        # close session
        session.close()

        df = pd.DataFrame(results)

        # Creating an empty dictionary to house results
        prec_dict = {}

        # Interating over the rows in the new dataframe to fill out the dictionary with help from Xpert
        for index, row in df.iterrows():
            key = row['date']
            value = row['prcp']
            prec_dict[key] = value

        return(prec_dict)
    
    # Returns a list of all the stations
    def query_stations(self):
        # Save reference to the table
        Measurement = self.Base.classes.measurement

        # Create our session (link)
        session = Session(self.engine)

        # Query the stations and then group by the stations to limit the stations listed to the 9 stations where readings were taken
        results = session.query(Measurement.station).group_by(Measurement.station).all()

        # close session
        session.close()

        # Create the DataFrame and turn it into a dictionary
        df = pd.DataFrame(results)
        data = df.to_dict(orient="records")
        return(data)
    
    # Gets the station id for the most active station
    def most_active_station(self):
        # Save reference to the table
        Measurement = self.Base.classes.measurement

        # Create our session (link)
        session = Session(self.engine)

        # Find the most active station
        station_act = session.query(Measurement.station, func.count(Measurement.station)).\
            group_by(Measurement.station).\
            order_by(func.count(Measurement.station).desc()).\
            all()
        
        # station_act[0][0] houses the name of the most active station
        return station_act[0][0]
    
    # Gets dates and temperature observations for the most-active station for the previous year
    def query_tobs(self):
        # Save reference to the table
        Measurement = self.Base.classes.measurement

        # Create our session (link)
        session = Session(self.engine)

        # Find the most active station
        ma_station = self.most_active_station()

        # Calling the year_before_func() that was defined above
        year_before_str = self.year_before_func()

        # Last year precipitation information with both date and temperature
        results = session.query(Measurement.date, Measurement.tobs).filter(Measurement.date >= year_before_str, Measurement.station == ma_station).all()

        # close session
        session.close()

        # Create the DataFrame and turn it into a dictionary
        df = pd.DataFrame(results)
        data = df.to_dict(orient="records")
        return(data)
    
    # Gets the minimum, maximum, and average temperatures for the most-active station from a given date to the most recent date
    def query_tobs_dyn_start(self, start_date):
        # Save reference to the table
        Measurement = self.Base.classes.measurement

        # Create our session (link)
        session = Session(self.engine)

        # Find the most active station
        ma_station = self.most_active_station()

        # Find the temp stats for dates after the start date
        results = session.query(func.min(Measurement.tobs), func.max(Measurement.tobs), func.avg(Measurement.tobs)).\
        filter(Measurement.station == ma_station, Measurement.date >= f'{start_date}').all()

        # close session
        session.close()

        # Set column names:
        cols = [f"min temp from {start_date}", f"max temp from {start_date}", f"avg temp from {start_date}"]

        # Create the DataFrame and turn it into a dictionary
        df = pd.DataFrame(results, columns=cols)
        data = df.to_dict(orient="records")
        return(data)
    
    # Gets the minimum, maximum, and average temperatures for the most-active station between 2 dates (including the 2 dates)
    def query_tobs_dyn_start_end(self, start_date, end_date):
       # Save reference to the table
        Measurement = self.Base.classes.measurement

        # Create our session (link)
        session = Session(self.engine)

        # Find the most active station
        ma_station = self.most_active_station()

        # Find the temp stats for dates between (and including) the start and end dates
        results = session.query(func.min(Measurement.tobs), func.max(Measurement.tobs), func.avg(Measurement.tobs)).\
        filter(Measurement.station == ma_station, Measurement.date >= f'{start_date}', Measurement.date <= f'{end_date}').all()

        # close session
        session.close()

        # Set column names:
        cols = [f"min temp beween {start_date} and {end_date}", f"max temp beween {start_date} and {end_date}", f"avg temp beween {start_date} and {end_date}"]

        # Create the DataFrame and turn it into a dictionary
        df = pd.DataFrame(results, columns=cols)
        data = df.to_dict(orient="records")
        return(data)