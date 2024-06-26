import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, text, func

import pandas as pd
import numpy as np

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

    def query_passengers_orm(self):
        # Save reference to the table
        Passenger = self.Base.classes.passenger

        # Create our session (link)
        session = Session(self.engine)

        # Query all results
        results = session.query(Passenger.name, Passenger.age, Passenger.sex).all()

        # close session
        session.close()

        df = pd.DataFrame(results)
        data = df.to_dict(orient="records")
        return(data)
    
    # same thing with raw SQL
    def query_passengers_raw(self):

        # Query all results
        query = "Select name, age, sex from passenger;"

        df = pd.read_sql(text(query), con = self.engine)
        data = df.to_dict(orient="records")
        return(data)
    
    def query_dynamic_orm(self, min_age, gender):
        # Save reference to the table
        Passenger = self.Base.classes.passenger

        # Create our session (link)
        session = Session(self.engine)

        # Query all results
        results = session.query(Passenger.name, Passenger.age, Passenger.sex).filter(Passenger.age >= min_age).filter(Passenger.sex == gender).order_by(Passenger.name).all()

        # close session
        session.close()

        df = pd.DataFrame(results)
        data = df.to_dict(orient="records")
        return(data)
    
    def query_dynamic_raw(self, min_age, gender):

        # Query all results
        query = f"""
                Select
                    name,
                    age,
                    sex
                From
                    passenger
                Where
                    age >= {min_age}
                    AND sex = '{gender}'
                Order by
                    name asc
                """
        df = pd.read_sql(text(query), con = self.engine)
        data = df.to_dict(orient="records")
        return(data)