
# coding: utf-8

# In[1]:


#import numpy as np
import pandas as pd
from pandas import DataFrame, Series
import sqlite3 as db


# In[2]:


# Problem Statement: Read the following data set: https://archive.ics.uci.edu/ml/machine-learning-databases/adult/adult.data4

url = "https://archive.ics.uci.edu/ml/machine-learning-databases/adult/adult.data"
col_list = ['age','workclass','fnlwgt','education','education-num','marital-status','occupation',
           'relationship','race','sex','capital-gain','capital-loss','hours-per-week','native-country','Label']


# In[3]:


# Import the data from the url above into Pandas DataFrame

adult = pd.read_csv(url,sep=",",delimiter=",",names=col_list,skipinitialspace=True)


# In[4]:


print(adult.columns)
import re
adult.columns = [re.sub("[-]", "_", col) for col in adult.columns]

print(adult.columns)


# In[5]:


adult.head()


# In[6]:


# Task


# In[7]:


# Create an sqlalchemy engine using a sample from the data set

# By importing corresponding sqlalchemy library

from sqlalchemy import *
from sqlalchemy import create_engine, ForeignKey
from sqlalchemy import Column, Date, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, backref


# In[8]:


# Create database engine

engine = create_engine('sqlite://', echo=True)


# In[9]:


# connect to database and load the dataframe adultdb into sqlite

adult.to_sql('adultdb', con=engine)


# In[12]:


from sqlalchemy.orm import Session
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

# create an Object to hold SQLAlchemy data types to map properties of Python classes into columns on a relation database table

Base = declarative_base(engine)
class Adultdb(Base):
    """
    eg. fields: id, title
    """
    __tablename__ = 'adultdb'
    __table_args__ = {'autoload': True}
    
    index = Column(Integer(), primary_key=True)
    age = Column(Integer())
    workclass = Column(String())
    
    
    #Create SQLAlchemy Sessions

def loadSession():
    """"""
    metadata = Base.metadata
    Session = sessionmaker(bind=engine)
    session = Session()
    return session


# In[13]:


#Querying Data with SQLAlchemy ORM
if __name__ == "__main__":
    session = loadSession()
    rows = session.query(Adultdb).first()
    print("AGE","SEX   ","WORKCLAS ","COUNTRY ","OCCUPATION")
    print("*"*80)
    print(rows.age,rows.sex,rows.workclass,rows.native_country,rows.occupation)


# In[14]:


# Write two basic update queries


# In[15]:


# Update query-1

if __name__ =="__main__":
    session = loadSession()
    rows = session.query(Adultdb).filter_by(fnlwgt=77516).first()
    print(rows)
    rows.occupation ='Student'
    rows.workclass ='Study'
    session.commit()


# In[16]:


# Verify update results

if __name__ == "__main__":
    session = loadSession()
    rows = session.query(Adultdb).filter_by(fnlwgt=77516).first()
    print("occupation : ",rows.occupation,"workclass :",rows.workclass,"fnlwgt : ",rows.fnlwgt)


# In[17]:


# update query-2

if __name__ == "__main__":
    session = loadSession()
    row = session.query(Adultdb).filter_by(age=60,education='Masters').all()
    for i in row:
        i.occupation = 'Retired'
    session.commit()


# In[18]:


# Verify query-2

if __name__ == "__main__":
    session = loadSession()
    row = session.query(Adultdb).filter_by(age=60,education='Masters').all()
    for i in row:
        print(i.age,i.education,i.sex,i.occupation,i.workclass)


# In[19]:


# Write two delete queries


# In[20]:


#Check for all rows which have occupation as ? - Querying Data with SQLAlchemy ORM

if __name__ == "__main__":
    session = loadSession()
    rows = session.query(Adultdb).filter_by(occupation="?").all()
    print("Count of rows before delete operation : ",len(rows))


# In[21]:


# Delete-1

#Delete rows which have occupation as "?" - Delete Data with SQLAlchemy ORM
if __name__ == "__main__":
    session = loadSession()
    session.query(Adultdb).filter_by(occupation="?").delete(synchronize_session='fetch')
    session.commit()
    rows = session.query(Adultdb).filter_by(occupation="?").all()
    print("Count of rows after delete operation : ",len(rows))


# In[22]:


# Delete-2

#Check for all rows which have education as  Some-college- Querying Data with SQLAlchemy ORM
if __name__ == "__main__":
    session = loadSession()
    rows = session.query(Adultdb).filter_by(education="Some-college").all()
    print("count of rows :",len(rows))


# In[23]:


# #delete rows from table adultdb which have education as "Some-college" - Delete Data with SQLAlchemy ORM

if __name__ == "__main__":
    session = loadSession()
    session.query(Adultdb).filter_by(education="Some-college").delete(synchronize_session='fetch')
    session.commit()
    rows = session.query(Adultdb).filter_by(education="Some-college").all()
    print("Count of rows after Delete : ",len(rows))


# In[24]:


# Write two filter queries


# In[25]:


# Filter Query-1
# Querying Data with SQLAlchemy ORM based on filter on workclass column having a value as "Private"

if __name__ == "__main__":
    session = loadSession()
    rows = session.query(Adultdb).filter_by(workclass='Private').all()
    for i in rows:
        print(i.age,i.sex,i.workclass,i.native_country)


# In[27]:


# Querying the Data with SQLALCHEMY ORM based on filter on marital_status column having the value as "Never-Married"

count = 0
for rows in engine.execute("SELECT * FROM adultdb where marital_status == 'Never-married'").fetchall():
    print(rows.age,",",rows.marital_status,",",rows.occupation)
    count+=1
    
print("Total Number of rows fetched : ",count)


# In[28]:


# Querying Data with SQLAlchemy ORM based on filter on native_country column having a value as "Columbia"

count = 0
for rows in engine.execute("SELECT age,sex,occupation FROM adultdb where native_country = 'Columbia' ").fetchall():
    print(rows.age," ",rows.sex," ",rows.occupation)
    count+=1
print("Total Records Retrieved : ",count)


# In[29]:


# Write two function queries


# In[30]:


# Querying Data with SQLAlchemy ORM - Use aggregate Function -count the number of male and female based on sex data column values

# Function Query-1
from sqlalchemy.sql import func
if __name__ == "__main__":
    session = loadSession()
    result = session.query(func.count(Adultdb.age).label('count_age'), Adultdb.sex ).group_by(Adultdb.sex).all()
    print("Count of people based on sex ")
    print('*'*80)
          
    for rows in result:
        print(rows)


# In[31]:


# Querying Data with SQLAlchemy ORM - Use aggregate Function -average age and minimum age of people based on sex column value 

# Function Query-2
if __name__ == "__main__":
    session = loadSession()
    result = session.query(func.avg(Adultdb.age).label('avg_age'),func.min(Adultdb.age).label('min_age'), Adultdb.sex).group_by(Adultdb.sex).all()
    print("Average age,minimum age  of people based on sex ")
    print('*'*80)
    for rows in result:
        print(rows)

