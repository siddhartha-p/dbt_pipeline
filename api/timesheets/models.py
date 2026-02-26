"""
Timesheet model mapping to silver.stg_timesheets table.
"""
from sqlalchemy import Column, String, Date, DateTime, Numeric, ARRAY
from api.database import Base


class Timesheet(Base):
    """
    Timesheet model mapping to silver.stg_timesheets table.
    Contains individual punch records with cleaned and validated data.
    """
    
    __tablename__ = "stg_timesheets"
    __table_args__ = {"schema": "silver"}
    
    # Composite primary key: employee + date + punch_in
    client_employee_id = Column(String, primary_key=True)
    punch_apply_date = Column(Date, primary_key=True)
    punch_in_datetime = Column(DateTime, primary_key=True)
    
    department_id = Column(String)
    department_name = Column(String)
    home_department_id = Column(String)
    home_department_name = Column(String)
    
    # Array fields (PostgreSQL ARRAY type)
    pay_code = Column(ARRAY(String))
    punch_in_comment = Column(ARRAY(String))
    punch_out_comment = Column(ARRAY(String))
    
    hours_worked = Column(Numeric(5, 2))
    punch_out_datetime = Column(DateTime)
    scheduled_start_datetime = Column(DateTime)
    scheduled_end_datetime = Column(DateTime)
    
    loaded_at = Column(DateTime)
    source_file = Column(String)
