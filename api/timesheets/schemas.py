"""
Pydantic schemas for Timesheet API.
Used for request/response validation.
"""
from pydantic import BaseModel
from typing import Optional
from datetime import date, datetime
from decimal import Decimal


class TimesheetResponse(BaseModel):
    """Schema for timesheet response."""
    client_employee_id: str
    department_id: Optional[str] = None
    department_name: Optional[str] = None
    home_department_id: Optional[str] = None
    home_department_name: Optional[str] = None
    pay_code: Optional[list[str]] = None
    punch_in_comment: Optional[list[str]] = None
    punch_out_comment: Optional[list[str]] = None
    hours_worked: Optional[Decimal] = None
    punch_apply_date: Optional[date] = None
    punch_in_datetime: Optional[datetime] = None
    punch_out_datetime: Optional[datetime] = None
    scheduled_start_datetime: Optional[datetime] = None
    scheduled_end_datetime: Optional[datetime] = None
    loaded_at: Optional[datetime] = None
    source_file: Optional[str] = None
    
    class Config:
        from_attributes = True


class TimesheetListResponse(BaseModel):
    """Schema for paginated timesheet list response."""
    total: int
    page: int
    limit: int
    timesheets: list[TimesheetResponse]


class TimesheetSummary(BaseModel):
    """Schema for timesheet summary by employee."""
    client_employee_id: str
    total_records: int
    total_hours_worked: Decimal
    first_punch_date: Optional[date] = None
    last_punch_date: Optional[date] = None
    unique_departments: int
