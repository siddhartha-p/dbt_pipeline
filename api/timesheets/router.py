"""
Timesheet router.
Provides read-only endpoints for timesheet data.
"""
from fastapi import APIRouter, Depends, HTTPException, status, Query
from sqlalchemy.orm import Session
from sqlalchemy import func
from typing import Optional
from datetime import date

from api.database import get_db
from api.auth.models import User
from api.dependencies import get_current_user
from api.timesheets.models import Timesheet
from api.timesheets.schemas import (
    TimesheetResponse,
    TimesheetListResponse,
    TimesheetSummary
)

router = APIRouter(prefix="/timesheets", tags=["Timesheets"])


@router.get("", response_model=TimesheetListResponse)
def list_timesheets(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
    page: int = Query(1, ge=1, description="Page number"),
    limit: int = Query(50, ge=1, le=100, description="Items per page"),
    employee_id: Optional[str] = Query(None, description="Filter by employee ID"),
    start_date: Optional[date] = Query(None, description="Filter by start date (inclusive)"),
    end_date: Optional[date] = Query(None, description="Filter by end date (inclusive)"),
    department: Optional[str] = Query(None, description="Filter by department name")
):
    """
    List all timesheets with optional filtering and pagination.
    
    - **page**: Page number (default: 1)
    - **limit**: Items per page (default: 50, max: 100)
    - **employee_id**: Filter by client employee ID
    - **start_date**: Filter timesheets on or after this date
    - **end_date**: Filter timesheets on or before this date
    - **department**: Filter by department name (partial match)
    """
    query = db.query(Timesheet)
    
    # Apply filters
    if employee_id:
        query = query.filter(Timesheet.client_employee_id == employee_id)
    
    if start_date:
        query = query.filter(Timesheet.punch_apply_date >= start_date)
    
    if end_date:
        query = query.filter(Timesheet.punch_apply_date <= end_date)
    
    if department:
        query = query.filter(Timesheet.department_name.ilike(f"%{department}%"))
    
    # Get total count before pagination
    total = query.count()
    
    # Apply ordering and pagination
    skip = (page - 1) * limit
    timesheets = query.order_by(
        Timesheet.punch_apply_date.desc(),
        Timesheet.punch_in_datetime.desc()
    ).offset(skip).limit(limit).all()
    
    return TimesheetListResponse(
        total=total,
        page=page,
        limit=limit,
        timesheets=timesheets
    )


@router.get("/employee/{employee_id}", response_model=TimesheetListResponse)
def get_employee_timesheets(
    employee_id: str,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
    page: int = Query(1, ge=1, description="Page number"),
    limit: int = Query(50, ge=1, le=100, description="Items per page"),
    start_date: Optional[date] = Query(None, description="Filter by start date (inclusive)"),
    end_date: Optional[date] = Query(None, description="Filter by end date (inclusive)")
):
    """
    Get all timesheets for a specific employee.
    
    - **employee_id**: The client employee ID
    - **page**: Page number (default: 1)
    - **limit**: Items per page (default: 50, max: 100)
    - **start_date**: Filter timesheets on or after this date
    - **end_date**: Filter timesheets on or before this date
    """
    query = db.query(Timesheet).filter(
        Timesheet.client_employee_id == employee_id
    )
    
    # Apply date filters
    if start_date:
        query = query.filter(Timesheet.punch_apply_date >= start_date)
    
    if end_date:
        query = query.filter(Timesheet.punch_apply_date <= end_date)
    
    # Get total count
    total = query.count()
    
    if total == 0:
        # Check if employee exists at all
        any_record = db.query(Timesheet).filter(
            Timesheet.client_employee_id == employee_id
        ).first()
        
        if not any_record:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"No timesheets found for employee '{employee_id}'"
            )
    
    # Apply ordering and pagination
    skip = (page - 1) * limit
    timesheets = query.order_by(
        Timesheet.punch_apply_date.desc(),
        Timesheet.punch_in_datetime.desc()
    ).offset(skip).limit(limit).all()
    
    return TimesheetListResponse(
        total=total,
        page=page,
        limit=limit,
        timesheets=timesheets
    )


@router.get("/employee/{employee_id}/summary", response_model=TimesheetSummary)
def get_employee_timesheet_summary(
    employee_id: str,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
    start_date: Optional[date] = Query(None, description="Filter by start date"),
    end_date: Optional[date] = Query(None, description="Filter by end date")
):
    """
    Get a summary of timesheets for a specific employee.
    
    - **employee_id**: The client employee ID
    - **start_date**: Optional start date filter
    - **end_date**: Optional end date filter
    
    Returns aggregated statistics including total hours, record count, and date range.
    """
    query = db.query(Timesheet).filter(
        Timesheet.client_employee_id == employee_id
    )
    
    if start_date:
        query = query.filter(Timesheet.punch_apply_date >= start_date)
    
    if end_date:
        query = query.filter(Timesheet.punch_apply_date <= end_date)
    
    # Get summary statistics
    summary = db.query(
        func.count().label("total_records"),
        func.coalesce(func.sum(Timesheet.hours_worked), 0).label("total_hours"),
        func.min(Timesheet.punch_apply_date).label("first_date"),
        func.max(Timesheet.punch_apply_date).label("last_date"),
        func.count(func.distinct(Timesheet.department_name)).label("unique_depts")
    ).filter(
        Timesheet.client_employee_id == employee_id
    )
    
    if start_date:
        summary = summary.filter(Timesheet.punch_apply_date >= start_date)
    if end_date:
        summary = summary.filter(Timesheet.punch_apply_date <= end_date)
    
    result = summary.first()
    
    if result.total_records == 0:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No timesheets found for employee '{employee_id}'"
        )
    
    return TimesheetSummary(
        client_employee_id=employee_id,
        total_records=result.total_records,
        total_hours_worked=result.total_hours or 0,
        first_punch_date=result.first_date,
        last_punch_date=result.last_date,
        unique_departments=result.unique_depts
    )
