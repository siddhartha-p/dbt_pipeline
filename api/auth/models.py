"""
User model for authentication.
Maps to api.users table in PostgreSQL.
"""
from sqlalchemy import Column, Integer, String, Boolean, DateTime, func
from api.database import Base


class User(Base):
    """User model for authentication and authorization."""
    
    __tablename__ = "users"
    __table_args__ = {"schema": "api"}
    
    id = Column(Integer, primary_key=True, index=True)
    username = Column(String(50), unique=True, nullable=False, index=True)
    email = Column(String(100), unique=True, nullable=False, index=True)
    hashed_password = Column(String(255), nullable=False)
    role = Column(String(20), default="user")  # 'admin' or 'user'
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())
    
    def is_admin(self) -> bool:
        """Check if user has admin role."""
        return self.role == "admin"
