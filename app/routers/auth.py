"""Authentication API routes."""

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from app.db.session import get_db
from app.db.models import User
from app.schemas.auth import UserRegister, UserLogin, Token, UserResponse
from app.services.auth_service import verify_password, get_password_hash, create_access_token
from datetime import timedelta
from app.core.config import settings

router = APIRouter(prefix="/auth", tags=["Authentication"])


@router.post("/register", response_model=UserResponse, status_code=status.HTTP_201_CREATED)
async def register(user_data: UserRegister, db: AsyncSession = Depends(get_db)):
    """
    Register a new user.
    
    Creates a new user account with email, name, and password.
    Password is hashed before storage.
    """
    try:
        # Check if user already exists
        result = await db.execute(select(User).where(User.email == user_data.email))
        existing_user = result.scalar_one_or_none()
        
        if existing_user:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Email already registered"
            )
        
        # Create new user
        hashed_password = get_password_hash(user_data.password)
        new_user = User(
            email=user_data.email,
            name=user_data.name,
            password_hash=hashed_password
        )
        
        db.add(new_user)
        await db.commit()
        await db.refresh(new_user)
        
        return UserResponse(
            id=new_user.id,
            email=new_user.email,
            name=new_user.name
        )
    except HTTPException:
        raise
    except Exception as e:
        await db.rollback()
        # Check if it's a database column error
        error_str = str(e).lower()
        if "column" in error_str and "does not exist" in error_str:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Database schema error. Please run migrations: python run_migrations.py"
            )
        # Log the actual error for debugging
        print(f"Registration error: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Registration failed: {str(e)}"
        )


@router.post("/login", response_model=Token)
async def login(credentials: UserLogin, db: AsyncSession = Depends(get_db)):
    """
    Login user and return JWT access token.
    
    Validates email and password, then returns a JWT token for authentication.
    """
    # Find user by email
    result = await db.execute(select(User).where(User.email == credentials.email))
    user = result.scalar_one_or_none()
    
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect email or password"
        )
    
    # Verify password
    if not verify_password(credentials.password, user.password_hash):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect email or password"
        )
    
    # Create access token
    access_token_expires = timedelta(minutes=settings.ACCESS_TOKEN_EXPIRE_MINUTES)
    access_token = create_access_token(
        data={"sub": str(user.id), "email": user.email},
        expires_delta=access_token_expires
    )
    
    return Token(access_token=access_token, token_type="bearer")


@router.post("/logout")
async def logout():
    """
    Logout user.
    
    Logs the logout event. With JWT tokens, actual invalidation happens client-side
    by removing the token. In a production system, you could implement token blacklisting here.
    """
    # In a production system, you might want to:
    # 1. Add the token to a blacklist/revocation list
    # 2. Log the logout event to database
    # 3. Clear any server-side sessions
    
    # For now, we just acknowledge the logout
    return {
        "message": "Logout successful",
        "success": True
    }

