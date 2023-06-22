from sqlalchemy import create_engine, Column, Integer, String
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from autocompy import config
from flask_login import current_user

# Define the MySQL connection URL
db_url = f"mysql://{config.m_username}:{config.m_password}@{config.m_host}/autocompy"

# Create the database engine
engine = create_engine(db_url)

# Create a session factory
Session = sessionmaker(bind=engine)

# Create a base class for declarative models
Base = declarative_base()


# Define a sample model class
class autocompy_audit_history(Base):
    __tablename__ = 'autocompy_audit'
    autocompy_trx_id = Column(Integer, primary_key=True, autoincrement=True)
    username = Column(String(20))
    user_email = Column(String(40))
    execution_time = Column(String(10))
    source = Column(String(20))
    source_path = Column(String(100))
    target = Column(String(20))
    target_path = Column(String(100))
    mode = Column(String(10))
    status_code = Column(String(10))
    status_description = Column(String(50))
    executed_at = Column(String(40))


# Create the database tables
Base.metadata.create_all(engine)

# Create a new session
session = Session()


def autocompy_history(output_dict, execution_time, executed_at):

    data = {
        'username': current_user.username,
        'user_email': current_user.email,
        'execution_time': str(execution_time),
        'source': output_dict.get("source"),
        'source_path': output_dict.get("source_path"),
        'target': output_dict.get("target"),
        'target_path': output_dict.get("target_path"),
        'mode': output_dict.get("mode"),
        'status_code': output_dict.get("job_status"),
        'status_description': output_dict.get("status"),
        'executed_at': str(executed_at)
    }

    print(data)
    # Perform a sample insert
    new_user = autocompy_audit_history(**data)
    session.add(new_user)
    session.commit()
    print("History captured successfully ")

    # Close the session
    session.close()
