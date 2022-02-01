import os
from contextlib import contextmanager

from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from src.configurations import DBConfigurations

# 출처 : https://phsun102.tistory.com/63
# create_engine : 인자값으로 DB URL을 추가하면 DB Host에 DB 연결을 생성. 이 함수가 DB 연결의 출발점.
engine = create_engine(
    DBConfigurations.sql_alchemy_database_url,
    encoding="utf-8",
    pool_recycle=3600,
    echo=False,
)

# sessionmaker : 호출되었을 때, 세션을 생성해준다.
# auticommit : api가 호출되어 DB의 내용이 변경된 경우 자동으로 commit하며 변경할지에 대한 여부를 결정한다. False로 지정한 경우에는 insert, update, delete 등으로 내용이 변경됐을 때, 수동적으로 commit을 진행해주어야 한다. 
# autocommit을 비활성화하는 이유는 데이터 변경 작업을 사용할 경우 여러 줄의 SQL쿼리를 사용했을 때 한번에 반영시키기 위함. 
# autoflush : 호출되면서 commit 되지 않은 부분의 내역을 삭제할 시의 여부를 정하는 부분.
# bind : 어떤 엔진을 통해 DB 연결을 할 지 결정하는 부분. 
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# declarative_base: 상속된 DB 모델 클래스들을 자동적으로 연결시켜주는 역할을 한다. 즉, 테이블명이 일치하는 모델을 찾아 쿼리문을 실행시켜준다. 
Base = declarative_base() 

def get_db():
    db = SessionLocal()
    try:
        yield db # DB 연결 성공한 경우 DB 세션 시작
    except:
        db.rollback()
        raise
    finally:
        db.close() # DB 세션이 시작된 후, API 호출이 마무리되면 DB 세션을 닫아준다

@contextmanager
def get_context_db():
    db = SessionLocal()
    try:
        yield db
    except:
        db.rollback()
        raise
    finally:
        db.close()