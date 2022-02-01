import datetime
from typing import Dict, Optional

from pydantic import BaseModel

# 출처 : https://blog.neonkid.xyz/281
# pydantic으로 모델을 매핑하는 이유
# FastAPI에서 response로 응답하기 위해서는 어떻게 해야될까요? 가장 간단하게는 ORM 모델을 JsonDecoder등을 이용하여 JSON으로 변환하는 방법이 있다.
# 하지만 이 방법은 DateTime이나 Relationship을 이용하는 경우 해당 모델에서 사용하는 모든 타입에 대해 JSON 타입으로 변환해야 하는 로직을 일일이 수행해줘야 한다는 단점이 존재.
# 이를 좀더 편하게 사용하기 위해 Pydantic에 내장된 ORM Model과 FastAPI의 jsonable_encoder를 사용하면 직접 JSON 파서를 구현하지 않아도 Pydantic 모델로 변환해주고 이에 맞춰 JSON 포맷으로 인코딩 해주기 때문

# Pydantic에서 BaseModel은 Python의 dict 형태의 데이터를 Pydantic 모델로 변환하는데 사용하며 여기에 orm_mode를 추가하는 경우 SQLAlchemy의 ORM Model 형태의 데이터를 Pydantic 모델로 변환하는 로직을 사용하게 된다. 

# FastAPI에서는 서버 데이터 반환 시 JSON으로 변환하기 위한 인코더를 제공합니다. 
# 실제로 FastAPI는 데이터 모델로 Pydantic을 채택하고 있으며 response_model에 Pydantic 모델을 명시하는 경우 jsonable_encoder 함수가 작동합니다. 
# 이 때 jsonable_encoder는 오직 dict 형태의 데이터만을 json 값으로 반환해주는데, orm_mode가 설정되어 있는 경우 Pydantic의 from_orm이라는 함수에 있는 로직 그대로를 반환하여 json값으로 변환해주게 됩니다. 

class ProjectBase(BaseModel):
    project_name: str
    description: Optional[str]

class ProjectCreate(ProjectBase):
    pass

# Pydantic 모델을 정의하였을 때는 바로 dict로 변환해서 json으로 반환하고, orm_mode를 사용하는 경우에는 from_orm 함수에 의존한 채 바로 json으로 변환하게 됩니다. 
class Project(ProjectBase):
    project_id: int
    created_datetime: datetime.datetime

    class Config:
        orm_mode = True

class ModelBase(BaseModel):
    project_id: str
    model_name: str
    description: Optional[str]

class ModelCreate(ModelBase):
    pass

class Model(ModelBase):
    model_id: int
    created_datetime: datetime.datetime
    
    class Config:
        orm_mode = True

class ExperimentBase(BaseModel):
    model_id: str
    model_version_id: str
    parameters: Optional[Dict]
    training_dataset: Optional[str]
    validation_dataset: Optional[str]
    test_dataset: Optional[str]
    evaluations: Optional[Dict]
    artifact_file_paths: Optional[Dict]

class ExperimentCreate(ExperimentBase):
    pass

class ExperimentEvaluations(BaseModel):
    evaluations: Dict

class ExperimentArtifactFilePaths(BaseModel):
    artifact_file_paths: Dict

class Experiment(ExperimentBase):
    experiment_id: int
    created_datetime: datetime.datetime

    class Config:
        orm_mode = True

