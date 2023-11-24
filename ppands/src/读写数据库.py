import pandas as pd

from sqlalchemy import create_engine
engine = create_engine(f'mysql+pymysql://kaka:Xincheng0927@119.45.173.253:3306/test?charset=utf8')

# df = pd.read_csv("/workspace/ppands/data/test.csv",encoding="utf-8",index_col=[0], parse_dates=['datetime'] )
# print(df)
# df.to_sql('test_pdtosql', engine, index=False, if_exists="append")

# df = pd.read_sql("test_pdtosql", engine)
# print(df)
df1 = pd.read_sql("select * from test_pdtosql where  age = 28", engine)
print(df1)

