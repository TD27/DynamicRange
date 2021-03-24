import datetime as datetime
import pickle

from DynamicRange import DR_model

PathModel = './dynamic_range_model.pkl'
model = pickle.load(open(PathModel, 'rb'))

print('--- Iteration 1 ---')
PathData = './daily_data_1.pkl'
data = pickle.load(open(PathData, 'rb'))
model.fit(data)

print(model.df_train)
print(model.df_predict)

print('--- Iteration 2 ---')
PathData = './daily_data_2.pkl'
data = pickle.load(open(PathData, 'rb'))
model.fit(data)

print(model.df_train)
print(model.df_predict)

print('--- Iteration 3 ---')
PathData = './daily_data_3.pkl'
data = pickle.load(open(PathData, 'rb'))
model.fit(data)

print(model.df_train)
print(model.df_predict)

print('--- Iteration 4 ---')
PathData = './daily_data_4.pkl'
data = pickle.load(open(PathData, 'rb'))
model.fit(data)

print(model.df_train)
print(model.df_predict)

time = datetime.datetime.strptime("2021-06-09 00:00:12", '%Y-%m-%d %H:%M:%S').time()
print(model.get(time))


PathModel = './trained_model.pkl'
pickle.dump(model, open(PathModel, 'wb'))

