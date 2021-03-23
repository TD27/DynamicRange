import pandas as pd
import datetime as dt
import pickle


class DR_model:

    def __init__(self):
        self.max_limit = 0
        self.min_limit = 0
        self.df_new = pd.DataFrame()
        self.df_train = pd.DataFrame()
        self.df_predict = pd.DataFrame({'Min_Limit': [],
                                        'Max_Limit': [], })

    def fit(self, new_df):
        self.df_new = new_df

        self.df_new = self.df_new.set_index('Time_range')

        if self.df_train.size > 0:
            self.df_train = pd.concat([self.df_train, self.df_new['Value']], axis=1)
        else:
            self.df_train = self.df_new

        # Calculate prediction
        self.df_predict['Median'] = self.df_train.median(axis=1)

        if len(self.df_train.columns) >= 2:
            self.df_predict['STD'] = self.df_train.std(axis=1)
        else:
            print('Model cannot calculate value. It needs more days to predict value')
            self.df_predict['STD'] = 0

        self.df_predict['Min_Limit'] = self.df_predict['Median'] - self.df_predict['STD']
        self.df_predict['Max_Limit'] = self.df_predict['Median'] + self.df_predict['STD']
        return self.df_predict

    def get(self, time):
        # Returns min limit and max limit for a given time

        # time = dt.datetime.strptime(time_str, '%Y-%m-%d %H:%M:%S')
        ts = 60 * ((time.hour * 60) + time.minute) + time.second

        self.max_limit = self.df_predict.loc[:ts].iloc[-1]['Max_Limit']
        self.min_limit = self.df_predict.loc[:ts].iloc[-1]['Min_Limit']

        return self.min_limit, self.max_limit



model = DR_model()

name = '/home/tomasdolezal/PycharmProjects/DynamicRange/model.pkl'
pickle_file = open(name, 'wb')
pickle.dump(model, pickle_file)
pickle_file.close()

print('pickling finished!')
