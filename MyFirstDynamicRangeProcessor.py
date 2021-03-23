#!/usr/bin/env python3

import datetime
import pickle

import bspump
import bspump.common
import bspump.file
import bspump.trigger
import numpy as np
import pandas as pd

import DynamicRange

from DynamicRange import DR_model


class RangeEvaluator(bspump.Processor):

    def __init__(self, app, id=None, config=None):
        super().__init__(app, id, config)

        self.Symptom = False

        # špatně umisťuji pickle file
        # Download and unpickle a model
        pickle_file = open('/home/tomasdolezal/PycharmProjects/DynamicRange/model.pkl', 'rb')  # use your path
        self.model = pickle.load(pickle_file)
        pickle_file.close()

    def process(self, context, event):

        time = datetime.datetime.strptime(event['Time'], '%Y-%m-%d %H:%M:%S').time()

        print('Jsem v RangeEvaluatoru')
        print(self.model.df_predict)
        print(time)

        min_limit, max_limit = self.model.get(time)

        print(min_limit)

        # Value exceeds the given min or max limits for the actual time period
        if (event['Value'] < min_limit) | (event['Value'] > max_limit):
            print('Exceeded min limits')
        else:
            print('Value is OK')


class Harvester(bspump.Processor):

    def __init__(self, app, id=None, config=None):
        super().__init__(app, id, config)

        self.Resolution = 60 * 5  # 15 min sample interval
        self.StartTime = datetime.datetime(year=2020, month=6, day=8)  # the day I want to capture events
        self.MaxSample = int((24 * 60 * 60) / self.Resolution)  # Max number of samples in a day
        self.CurrentWindow = np.zeros((self.MaxSample, 4))

        # Puts time range in seconds into the first column
        for i in range(self.MaxSample):
            dt = i * self.Resolution
            self.CurrentWindow[i, 0] = dt

    def process(self, context, event):
        # Time of incoming event
        time = datetime.datetime.strptime(
            event['Time'],
            '%Y-%m-%d %H:%M:%S'
        )

        # Difference between incoming event and StartTime
        dt = (time - self.StartTime).total_seconds() / self.Resolution

        # An early event
        if dt < 0:
            return event

        # A late event
        if dt >= self.MaxSample:
            return event

        try:
            value = float(event['Value'])
        except ValueError:  # some events may have NaN value
            return event

        dt = int(dt)  # Integer makes number of the sample in a day

        # Calculates an average value in a given time interval
        self.CurrentWindow[dt, 1] = self.CurrentWindow[dt, 1] + value  # sum of values
        self.CurrentWindow[dt, 2] = self.CurrentWindow[dt, 2] + 1  # count of values
        self.CurrentWindow[dt, 3] = self.CurrentWindow[dt, 1] / self.CurrentWindow[dt, 2]  # average of values

        return event

    def dump(self):
        # Returns Pandas DataFrame
        df = pd.DataFrame(self.CurrentWindow[:, [0, 3]])
        df = df.rename(columns={0: 'Time_range', 1: 'Value'})
        return df


class MyFirstPipeline(bspump.Pipeline):
    def __init__(self, app, pipeline_id):
        super().__init__(app, pipeline_id)

        # Definition of my source
        self.Source = bspump.file.FileCSVSource(
            app, self,
            config={'path': './data2.csv', 'delimiter': ',', 'post': 'noop'}
        ).on(bspump.trigger.PubSubTrigger(app, "go!", pubsub=self.PubSub))

        # Definition of my processors
        self.Print = bspump.common.PPrintProcessor(app, self)
        # self.Harvester = Harvester(app, self)
        self.RangeEvaluator = RangeEvaluator(app, self)

        # Definition of my sink
        self.Sink = bspump.common.NullSink(app, self)

        # Definition of my pipeline
        self.build(
            self.Source,
            # self.Harvester,
            self.Print,
            self.RangeEvaluator,
            self.Sink
        )

        self.PubSub.publish("go!")
        self.PubSub.subscribe("bspump.pipeline.cycle_end!", self.on_cycle_end)

    def on_cycle_end(self, event_name, pipeline):
        #       df = self.Harvester.dump()

        self.App.stop()


print('I have started')


if __name__ == '__main__':
    # Registration of the PumpService in ASAB
    app = bspump.BSPumpApplication()  # application object
    my_service = app.get_service("bspump.PumpService")  # service registration

    # Registration of the pipeline MyFirstPipeline
    my_pipeline = MyFirstPipeline(app, 'MyFirstPipeline')
    my_service.add_pipeline(my_pipeline)

    app.run()
