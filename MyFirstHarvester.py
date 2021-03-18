#!/usr/bin/env python3

import datetime
import numpy as np
import bspump
import bspump.common
import bspump.file
import bspump.trigger
import pandas as pd


class Harvester(bspump.Pipeline):

    def __init__(self, app, id=None, config=None):
        super().__init__(app, id, config)

        self.Resolution = 60 * 60  # 5min interval# 5min interval
        self.MaxSample = int((24 * 60 * 60) / self.Resolution)
        self.Dataset = np.zeros((3, self.MaxSample))
        self.StartTime = datetime.datetime(year=2020, month=6, day=8)

    def process(self, context, event):
        time = datetime.datetime.strptime(
            event['Time'],
            '%Y-%m-%d %H:%M:%S'
        )

        # Delta time from StartTime
        dt = (time - self.StartTime).total_seconds() / self.Resolution

        # An early event
        if dt < 0:
            return event

        # A late event
        if dt >= self.MaxSample:
            return event

        try:
            value = float(event['Value'])
        except ValueError:
            return event

        dt = int(dt)

        self.Dataset[0, dt] = self.Dataset[0, dt] + value
        self.Dataset[1, dt] = self.Dataset[1, dt] + 1
        self.Dataset[2, dt] = self.Dataset[0, dt] / self.Dataset[1, dt]

        return event

    def dump(self):
        # Returns average value of incoming events
        df = pd.DataFrame(self.Dataset[2])
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
        self.Harvester = Harvester(app, self)

        # Definition of my sink
        self.Sink = bspump.common.NullSink(app, self)

        # Definition of my pipeline
        self.build(
            self.Source,
            # self.Print,
            self.Harvester,
            self.Sink
        )

        self.PubSub.publish("go!")
        self.PubSub.subscribe("bspump.pipeline.cycle_end!", self.on_cycle_end)

    def on_cycle_end(self, event_name, pipeline):
        df = self.Harvester.dump()
        print(df)
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
