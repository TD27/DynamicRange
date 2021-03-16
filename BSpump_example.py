#!/usr/bin/env python3
import datetime
import numpy as np

import bspump
import bspump.common
import bspump.file
import bspump.trigger


class Harvester(bspump.Pipeline):

    def __init__(self, app, id=None, config=None):
        super().__init__(app, id, config)

        # 5 min harvesting granularity
        self.Resolution = 60 * 5
        self.MaxRow = int((24 * 60 * 60) // self.Resolution)
        self.CurrentWindow = np.zeros((2, self.MaxRow))
        self.StartTime = datetime.datetime(year=2020, month=6, day=7)

        for i in range(self.MaxRow):
            self.CurrentWindow[0, i] = (
                    self.Resolution * i
            )

    def process(self, context, event):
        time = datetime.datetime.strptime(
            event['Time'],
            "%Y-%m-%d %H:%M:%S"
        )

        # Don't process empty values
        try:
            value = float(event['Value'])
        except ValueError:
            return event

        dt = int((time - self.StartTime).total_seconds() / self.Resolution)

        if dt >= self.MaxRow:
            # This early event
            return event

        if dt < 0:
            # This is late event
            return event

        # TODO: Apply aggregation
        self.CurrentWindow[1, dt] = value

        return event

    def dump(self):
        print(self.CurrentWindow)


# df = pd.DataFrame(self.CurrentWindow)


class SamplePipeline(bspump.Pipeline):

    def __init__(self, app, pipeline_id):
        super().__init__(app, pipeline_id)
        self.Harvester = Harvester(app, self)

        self.build(
            bspump.file.FileCSVSource(
                app,
                self,
                config={
                    'path': './data2.csv',
                    'delimiter': ',',
                    'post': 'noop'
                }).on(
                bspump.trigger.PubSubTrigger(
                    app, "go!", pubsub=self.PubSub
                )
            ),
            # bspump.common.PPrintProcessor(app, self),
            self.Harvester,
            bspump.common.NullSink(app, self)
        )

        self.PubSub.publish("go!")
        self.PubSub.subscribe("bspump.pipeline.cycle_end!", self.on_cycle_end)

    def on_cycle_end(self, event_name, pipeline):
        '''
        This ensures that at the end of the file scan, the target file is closed
        '''
        self.Harvester.dump()
        print("We are done!")
        self.App.stop()


if __name__ == '__main__':
    app = bspump.BSPumpApplication()

    svc = app.get_service("bspump.PumpService")

    pl = SamplePipeline(app, 'SamplePipeline')
    svc.add_pipeline(pl)

    app.run()


