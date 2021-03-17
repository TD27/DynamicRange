#!/usr/bin/env python3

# import datetime
import numpy as np
import bspump
import bspump.common
import bspump.file
import bspump.trigger


class Harvester(bspump.Pipeline):
    def __init__(self, app, id=None, config=None):
        super().__init__(app, id, config)

        self.Dataset = np.zeros(2, 288)

    def process(self, context, event):
        value = float(event['Value'])


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
        #        self.Harvester = Harvester(app, self)

        # Definition of my sink
        self.Sink = bspump.common.NullSink(app, self)

        # Definition of my pipeline
        self.build(
            self.Source,
            self.Print,
            self.Harvester,
            self.Sink
        )

        self.PubSub.publish("go!")
        self.PubSub.subscribe("bspump.pipeline.cycle_end!", self.on_cycle_end)

    def on_cycle_end(self, event_name, pipeline):
#        self.Harvester.dump()
        print("We are done!")
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
