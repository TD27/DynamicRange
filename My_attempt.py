#!/usr/bin/env python3

import datetime
import numpy as np

import bspump
import bspump.common
import bspump.file
import bspump.trigger

class Harvester(bspump.Pipeline):



class SamplePipeline(bspump.Pipeline):
    def __init__(self, app, pipeline_id):
        super().__init__(app, pipeline_id)

        self.Harvester = Harvester(app, self)

        self.Sink = bspump.file.FileCSVSink(app, self, config={'path': '/home/tomasdolezal/data/data_out.csv'})

        self.build(
            bspump.file.FileCSVSource(
                app, self, config={'path': './data2.csv', 'delimiter': ',', 'post': 'noop'}
            ).on(bspump.trigger.PubSubTrigger(app, "go!", pubsub=self.PubSub)),

            bspump.common.PPrintProcessor(app, self),

            self.Harvester(app, self),


            self.Sink
        )

        # self.PubSub.publish("go!")

        self.PubSub.subscribe("bspump.pipeline.cycle_end!", self.on_cycle_end)

    def on_cycle_end(self, event_name, pipeline):
        self.Harvester.dump()
        print("We are done!")
        self.App.stop()


if __name__ == '__init__':
    app = bspump.BSPumpApplication()
    svc = app.get_service("bspump.PumpService")

    pl = SamplePipeline(app, 'SamplePipeline')
    svc.add_pipeline(pl)

    app.run()




