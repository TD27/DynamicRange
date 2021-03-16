import logging

import bspump
import bspump.common
import bspump.file
import bspump.trigger

###

L = logging.getLogger(__name__)


###


class SamplePipeline(bspump.Pipeline):

    def __init__(self, app, pipeline_id):
        super().__init__(app, pipeline_id)

        self.Sink = bspump.file.FileCSVSink(app, self, config={'path': '/home/tomasdolezal/data/out.csv'})

        self.build(
            bspump.file.FileCSVSource(
                app, self, config={'path': './data2.csv', 'delimiter': ';', 'post': 'noop'}
            ).on(bspump.trigger.RunOnceTrigger(app)),
            bspump.common.PPrintProcessor(app, self),
            self.Sink
        )

        self.PubSub.subscribe("bspump.pipeline.cycle_end!", self.on_cycle_end)

    def on_cycle_end(self, event_name, pipeline):

        self.Sink.rotate()


if __name__ == '__main__':
    app = bspump.BSPumpApplication()

    svc = app.get_service("bspump.PumpService")

    # Construct and register Pipeline
    pl = SamplePipeline(app, 'SamplePipeline')
    svc.add_pipeline(pl)

    app.run()
