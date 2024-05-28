from typing import Any

from dagster import InputContext, IOManager, OutputContext, io_manager


class NoopIOManager(IOManager):
    def handle_output(self, context: OutputContext, obj: Any) -> Any:
        pass

    def load_input(self, context: InputContext) -> Any:
        pass


@io_manager
def noop(_) -> NoopIOManager:
    return NoopIOManager()
