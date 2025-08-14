from typing import Optional, Any, Dict
from contextlib import contextmanager


class AutosubmitContext:
    """
    Context object that holds command-specific configuration and state.
    """

    def __init__(self):
        # Argument flags
        self.compress_remote_logs: bool = False
        # Additional custom config stored in a dictionary
        self._custom_config: Dict[str, Any] = {}

    def set_config(self, key: str, value: Any) -> None:
        """Set a custom configuration value."""
        self._custom_config[key] = value

    def get_config(self, key: str, default: Any = None) -> Any:
        """Get a custom configuration value."""
        return self._custom_config.get(key, default)

    def update_from_args(self, args) -> None:
        """Update context from command line arguments."""
        if hasattr(args, "compress_remote_logs"):
            self.compress_remote_logs = args.compress_remote_logs

class AutosubmitContextManager:
    """
    Simple context manager for Autosubmit execution context.
    """

    def __init__(self):
        self._context: Optional[AutosubmitContext] = None

    def set_context(self, context: AutosubmitContext) -> None:
        """Set the current context."""
        self._context = context

    def get_context(self) -> Optional[AutosubmitContext]:
        """Get the current context."""
        return self._context

    def clear_context(self) -> None:
        """Clear the current context."""
        self._context = None

    @contextmanager
    def execution_context(self, context: AutosubmitContext):
        """Context manager for setting and cleaning up execution context."""
        old_context = self.get_context()
        try:
            self.set_context(context)
            yield context
        finally:
            if old_context is not None:
                # Restore the old context for nested executions
                self.set_context(old_context)
            else:
                self.clear_context()


# Global context manager instance
_context_manager = AutosubmitContextManager()


# Context manager operations


def get_current_context() -> Optional[AutosubmitContext]:
    """Get the current execution context."""
    return _context_manager.get_context()


def set_current_context(context: AutosubmitContext) -> None:
    """Set the current execution context."""
    _context_manager.set_context(context)


def execution_context(context: AutosubmitContext):
    """Context manager for execution context."""
    return _context_manager.execution_context(context)


def create_context_from_args(args) -> AutosubmitContext:
    """Create a context object from command line arguments."""
    context = AutosubmitContext()
    context.update_from_args(args)
    return context
