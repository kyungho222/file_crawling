"""Local shim for `langchain` package imports used by the project.
Provides `text_splitter` submodule compatibility.
"""
from . import text_splitter
__all__ = ["text_splitter"]

