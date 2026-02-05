"""Shared signal handling for ragmail stages."""

from __future__ import annotations

import os
import signal
import sys
import threading
from typing import Callable

_INTERRUPTED = threading.Event()
_INTERRUPT_COUNT = 0
_LAST_SIGNAL: int | None = None
_INSTALLED = False
_CALLBACKS: list[Callable[[int], None]] = []


def install_signal_handlers(on_interrupt: Callable[[int], None] | None = None) -> None:
    global _INSTALLED
    if on_interrupt is not None:
        _CALLBACKS.append(on_interrupt)
    if _INSTALLED:
        return

    def _handler(signum, _frame):
        global _INTERRUPT_COUNT, _LAST_SIGNAL
        _INTERRUPT_COUNT += 1
        _LAST_SIGNAL = signum
        if not _INTERRUPTED.is_set():
            _INTERRUPTED.set()
            _emit_notice(signum)
        for cb in list(_CALLBACKS):
            try:
                cb(signum)
            except Exception:
                continue
        if _INTERRUPT_COUNT >= 2:
            _emit_force_exit()

    signal.signal(signal.SIGINT, _handler)
    signal.signal(signal.SIGTERM, _handler)
    _INSTALLED = True


def reset_interrupt() -> None:
    global _INTERRUPT_COUNT, _LAST_SIGNAL
    _INTERRUPTED.clear()
    _INTERRUPT_COUNT = 0
    _LAST_SIGNAL = None


def interrupted() -> bool:
    return _INTERRUPTED.is_set()


def interrupt_count() -> int:
    return _INTERRUPT_COUNT


def last_signal() -> int | None:
    return _LAST_SIGNAL


def raise_if_interrupted() -> None:
    if _INTERRUPTED.is_set():
        raise KeyboardInterrupt


def _emit_notice(signum: int) -> None:
    try:
        sig_name = signal.Signals(signum).name
    except Exception:
        sig_name = str(signum)
    out = sys.__stderr__ or sys.stderr
    out.write(f"\nReceived {sig_name}. Finishing current batch and saving checkpoints...\n")
    out.flush()


def _emit_force_exit() -> None:
    out = sys.__stderr__ or sys.stderr
    out.write("\nSecond interrupt received. Exiting immediately.\n")
    out.flush()
    os._exit(130)
