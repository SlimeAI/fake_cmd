import signal
import platform
from subprocess import Popen


def send_keyboard_interrupt(process: Popen):
    """
    Send keyboard interrupt to the process.
    """
    if platform.system() == 'Windows':
        process.send_signal(signal.CTRL_C_EVENT)
    else:
        process.send_signal(signal.SIGINT)
