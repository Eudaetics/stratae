"""Run the stub command: `python -m stratae.scripts.signatures`."""

import logging
import sys

from ._command import main

logging.basicConfig(format="%(message)s")
sys.exit(main())
